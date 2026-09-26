package taskpool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

var (
	// ErrClosed 表示任务池已开始关闭，不再接收任务。
	ErrClosed = errors.New("taskpool: closed")
	// ErrInvalidConfig 表示任务池配置无效。
	ErrInvalidConfig = errors.New("taskpool: invalid config")
	// ErrInvalidTask 表示提交参数无效。
	ErrInvalidTask = errors.New("taskpool: invalid task")
)

const (
	maxRetries           = 100
	retryDelay           = time.Second
	defaultSubmitTimeout = time.Second
	defaultIdleTimeout   = 5 * time.Minute
	defaultDrainTimeout  = 15 * time.Second
)

type poolState uint8

const (
	// stateOpen 接收新任务并正常调度。
	stateOpen poolState = iota
	// stateDraining 停止接收新任务，继续执行已接收任务。
	stateDraining
	// stateAborted 放弃排队任务，并取消运行中任务的 context。
	stateAborted
)

// Config 定义任务池的容量、消费者区间与失败重试行为。
type Config struct {
	// MinWorkers 和 MaxWorkers 是消费者自动扩缩容的固定区间。
	MinWorkers int
	MaxWorkers int
	// QueueSize 只限制等待执行的任务，不包含执行中的任务。
	QueueSize int
	// SubmitTimeout 限制队列满时的等待时间；零值默认 1 秒。
	SubmitTimeout time.Duration
	// MaxRetries 是首次执行失败后的附加尝试次数，范围为 1 到 100。
	MaxRetries int
	// IdleTimeout 为零时使用默认值。
	IdleTimeout time.Duration
	// DrainTimeout 限制停机信号触发后的排空宽限时间；零值默认 15 秒。
	DrainTimeout time.Duration
}

// Stats 是 Pool.Stats 返回的任务池状态快照。
type Stats struct {
	// Pending 是队列中等待执行的任务数，不包含运行中的任务。
	Pending int
	// Running 是正在执行的任务数，重试等待期间仍计入。
	Running int
	// Workers 是尚未退出的消费者数，包含空闲和执行中的消费者。
	Workers int
	// Accepted 是累计接收的任务数，Submit 成功时计入。
	Accepted uint64
	// Succeeded 是累计最终执行成功的任务数。
	Succeeded uint64
	// Failed 是累计执行结束但未成功的任务数。
	Failed uint64
	// Abandoned 是中止排空时累计放弃的未开始任务数。
	Abandoned uint64
}

type task struct {
	name string
	fn   func(context.Context) error
}

// Pool 执行已接收的进程内任务。创建方负责在退出前等待排空完成。
type Pool struct {
	// cfg 是已填入默认超时值的实例配置。
	cfg Config
	// stopCtx 只接收外部停机信号，取消后触发排空。
	stopCtx context.Context
	// ctx 传给任务执行，与 stopCtx 的取消独立。
	ctx context.Context
	// cancel 在中止排空或所有 worker 退出时取消 ctx。
	cancel context.CancelFunc

	// mu 保护队列、索引、统计、状态、terminalCalled 和 space 的替换。
	mu sync.Mutex
	// queue 是固定容量的环形等待队列，不包含执行中的任务。
	queue []task
	// head 指向下一个出队槽位。
	head int
	// tail 指向下一个入队槽位。
	tail int
	// stats 保存实时数量和累计计数，读写均须持有 mu。
	stats Stats
	// state 表示接收任务、排空或中止排空的阶段。
	state poolState
	// terminalCalled 保证 Wait 与 Shutdown 在实例生命周期内合计只调用一次。
	terminalCalled bool
	// ready 提示空闲 worker 可能有新任务，任务本身仍在 queue 中。
	ready chan struct{}
	// space 在满队列出现空位或开始关闭时关闭，唤醒等待提交者。
	space chan struct{}
	// closing 在开始排空时关闭，唤醒空闲 worker。
	closing chan struct{}
	// done 在最后一个 worker 退出时关闭，通知等待关闭的调用方。
	done chan struct{}
	// graceExpired 在停机宽限期结束后关闭，等待方决定是否中止任务。
	graceExpired chan struct{}
}

// New 创建任务池并启动最少数量的消费者；stopCtx 取消时自动开始排空。
func New(stopCtx context.Context, cfg Config) (*Pool, error) {
	if stopCtx == nil || cfg.MinWorkers < 1 || cfg.MaxWorkers < cfg.MinWorkers || cfg.QueueSize < 1 ||
		cfg.MaxRetries < 1 || cfg.MaxRetries > maxRetries ||
		cfg.SubmitTimeout < 0 || cfg.IdleTimeout < 0 || cfg.DrainTimeout < 0 {
		return nil, ErrInvalidConfig
	}
	if err := stopCtx.Err(); err != nil {
		return nil, err
	}
	if cfg.SubmitTimeout == 0 {
		cfg.SubmitTimeout = defaultSubmitTimeout
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	// 停机信号只负责启动排空；任务执行 context 独立，避免信号取消正在运行的任务。
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		cfg:          cfg,
		stopCtx:      stopCtx,
		ctx:          ctx,
		cancel:       cancel,
		queue:        make([]task, cfg.QueueSize),
		ready:        make(chan struct{}, cfg.MaxWorkers),
		space:        make(chan struct{}),
		closing:      make(chan struct{}),
		done:         make(chan struct{}),
		graceExpired: make(chan struct{}),
	}
	p.stats.Workers = cfg.MinWorkers
	for range cfg.MinWorkers {
		go p.worker()
	}
	if stopCtx.Done() != nil {
		go func() {
			select {
			case <-stopCtx.Done():
				// 宽限倒计时从停机信号开始，不随 Wait 或 Shutdown 的调用时间重置。
				graceCtx, cancelGrace := context.WithTimeout(context.Background(), p.cfg.DrainTimeout)
				defer cancelGrace()
				p.beginDrain()
				select {
				case <-p.done:
				case <-graceCtx.Done():
					close(p.graceExpired)
				}
			case <-p.done:
				// 主动关闭完成后退出监听，避免一直等待未取消的停机 context。
			}
		}()
	}
	return p, nil
}

// Submit 等待队列空位并接收任务；ctx 和 SubmitTimeout 只控制提交等待，不控制任务执行。
func (p *Pool) Submit(ctx context.Context, name string, fn func(context.Context) error) error {
	if ctx == nil || name == "" || fn == nil {
		return ErrInvalidTask
	}
	waitCtx := ctx
	timeoutStarted := false
	for {
		p.mu.Lock()
		if p.state == stateOpen && p.stopCtx.Err() != nil {
			p.beginDrainLocked()
		}
		if p.state != stateOpen {
			p.mu.Unlock()
			return ErrClosed
		}
		if err := waitCtx.Err(); err != nil {
			p.mu.Unlock()
			return err
		}
		if p.stats.Pending < p.cfg.QueueSize {
			p.queue[p.tail] = task{name: name, fn: fn}
			p.tail = (p.tail + 1) % len(p.queue)
			p.stats.Pending++
			p.stats.Accepted++
			p.expandLocked()
			select {
			case p.ready <- struct{}{}:
			default:
			}
			p.mu.Unlock()
			return nil
		}
		space := p.space
		p.mu.Unlock()
		if !timeoutStarted {
			var cancel context.CancelFunc
			waitCtx, cancel = context.WithTimeout(ctx, p.cfg.SubmitTimeout)
			defer cancel()
			timeoutStarted = true
		}
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-space:
		}
	}
}

func (p *Pool) beginDrain() {
	p.mu.Lock()
	p.beginDrainLocked()
	p.mu.Unlock()
}

func (p *Pool) beginDrainLocked() {
	// 只在持有 p.mu 时转换状态，统一关闭等待提交者使用的通知通道。
	if p.state != stateOpen {
		return
	}
	p.state = stateDraining
	close(p.closing)
	close(p.space)
}

func (p *Pool) expandLocked() {
	// 等待任务超过当前空闲消费者时再启动新消费者。
	need := p.stats.Pending - (p.stats.Workers - p.stats.Running)
	for need > 0 && p.stats.Workers < p.cfg.MaxWorkers {
		p.stats.Workers++
		go p.worker()
		need--
	}
}

func (p *Pool) worker() {
	for {
		p.mu.Lock()
		if p.stats.Pending > 0 && p.state != stateAborted {
			wasFull := p.stats.Pending == p.cfg.QueueSize
			item := p.queue[p.head]
			p.queue[p.head] = task{}
			p.head = (p.head + 1) % len(p.queue)
			p.stats.Pending--
			p.stats.Running++
			if wasFull && p.state == stateOpen {
				close(p.space)
				p.space = make(chan struct{})
			}
			p.mu.Unlock()
			p.execute(item)
			continue
		}
		if p.state != stateOpen {
			p.workerExitLocked()
			p.mu.Unlock()
			return
		}
		p.mu.Unlock()

		idle := time.NewTimer(p.cfg.IdleTimeout)
		select {
		case <-p.ready:
		case <-p.closing:
		case <-idle.C:
			p.mu.Lock()
			if p.state == stateOpen && p.stats.Pending == 0 && p.stats.Workers > p.cfg.MinWorkers {
				p.workerExitLocked()
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()
		}
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
	}
}

func (p *Pool) workerExitLocked() {
	p.stats.Workers--
	if p.state != stateOpen && p.stats.Workers == 0 {
		p.cancel()
		close(p.done)
	}
}

func (p *Pool) abortOnDrainTimeout() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 已无未完成任务时只等待 worker 收尾，不把收尾时间算作任务超时。
	if p.state == stateAborted || p.stats.Pending+p.stats.Running == 0 {
		return nil
	}
	unfinished := p.stats.Pending + p.stats.Running
	err := fmt.Errorf("taskpool: shutdown unfinished=%d: %w", unfinished, context.DeadlineExceeded)
	p.state = stateAborted
	p.stats.Abandoned += uint64(p.stats.Pending)
	for i := range p.queue {
		p.queue[i] = task{}
	}
	p.stats.Pending = 0
	p.cancel()
	return err
}

func (p *Pool) execute(item task) {
	ctx := p.ctx
	attempts := p.cfg.MaxRetries + 1
	succeeded := false
	for attempt := 1; attempt <= attempts; attempt++ {
		err, stack := invoke(ctx, item.fn)
		if err == nil {
			succeeded = true
			break
		}
		final := stack != nil || attempt == attempts || ctx.Err() != nil
		reportFailure(item.name, attempt, attempts, err, stack, final)
		if final {
			break
		}
		timer := time.NewTimer(retryDelay)
		select {
		case <-timer.C:
		case <-ctx.Done():
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		if err := ctx.Err(); err != nil {
			reportFailure(item.name, attempt, attempts, err, nil, true)
			break
		}
	}
	p.mu.Lock()
	p.stats.Running--
	if succeeded {
		p.stats.Succeeded++
	} else {
		p.stats.Failed++
	}
	p.mu.Unlock()
}

func invoke(ctx context.Context, fn func(context.Context) error) (err error, stack []byte) {
	defer func() {
		if value := recover(); value != nil {
			err = fmt.Errorf("panic: %v", value)
			stack = debug.Stack()
		}
	}()
	return fn(ctx), nil
}

func reportFailure(name string, attempt, attempts int, err error, stack []byte, final bool) {
	status := "retry"
	if final {
		status = "final"
	}
	record := fmt.Appendf(nil, "taskpool: task=%q attempt=%d/%d status=%s error=%q\n", name, attempt, attempts, status, err.Error())
	record = append(record, stack...)
	// 错误行与堆栈一次写入，避免并发 worker 的记录交叉。
	_, _ = os.Stderr.Write(record)
}

// Stats 返回当前数量与累计结果的快照。
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stats
}

func (p *Pool) claimTerminal() {
	p.mu.Lock()
	if p.terminalCalled {
		p.mu.Unlock()
		panic("taskpool: Wait or Shutdown called more than once")
	}
	p.terminalCalled = true
	p.mu.Unlock()
}

// Shutdown 主动开始排空并等待所有 worker 退出；与 Wait 合计只能调用一次。
func (p *Pool) Shutdown() error {
	p.claimTerminal()
	p.beginDrain()
	<-p.done
	return nil
}

// Wait 等待停机排空完成或宽限期到期；与 Shutdown 合计只能调用一次。
func (p *Pool) Wait() error {
	p.claimTerminal()
	select {
	case <-p.done:
		return nil
	case <-p.graceExpired:
		if err := p.abortOnDrainTimeout(); err != nil {
			return err
		}
		// 宽限期可能先于最后一个空闲 worker 退出；成功返回仍需等 done。
		<-p.done
		return nil
	}
}
