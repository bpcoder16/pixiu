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
	// SubmitTimeout 是从参数校验通过后开始计算的提交超时预算；零值默认 1 秒。
	SubmitTimeout time.Duration
	// MaxRetries 是首次执行失败后的附加尝试次数，范围为 0 到 100；零值不重试。
	MaxRetries int
	// IdleTimeout 为零时使用默认值。
	IdleTimeout time.Duration
	// DrainTimeout 限制首次 Shutdown 触发后的排空宽限时间；零值默认 15 秒。
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

// Pool 执行已接收的进程内任务。创建方负责在退出前调用 Shutdown。
type Pool struct {
	// cfg 是已填入默认超时值的实例配置。
	cfg Config
	// ctx 由池管理，传给任务执行并通知 Shutdown 结束等待。
	ctx context.Context
	// cancel 在中止排空或所有 worker 退出时取消 ctx，同时唤醒 Shutdown。
	cancel context.CancelFunc

	// mu 保护队列、索引、统计、状态、关闭结果、定时器和 space 的替换。
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
	// drainTimer 只在首次开始排空时创建，全部 worker 退出后停止。
	drainTimer *time.Timer
	// shutdownErr 保存排空超时的结果，任务随后退出也不清除。
	shutdownErr error
	// ready 提示空闲 worker 可能有新任务，任务本身仍在 queue 中。
	ready chan struct{}
	// space 在满队列出现空位或开始关闭时关闭，唤醒等待提交者。
	space chan struct{}
	// closing 在开始排空时关闭，唤醒空闲 worker。
	closing chan struct{}
	// done 在最后一个 worker 退出时关闭，通知等待关闭的调用方。
	done chan struct{}
}

// New 创建任务池并启动最少数量的消费者；调用方通过 Shutdown 发起关闭。
func New(cfg Config) (*Pool, error) {
	if cfg.MinWorkers < 1 || cfg.MaxWorkers < cfg.MinWorkers || cfg.QueueSize < 1 ||
		cfg.MaxRetries < 0 || cfg.MaxRetries > maxRetries ||
		cfg.SubmitTimeout < 0 || cfg.IdleTimeout < 0 || cfg.DrainTimeout < 0 {
		return nil, ErrInvalidConfig
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
	// 任务执行 context 由池统一取消，开始排空时仍允许已接收任务正常执行。
	ctx, cancel := context.WithCancel(context.Background())
	p := &Pool{
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
		queue:   make([]task, cfg.QueueSize),
		ready:   make(chan struct{}, cfg.MaxWorkers),
		space:   make(chan struct{}),
		closing: make(chan struct{}),
		done:    make(chan struct{}),
	}
	p.stats.Workers = cfg.MinWorkers
	for range cfg.MinWorkers {
		go p.worker()
	}
	return p, nil
}

// Submit 等待队列空位并接收任务；ctx 和 SubmitTimeout 只控制提交过程，不控制任务执行。
// 获取锁与等待空位共享超时预算；锁等待不可中断，可能在取得锁后才返回超时。
func (p *Pool) Submit(ctx context.Context, name string, fn func(context.Context) error) error {
	if ctx == nil || name == "" || fn == nil {
		return ErrInvalidTask
	}
	waitCtx, cancel := context.WithTimeout(ctx, p.cfg.SubmitTimeout)
	defer cancel()
	for {
		p.mu.Lock()
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
		select {
		case <-waitCtx.Done():
			return waitCtx.Err()
		case <-space:
		}
	}
}

func (p *Pool) beginDrain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 只在持有 p.mu 时转换状态，统一关闭等待提交者使用的通知通道。
	if p.state != stateOpen {
		return
	}
	p.state = stateDraining
	// 重复或并发 Shutdown 共用一次倒计时，超时中止不依赖调用 Wait。
	p.drainTimer = time.AfterFunc(p.cfg.DrainTimeout, p.abortOnDrainTimeout)
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
		idle.Stop()
	}
}

func (p *Pool) workerExitLocked() {
	p.stats.Workers--
	if p.state != stateOpen && p.stats.Workers == 0 {
		p.drainTimer.Stop()
		p.cancel()
		close(p.done)
	}
}

func (p *Pool) abortOnDrainTimeout() {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 已无未完成任务时只等待 worker 收尾，不把收尾时间算作任务超时。
	if p.state != stateDraining || p.stats.Pending+p.stats.Running == 0 {
		return
	}
	unfinished := p.stats.Pending + p.stats.Running
	p.shutdownErr = fmt.Errorf("taskpool: shutdown unfinished=%d: %w", unfinished, context.DeadlineExceeded)
	p.state = stateAborted
	p.stats.Abandoned = uint64(p.stats.Pending)
	clear(p.queue)
	p.stats.Pending = 0
	p.cancel()
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
		timer.Stop()
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

func (p *Pool) shutdownResult() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.shutdownErr
}

// Shutdown 开始排空并等待全部 worker 退出或宽限到期；允许重复或并发调用。
// 超时会自动取消任务，但不保证任务已经退出；需要确认收尾时继续调用 Wait。
func (p *Pool) Shutdown() error {
	p.beginDrain()
	<-p.ctx.Done()
	// 正常排空也会取消 ctx，仍通过保存的关闭结果区分正常结束与超时。
	return p.shutdownResult()
}

// Wait 可选地等待全部 worker 退出并返回关闭结果，不发起关闭，也不限制等待时间。
// 允许重复或并发调用；排空曾超时时，即使任务随后退出也仍返回该超时错误。
func (p *Pool) Wait() error {
	<-p.done
	return p.shutdownResult()
}
