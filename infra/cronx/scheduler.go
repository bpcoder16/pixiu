package cronx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-co-op/gocron/v2"
)

// 极短间隔可能使引擎持续追赶时间；在创建引擎前统一拒绝。
const minimumInterval = 100 * time.Millisecond

var (
	ErrStarted = errors.New("cronx: scheduler already started")
	ErrClosed  = errors.New("cronx: scheduler closed")
)

// Handler 必须在返回前等待本轮全部工作与清理结束，取消只作为协作停止信号。
type Handler func(context.Context) error

// RandomInterval 表示每轮重新抽取的 [Min, Max) 间隔，要求 100ms <= Min < Max。
type RandomInterval struct {
	Min time.Duration
	Max time.Duration
}

// Job 定义稳定的任务名与一种调度方式；Timeout 从进入 Handler 开始计时。
// 固定间隔 Every 至少为 100ms，RandomEvery 的下限遵循 RandomInterval。
type Job struct {
	Name         string
	Cron         string
	Every        time.Duration
	RandomEvery  *RandomInterval
	Timeout      time.Duration
	SuccessLevel logit.Level
	Handler      Handler
}

// Config 只包含运行配置；文件解析、环境与 Redis 客户端由调用方组合。
type Config struct {
	// Location 为 nil 时加载 Asia/Shanghai，失败则返回构造错误；非 nil 时保留对象的完整规则。
	Location   *time.Location
	LoggerName string
	Locker     gocron.Locker
}

// Scheduler 由 New 创建，使用后不可复制；一个实例只运行一次。
type Scheduler struct {
	engine gocron.Scheduler
	jobs   []Job
	ctx    context.Context
	cancel context.CancelFunc

	// 接纳任务及锁获取时先在 mu 下登记 work；关闭同一把门后才允许 Wait。
	mu       sync.Mutex
	started  bool
	closing  bool
	work     sync.WaitGroup
	closeErr error
	// closeDone 在停收时关闭，让 Run 退出；actualDone 等待任务、解锁及日志实际结束。
	closeDone     chan struct{}
	actualDone    chan struct{}
	engineStopped chan struct{}
}

// New 校验并复制全部任务，不执行业务；引擎构造会创建内部 goroutine。
func New(cfg Config, jobs ...Job) (*Scheduler, error) {
	if cfg.Location == nil {
		location, err := time.LoadLocation("Asia/Shanghai")
		if err != nil {
			return nil, fmt.Errorf("cronx: load default location: %w", err)
		}
		cfg.Location = location
	}
	s := &Scheduler{
		jobs:          make([]Job, 0, len(jobs)),
		closeDone:     make(chan struct{}),
		actualDone:    make(chan struct{}),
		engineStopped: make(chan struct{}),
	}
	definitions := make([]gocron.JobDefinition, len(jobs))
	names := make(map[string]struct{}, len(jobs))
	for i, job := range jobs {
		definition, err := prepareJob(&job, cfg.Locker != nil)
		if err != nil {
			return nil, fmt.Errorf("cronx: job %q: %w", job.Name, err)
		}
		if _, exists := names[job.Name]; exists {
			return nil, fmt.Errorf("cronx: duplicate job %q", job.Name)
		}
		s.jobs = append(s.jobs, job)
		names[job.Name] = struct{}{}
		definitions[i] = definition
	}
	s.ctx, s.cancel = context.WithCancel(logit.WithLoggerName(context.Background(), cfg.LoggerName))
	options := []gocron.SchedulerOption{
		gocron.WithLocation(cfg.Location),
		gocron.WithSchedulerMonitor(engineLifecycle{stopped: s.engineStopped}),
	}
	if cfg.Locker != nil {
		options = append(options, gocron.WithDistributedLocker(&trackedLocker{
			s:      s,
			locker: cfg.Locker,
		}))
	}
	engine, err := gocron.NewScheduler(options...)
	if err != nil {
		s.cancel()
		return nil, fmt.Errorf("cronx: create scheduler: %w", err)
	}
	s.engine = engine
	for i, job := range s.jobs {
		jobOptions := []gocron.JobOption{
			gocron.WithName(job.Name),
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
		}
		if job.Cron != "" {
			jobOptions = append(jobOptions, gocron.WithCronImplementation(&locationCron{}))
		}
		_, err = engine.NewJob(definitions[i], gocron.NewTask(func() { s.execute(job) }), jobOptions...)
		if err != nil {
			s.Cancel()
			_ = s.Wait(context.Background())
			return nil, fmt.Errorf("cronx: register job %q: %w", job.Name, err)
		}
	}
	return s, nil
}

func prepareJob(job *Job, locked bool) (gocron.JobDefinition, error) {
	if job.Name == "" || strings.TrimSpace(job.Name) != job.Name {
		return nil, errors.New("invalid name")
	}
	if job.Handler == nil {
		return nil, errors.New("nil handler")
	}
	if job.Timeout < 0 || locked && job.Timeout == 0 {
		return nil, errors.New("timeout must be nonnegative, and positive when using a locker")
	}
	if job.SuccessLevel == 0 {
		job.SuccessLevel = logit.InfoLevel
	}
	if job.SuccessLevel != logit.InfoLevel && job.SuccessLevel != logit.DebugLevel {
		return nil, errors.New("successLevel must be info or debug")
	}
	count := 0
	var definition gocron.JobDefinition
	if job.Cron != "" {
		count++
		if len(strings.Fields(job.Cron)) != 6 || strings.Contains(job.Cron, "=") || strings.Contains(job.Cron, "@") {
			return nil, errors.New("cron must contain exactly six fields without timezone or descriptor")
		}
		definition = gocron.CronJob(job.Cron, true)
	}
	if job.Every != 0 {
		count++
		if job.Every < minimumInterval {
			return nil, errors.New("every must be at least 100ms")
		}
		definition = gocron.DurationJob(job.Every)
	}
	if job.RandomEvery != nil {
		count++
		interval := *job.RandomEvery
		if interval.Min < minimumInterval || interval.Min >= interval.Max {
			return nil, errors.New("randomEvery requires 100ms <= min < max")
		}
		job.RandomEvery = &interval
		definition = gocron.DurationRandomJob(interval.Min, interval.Max)
	}
	if count != 1 {
		return nil, errors.New("exactly one of cron, every, randomEvery is required")
	}
	return definition, nil
}

// Run 启动调度并阻塞到停收；实际任务结束由 Wait 确认。
func (s *Scheduler) Run() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrStarted
	}
	s.started = true
	// 与 Stop 串行完成启动交接，避免关闭后的引擎又被迟到的 Start 启动。
	s.engine.Start()
	// 启动日志纳入实际工作；独立写入使慢 Writer 不阻止 Run 按关闭预算返回。
	s.work.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.work.Done()
		logit.Info(s.ctx, "CronStart", logit.Int("jobs", len(s.jobs)))
	}()
	<-s.closeDone
	return nil
}

func (s *Scheduler) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.work.Add(1)
	return true
}

// Stop 立即封闭任务与获锁入口，并异步停止引擎；不取消业务 context。
func (s *Scheduler) Stop() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	close(s.closeDone)
	s.mu.Unlock()
	go func() {
		err := s.engine.ShutdownWithContext(context.Background())
		<-s.engineStopped
		s.work.Wait()
		// 引擎自身等待超时不能代表业务失败；实际任务与解锁计数已在这里完成。
		if errors.Is(err, gocron.ErrStopJobsTimedOut) {
			err = nil
		}
		s.closeErr = err
		logit.Info(s.ctx, "CronStopped", logit.Err(err))
		s.cancel()
		close(s.actualDone)
	}()
	return nil
}

// Wait 等待引擎、业务、锁及日志实际收尾；ctx 到期不代替业务完成，也不自动取消业务。
func (s *Scheduler) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("cronx: nil wait context")
	}
	select {
	case <-s.actualDone:
		return s.closeErr
	default:
	}
	select {
	case <-s.actualDone:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel 先停收，再协作取消剩余业务；任务实际返回前仍不释放执行位置或锁。
func (s *Scheduler) Cancel() {
	_ = s.Stop()
	s.cancel()
}
