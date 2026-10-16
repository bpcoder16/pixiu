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
	// DrainTimeout 为零时使用总关闭期限的 2/3，默认 10 秒。
	DrainTimeout time.Duration
	// ShutdownTimeout 为零时默认 15 秒，与 HTTP 一致。
	ShutdownTimeout time.Duration
	Locker          gocron.Locker
}

// Scheduler 由 New 创建，使用后不可复制；一个实例只运行一次。
type Scheduler struct {
	config Config
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
	// closeDone 在有界关闭结束并发布 closeErr 后关闭，供 Run 和重复 Shutdown 等待。
	closeDone chan struct{}
	// actualDone 在引擎停止且任务、锁操作和启动日志全部结束后关闭，供实际排空等待。
	actualDone chan struct{}
	// shutdownLogged 在关闭日志写入结束后关闭；Shutdown 按剩余预算等待，Wait 等到实际结束。
	shutdownLogged chan struct{}
	// engineStopped 在收到引擎停止回调时关闭，供关闭流程在等待 work 前确认引擎停止调度。
	engineStopped chan struct{}
}

// New 校验并复制全部任务，不执行业务；引擎构造会创建内部 goroutine。
func New(cfg Config, jobs ...Job) (*Scheduler, error) {
	if cfg.ShutdownTimeout < 0 || cfg.DrainTimeout < 0 {
		return nil, errors.New("cronx: negative shutdownTimeout or drainTimeout")
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 15 * time.Second
	}
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = cfg.ShutdownTimeout / 3 * 2
	}
	if cfg.DrainTimeout <= 0 || cfg.DrainTimeout >= cfg.ShutdownTimeout {
		return nil, errors.New("cronx: drainTimeout must be positive and less than shutdownTimeout")
	}
	if cfg.Location == nil {
		location, err := time.LoadLocation("Asia/Shanghai")
		if err != nil {
			return nil, fmt.Errorf("cronx: load default location: %w", err)
		}
		cfg.Location = location
	}
	s := &Scheduler{
		config:         cfg,
		jobs:           make([]Job, 0, len(jobs)),
		closeDone:      make(chan struct{}),
		actualDone:     make(chan struct{}),
		shutdownLogged: make(chan struct{}),
		engineStopped:  make(chan struct{}),
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
		gocron.WithStopTimeout(cfg.ShutdownTimeout),
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
			closeErr := s.Shutdown()
			s.Wait()
			return nil, errors.Join(fmt.Errorf("cronx: register job %q: %w", job.Name, err), closeErr)
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

// Run 启动调度并阻塞到 Shutdown 的有界关闭结束；任务错误不会结束服务。
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
	// 与 Shutdown 串行完成启动交接，避免关闭后的引擎又被迟到的 Start 启动。
	s.engine.Start()
	// 启动日志纳入实际工作；独立写入使慢 Writer 不阻止 Run 按关闭预算返回。
	s.work.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.work.Done()
		logit.Info(s.ctx, "CronStart", logit.Int("jobs", len(s.jobs)))
	}()
	<-s.closeDone
	return s.closeErr
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

// Shutdown 立即停收，先自然排空，再取消业务；全部阶段共用首次调用的总预算。
// 超时不能强杀 Handler，重复调用返回同一次结果，不重新计时。
func (s *Scheduler) Shutdown() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		<-s.closeDone
		return s.closeErr
	}
	s.closing = true
	started := time.Now()
	s.mu.Unlock()

	ctx, cancel := context.WithDeadline(context.Background(), started.Add(s.config.ShutdownTimeout))
	defer cancel()
	drain := time.NewTimer(time.Until(started.Add(s.config.DrainTimeout)))
	defer drain.Stop()
	// 引擎返回超时不代表它已停止派发，更不代表业务与锁已经结束。
	// actualDone 只在引擎停止通知和实际工作计数都完成后关闭，供 Wait 使用。
	var engineErr error
	go func() {
		engineErr = s.engine.ShutdownWithContext(ctx)
		<-s.engineStopped
		s.work.Wait()
		close(s.actualDone)
	}()
	var err error
	select {
	case <-s.actualDone:
		err = engineErr
	case <-drain.C:
		s.cancel()
		select {
		case <-s.actualDone:
			err = engineErr
		case <-ctx.Done():
			err = ctx.Err()
		}
	case <-ctx.Done():
		err = ctx.Err()
	}
	s.cancel()
	// 写入使用排空结果快照，避免日志与下面发布的最终关闭错误并发读写。
	// 慢 Writer 仍由 Wait 等待，但不能绕过 Shutdown 剩余的总预算。
	go func(drainErr error) {
		defer close(s.shutdownLogged)
		if drainErr != nil {
			logit.Error(s.ctx, "CronShutdown", logit.Err(drainErr), logit.Dur("duration", time.Since(started)))
		} else {
			logit.Info(s.ctx, "CronShutdown", logit.Dur("duration", time.Since(started)))
		}
	}(err)
	select {
	case <-s.shutdownLogged:
	case <-ctx.Done():
	}
	if ctxErr := ctx.Err(); ctxErr != nil && !errors.Is(err, ctxErr) {
		err = errors.Join(err, ctxErr)
	}
	s.closeErr = err
	close(s.closeDone)
	return err
}

// Wait 不发起关闭，无限等待实际工作和日志收尾；忽略取消的任务或阻塞的 Writer 可能永不结束。
func (s *Scheduler) Wait() {
	<-s.closeDone
	<-s.actualDone
	<-s.shutdownLogged
}
