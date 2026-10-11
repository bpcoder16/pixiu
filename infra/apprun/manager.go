package apprun

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

var (
	ErrStarted          = errors.New("apprun: manager already started")
	ErrNilContext       = errors.New("apprun: nil context")
	ErrNoServices       = errors.New("apprun: no services registered")
	ErrInvalidService   = errors.New("apprun: invalid service")
	ErrDuplicateService = errors.New("apprun: duplicate service name")
	ErrUnexpectedExit   = errors.New("apprun: service exited before shutdown")
	// ErrShutdownTimeout 表示总关闭期限耗尽；尚未结束的回调可能继续执行。
	ErrShutdownTimeout = errors.New("apprun: shutdown timeout")
)

const defaultShutdownTimeout = 20 * time.Second

// Service 描述一个常驻服务。两个回调均必填。
// Run 必须持续运行至 Shutdown 请求停止或发生错误，正常关闭时返回 nil。
// Run 不得只启动后台 goroutine 就返回；未发起关闭时返回 nil 会被视为 ErrUnexpectedExit。
// Shutdown 必须允许与 Run 并发及先于 Run 执行，关闭后的 Run 不得重新启动服务。
type Service struct {
	Name     string
	Run      func() error
	Shutdown func() error
}

// Manager 并发运行常驻服务，按登记逆序关闭。零值可用，使用后不可复制。
// 自定义关闭期限须通过 New 设置，创建后不可修改。
// 依赖先登记、使用依赖的生产者后登记；登记成功的服务由本次 Run 负责关闭。
type Manager struct {
	// shutdownTimeout 是全部服务共享的关闭期限，不包含 Run 返回后的资源清理时间。
	shutdownTimeout time.Duration

	mu       sync.Mutex
	services []Service
	started  bool
}

// New 创建管理器并固定总关闭期限；零值默认 20 秒，负值返回错误。
func New(shutdownTimeout time.Duration) (*Manager, error) {
	if shutdownTimeout < 0 {
		return nil, fmt.Errorf("apprun: negative shutdown timeout: %s", shutdownTimeout)
	}
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	return &Manager{shutdownTimeout: shutdownTimeout}, nil
}

// PanicError 保存运行或关闭回调的 panic；立即记录堆栈,并通过返回错误交给调用方。
type PanicError struct {
	Service   string
	Operation string
	Value     any
	Stack     []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("apprun: service %q %s panic: %v", e.Service, e.Operation, e.Value)
}

// Register 登记名称唯一的服务；运行开始后不能追加登记。
func (m *Manager) Register(service Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return ErrStarted
	}
	if service.Name == "" || strings.TrimSpace(service.Name) != service.Name || service.Run == nil || service.Shutdown == nil {
		return fmt.Errorf("%w: %q requires a name, Run and Shutdown", ErrInvalidService, service.Name)
	}
	for _, existing := range m.services {
		if existing.Name == service.Name {
			return fmt.Errorf("%w: %q", ErrDuplicateService, service.Name)
		}
	}
	m.services = append(m.services, service)
	return nil
}

// Run 阻塞至 ctx 请求停机或任一服务退出，并完成逆序关闭与等待。
// ctx 只通知管理器，不控制业务工作；取消本身不作为错误返回。
// 预取消也会关闭已登记服务。全部关闭及退出等待共享总关闭期限，本方法不强制终止回调。
// 总期限耗尽返回 ErrShutdownTimeout，停止发起后续关闭；未退出任务可能与调用方清理并发。
// Shutdown 错误或 panic 均在剩余总关闭期限内等待 Run 返回，并保留原始错误。
func (m *Manager) Run(ctx context.Context) error {
	return m.run(ctx, nil)
}

type serviceRun struct {
	service  Service
	stopping atomic.Bool
	done     chan struct{}
	err      error // 写入后关闭 done；关闭协程只在收到 done 后读取。
}

func (m *Manager) run(ctx context.Context, onStop func()) error {
	if ctx == nil {
		return ErrNilContext
	}
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return ErrStarted
	}
	if len(m.services) == 0 {
		m.mu.Unlock()
		return ErrNoServices
	}
	shutdownTimeout := m.shutdownTimeout
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	m.started = true
	services := m.services
	m.services = nil
	m.mu.Unlock()

	logit.Info(ctx, "AppRunStart", logit.Int("services", len(services)))
	runs := make([]*serviceRun, len(services))
	// 每个服务最多发送一次，即使已经进入关闭流程也不会卡住退出协程。
	exited := make(chan struct{}, len(services))
	for i, service := range services {
		state := &serviceRun{
			service: service,
			done:    make(chan struct{}),
		}
		runs[i] = state
		if ctx.Err() != nil {
			close(state.done)
			continue
		}
		go func() {
			err := call(ctx, service.Name, "run", service.Run)
			if err == nil && !state.stopping.Load() {
				err = ErrUnexpectedExit
			}
			state.err = err
			close(state.done)
			exited <- struct{}{}
		}()
	}

	reason := "service_exit"
	select {
	case <-ctx.Done():
		reason = context.Cause(ctx).Error()
	case <-exited:
	}
	// 使用独立 context，避免停机通知的取消立即耗尽整个收尾期限。
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// 信号订阅须在阻塞收尾前释放，让后续中断恢复原有行为。
	if onStop != nil {
		onStop()
	}
	logit.Info(ctx, "AppRunStopping", logit.Str("reason", reason))
	var errs []error
	for i, state := range slices.Backward(runs) {
		if shutdownCtx.Err() != nil {
			return shutdownTimedOut(ctx, runs[:i+1], "shutdown", nil, errs, nil)
		}
		started := time.Now()
		state.stopping.Store(true)
		// 回调可能不返回；缓冲结果让总超时返回后的迟到回调也能完成。
		shutdownDone := make(chan error, 1)
		go func() {
			shutdownDone <- call(ctx, state.service.Name, "shutdown", state.service.Shutdown)
		}()
		var shutdownErr error
		select {
		case shutdownErr = <-shutdownDone:
		case <-shutdownCtx.Done():
			return shutdownTimedOut(ctx, runs[:i+1], "shutdown", nil, errs, shutdownDone)
		}
		// Shutdown 返回不代表 Run 已经完成；先等待当前生产者，再释放后续依赖。
		select {
		case <-state.done:
		case <-shutdownCtx.Done():
			return shutdownTimedOut(ctx, runs[:i+1], "run", shutdownErr, errs, nil)
		}
		for _, result := range []struct {
			operation string
			err       error
		}{
			{operation: "run", err: state.err},
			{operation: "shutdown", err: shutdownErr},
		} {
			if result.err != nil {
				errs = append(errs, fmt.Errorf("apprun: service %q %s: %w", state.service.Name, result.operation, result.err))
			}
		}
		serviceErr := errors.Join(state.err, shutdownErr)
		level := logit.InfoLevel
		if serviceErr != nil {
			level = logit.ErrorLevel
		}
		logit.Output(ctx, level, 0, "AppServiceShutdown",
			logit.Str("service", state.service.Name),
			logit.Dur("duration", time.Since(started)),
			logit.Err(serviceErr),
		)
	}
	err := errors.Join(errs...)
	logit.Info(ctx, "AppRunStopped", logit.Err(err))
	return err
}

func shutdownTimedOut(ctx context.Context, pending []*serviceRun, operation string, shutdownErr error, errs []error, shutdownDone <-chan error) error {
	// 超时与关闭结果可能同时就绪；只收集已到达的结果，不延长关闭期限。
	select {
	case shutdownErr = <-shutdownDone:
	default:
	}
	errs = appendCompletedRunErrors(errs, pending)
	state := pending[len(pending)-1]
	if shutdownErr != nil {
		errs = append(errs, fmt.Errorf("apprun: service %q shutdown: %w", state.service.Name, shutdownErr))
	}
	errs = append(errs, fmt.Errorf("%w: service %q %s: %w", ErrShutdownTimeout, state.service.Name, operation, context.DeadlineExceeded))
	err := errors.Join(errs...)
	logit.Error(ctx, "AppRunShutdownTimeout", logit.Err(err))
	return err
}

func appendCompletedRunErrors(errs []error, pending []*serviceRun) []error {
	// 提前返回时仍保留已发布的 Run 错误；不等待未退出回调，也不读取其错误字段。
	for _, state := range pending {
		select {
		case <-state.done:
			if state.err != nil {
				errs = append(errs, fmt.Errorf("apprun: service %q run: %w", state.service.Name, state.err))
			}
		default:
		}
	}
	return errs
}

func call(ctx context.Context, name, operation string, fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			panicErr := &PanicError{
				Service:   name,
				Operation: operation,
				Value:     value,
				Stack:     debug.Stack(),
			}
			err = panicErr
			// 在任何后续等待之前输出,避免收尾阻塞掩盖最初的 panic。
			logit.Error(ctx, "AppServicePanic", logit.Err(panicErr), logit.Str("stack", string(panicErr.Stack)))
		}
	}()
	return fn()
}
