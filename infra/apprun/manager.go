package apprun

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
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
	// ErrShutdownTimeout 表示统一停收与排空期限耗尽；尚未结束的回调可能继续执行。
	ErrShutdownTimeout = errors.New("apprun: shutdown timeout")
)

const defaultShutdownTimeout = 30 * time.Second

// Service 描述常驻服务，全部回调必填。Stop 只封闭入口并启动排空，Wait 等待实际工作结束。
// Wait 返回 nil 才确认排空；错误或 panic 保留，管理器返回前仍会调用 Cancel 兜底。
// Cancel 只发出协作取消通知，必须立即返回；超时不保证任意 Go 任务已经终止。
// Stop/Cancel 可先于或并发于 Run，停收后 Run 不得重新启动。Run 不得只启动后台任务就返回。
type Service struct {
	Name   string
	Run    func() error
	Stop   func() error
	Wait   func(context.Context) error
	Cancel func()
}

// Manager 并发运行常驻服务，统一停收并并行排空。零值可用，使用后不可复制。
// 自定义关闭期限须通过 New 设置，创建后不可修改。
// 登记顺序不代表启动依赖或停机顺序；登记成功的服务由本次 Run 负责停收。
type Manager struct {
	// shutdownTimeout 是全部服务共享的关闭期限，不包含 Run 返回后的资源清理时间。
	shutdownTimeout time.Duration

	mu       sync.Mutex
	services []Service
	started  bool
}

// New 创建管理器并固定总关闭期限；零值默认 30 秒，负值返回错误。
func New(shutdownTimeout time.Duration) (*Manager, error) {
	if shutdownTimeout < 0 {
		return nil, fmt.Errorf("apprun: negative shutdown timeout: %s", shutdownTimeout)
	}
	if shutdownTimeout == 0 {
		shutdownTimeout = defaultShutdownTimeout
	}
	return &Manager{shutdownTimeout: shutdownTimeout}, nil
}

// PanicError 保存运行或关闭回调的 panic；保留堆栈并通过返回错误交给调用方。
type PanicError struct {
	Service   string
	Operation string
	Value     any
	Stack     []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("apprun: service %q %s panic: %v", e.Service, e.Operation, e.Value)
}

// serviceError 延后生成错误文本，避免自定义 Error/Format 阻塞关闭控制协程。
type serviceError struct {
	name      string
	operation string
	err       error
}

func (e *serviceError) Error() string {
	return fmt.Sprintf("apprun: service %q %s: %v", e.name, e.operation, e.err)
}

func (e *serviceError) Unwrap() error { return e.err }

// Register 登记名称唯一的服务；运行开始后不能追加登记。
func (m *Manager) Register(service Service) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.started {
		return ErrStarted
	}
	if service.Name == "" || strings.TrimSpace(service.Name) != service.Name || service.Run == nil || service.Stop == nil || service.Wait == nil || service.Cancel == nil {
		return fmt.Errorf("%w: %q requires a name, Run, Stop, Wait and Cancel", ErrInvalidService, service.Name)
	}
	for _, existing := range m.services {
		if existing.Name == service.Name {
			return fmt.Errorf("%w: %q", ErrDuplicateService, service.Name)
		}
	}
	m.services = append(m.services, service)
	return nil
}

// Run 等待 ctx 取消或任一服务退出，统一停收并在同一个期限内等待全部服务排空。
// ctx 只通知管理器，不提前取消业务；预取消也会停收已登记服务，但不启动 Run。
// 超时通知未完成服务 Cancel 并返回 ErrShutdownTimeout；入口随后有界回收资源并退出。
func (m *Manager) Run(ctx context.Context) error { return m.run(ctx, nil) }

type serviceRun struct {
	service  Service
	stopping atomic.Bool
	done     chan struct{}
	drained  chan struct{}
	err      error // 仅在 done 关闭后读取。
}

type shutdownResult struct {
	name      string
	operation string
	err       error
	finished  bool
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
	timeout := m.shutdownTimeout
	if timeout == 0 {
		timeout = defaultShutdownTimeout
	}
	m.started = true
	services := m.services
	m.services = nil
	m.mu.Unlock()

	runs := make([]*serviceRun, len(services))
	exited := make(chan struct{}, len(services))
	for i, service := range services {
		state := &serviceRun{
			service: service,
			done:    make(chan struct{}),
			drained: make(chan struct{}),
		}
		runs[i] = state
		if ctx.Err() != nil {
			close(state.done)
			continue
		}
		go func() {
			err := call(service.Name, "run", service.Run)
			if err == nil && !state.stopping.Load() {
				err = ErrUnexpectedExit
			}
			state.err = err
			close(state.done)
			exited <- struct{}{}
		}()
	}
	select {
	case <-ctx.Done():
	case <-exited:
	}
	drainCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	deadline, _ := drainCtx.Deadline()
	if onStop != nil {
		onStop()
	}
	// 先标记全部服务，再发停收，避免先停服务引起其他 Run 返回被误判为意外退出。
	for _, state := range runs {
		state.stopping.Store(true)
	}
	results := make(chan shutdownResult, 3*len(runs))
	pending := make(map[string]string, len(runs))
	for _, state := range runs {
		pending[state.service.Name] = "stop"
		go func() {
			started := time.Now()
			stopErr := call(state.service.Name, "stop", state.service.Stop)
			results <- shutdownResult{
				name:      state.service.Name,
				operation: "stop",
				err:       stopErr,
			}
			waitErr := call(state.service.Name, "wait", func() error { return state.service.Wait(drainCtx) })
			results <- shutdownResult{
				name:      state.service.Name,
				operation: "wait",
				err:       waitErr,
			}
			// Run 的返回和实际业务排空分别确认，不能用监听器退出代替 Handler 完成。
			select {
			case <-state.done:
			case <-drainCtx.Done():
				return
			}
			if drainCtx.Err() != nil {
				return
			}
			logit.Info(ctx, "AppServiceStopped", logit.Str("service", state.service.Name), logit.Dur("duration", time.Since(started)), logit.Err(errors.Join(stopErr, waitErr, state.err)))
			// 只发布期限内完成的结果，避免日志超时后的迟到结果掩盖真正的超时。
			if !time.Now().Before(deadline) {
				return
			}
			// Wait 失败不能证明业务已结束，返回前仍需发出取消通知。
			if waitErr == nil {
				close(state.drained)
			}
			results <- shutdownResult{
				name:     state.service.Name,
				finished: true,
			}
		}()
	}
	var errs []error
	remaining := len(runs)
	collect := func(result shutdownResult) {
		if result.err != nil {
			errs = append(errs, &serviceError{
				name:      result.name,
				operation: result.operation,
				err:       result.err,
			})
		}
		// 只由收集协程根据已发布的结果更新进度，避免读取回调中的未同步状态。
		switch result.operation {
		case "stop":
			pending[result.name] = "wait"
		case "wait":
			pending[result.name] = "run"
		}
		if result.finished {
			delete(pending, result.name)
			remaining--
		}
	}
	for remaining > 0 && drainCtx.Err() == nil {
		select {
		case result := <-results:
			collect(result)
		case <-drainCtx.Done():
		}
	}
	// 只读取已经发布的结果，迟到发送由缓冲承接，不延长退出期限。
	for len(results) > 0 {
		collect(<-results)
	}
	// 计时器可能在结果入队后、收集完成前到期；已全部完成时不再追加超时错误。
	if remaining > 0 && drainCtx.Err() != nil {
		errs = append(errs, ErrShutdownTimeout, drainCtx.Err())
		for _, state := range runs {
			if operation, ok := pending[state.service.Name]; ok {
				errs = append(errs, fmt.Errorf("apprun: service %q %s: %w", state.service.Name, operation, drainCtx.Err()))
			}
		}
	}
	for _, state := range runs {
		select {
		case <-state.drained:
			continue
		default:
		}
		// 超时或 Wait 失败均需取消兜底；panic 转为错误，继续取消其余组件。
		if err := call(state.service.Name, "cancel", func() error {
			state.service.Cancel()
			return nil
		}); err != nil {
			errs = append(errs, err)
		}
	}
	for _, state := range runs {
		select {
		case <-state.done:
			if state.err != nil {
				errs = append(errs, &serviceError{
					name:      state.service.Name,
					operation: "run",
					err:       state.err,
				})
			}
		default:
		}
	}
	return errors.Join(errs...)
}

func call(name, operation string, fn func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			panicErr := &PanicError{
				Service:   name,
				Operation: operation,
				Value:     value,
				Stack:     debug.Stack(),
			}
			err = panicErr
		}
	}()
	return fn()
}
