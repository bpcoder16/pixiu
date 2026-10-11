package apprun

import (
	"context"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistrationAndSingleRun(t *testing.T) {
	var manager Manager
	if err := manager.Run(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("nil context: %v", err)
	}
	if err := manager.Run(context.Background()); !errors.Is(err, ErrNoServices) {
		t.Fatalf("空管理器: %v", err)
	}
	for _, service := range []Service{
		{},
		{Name: "service", Run: func() error { return nil }},
		{Name: "service", Shutdown: func() error { return nil }},
		{Name: " service ", Run: func() error { return nil }, Shutdown: func() error { return nil }},
	} {
		if err := manager.Register(service); !errors.Is(err, ErrInvalidService) {
			t.Fatalf("无效服务被接受: %v", err)
		}
	}
	started, stop := make(chan struct{}), make(chan struct{})
	service := Service{
		Name: "service",
		Run: func() error {
			close(started)
			<-stop
			return nil
		},
		Shutdown: func() error {
			close(stop)
			return nil
		},
	}
	register(t, &manager, service)
	if err := manager.Register(service); !errors.Is(err, ErrDuplicateService) {
		t.Fatalf("重复服务: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&manager, ctx)
	await(t, started)
	if err := manager.Register(service); !errors.Is(err, ErrStarted) {
		t.Fatalf("运行期登记: %v", err)
	}
	var callers sync.WaitGroup
	for range 8 {
		callers.Go(func() {
			if err := manager.Run(ctx); !errors.Is(err, ErrStarted) {
				t.Errorf("并发运行: %v", err)
			}
		})
	}
	callers.Wait()
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	if err := manager.Run(context.Background()); !errors.Is(err, ErrStarted) {
		t.Fatalf("关闭后重启: %v", err)
	}
}

func TestShutdownWaitsForProducerBeforeDependency(t *testing.T) {
	var manager Manager
	dependencyRun, producerRun := make(chan struct{}), make(chan struct{})
	stopDependency, stopProducer := make(chan struct{}), make(chan struct{})
	producerStopping, dependencyStopping := make(chan struct{}), make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(stopProducer) }) }
	defer unblock()
	register(t, &manager, Service{
		Name: "dependency",
		Run: func() error {
			close(dependencyRun)
			<-stopDependency
			return nil
		},
		Shutdown: func() error {
			close(dependencyStopping)
			close(stopDependency)
			return nil
		},
	})
	register(t, &manager, Service{
		Name: "producer",
		Run: func() error {
			close(producerRun)
			<-stopProducer
			return nil
		},
		Shutdown: func() error {
			// 关闭请求已经发出，但 Run 还要完成在途工作。
			close(producerStopping)
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&manager, ctx)
	await(t, dependencyRun)
	await(t, producerRun)
	cancel()
	await(t, producerStopping)
	select {
	case <-dependencyStopping:
		t.Fatal("生产者退出前关闭了依赖")
	case err := <-done:
		t.Fatalf("生产者退出前管理器返回: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	await(t, dependencyStopping)
}

func TestRunFailureClosesAllAndPreservesErrors(t *testing.T) {
	runFailure, closeFailure := errors.New("listen failed"), errors.New("flush failed")
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{name: "启动失败", err: runFailure, want: runFailure},
		{name: "意外正常返回", want: ErrUnexpectedExit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var manager Manager
			var closed []string
			stop := make(chan struct{})
			register(t, &manager, Service{
				Name: "dependency",
				Run: func() error {
					<-stop
					return nil
				},
				Shutdown: func() error {
					closed = append(closed, "dependency")
					close(stop)
					return closeFailure
				},
			})
			register(t, &manager, Service{
				Name: "http",
				Run:  func() error { return tc.err },
				Shutdown: func() error {
					closed = append(closed, "http")
					return nil
				},
			})
			err := await(t, run(&manager, context.Background()))
			if !errors.Is(err, tc.want) || !errors.Is(err, closeFailure) {
				t.Fatalf("丢失错误链: %v", err)
			}
			if !strings.Contains(err.Error(), "http") || !strings.Contains(err.Error(), "dependency") {
				t.Fatalf("缺少服务名: %v", err)
			}
			if !slices.Equal(closed, []string{"http", "dependency"}) {
				t.Fatalf("回收顺序: %v", closed)
			}
		})
	}
}

func TestCanceledBeforeRunClosesWithoutStarting(t *testing.T) {
	var manager Manager
	var closed []string
	for _, name := range []string{"dependency", "producer"} {
		register(t, &manager, Service{
			Name: name,
			Run: func() error {
				t.Error("预取消仍启动 Run")
				return nil
			},
			Shutdown: func() error {
				closed = append(closed, name)
				return nil
			},
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(closed, []string{"producer", "dependency"}) {
		t.Fatalf("预取消没有回收资源: %v", closed)
	}
}

func TestRunErrorDuringShutdownIsNotSuppressed(t *testing.T) {
	var manager Manager
	started, stop := make(chan struct{}), make(chan struct{})
	failure := errors.New("run failed while shutting down")
	register(t, &manager, Service{
		Name: "service",
		Run: func() error {
			close(started)
			<-stop
			return failure
		},
		Shutdown: func() error {
			close(stop)
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&manager, ctx)
	await(t, started)
	cancel()
	if err := await(t, done); !errors.Is(err, failure) {
		t.Fatalf("关闭期间吞掉了运行错误: %v", err)
	}
}

func TestPanicStillClosesRemainingServices(t *testing.T) {
	for _, operation := range []string{"run", "shutdown"} {
		t.Run(operation, func(t *testing.T) {
			var manager Manager
			stop := make(chan struct{})
			closed := false
			register(t, &manager, Service{
				Name: "dependency",
				Run: func() error {
					<-stop
					return nil
				},
				Shutdown: func() error {
					closed = true
					close(stop)
					return nil
				},
			})
			register(t, &manager, Service{
				Name: "broken",
				Run: func() error {
					if operation == "run" {
						panic("broken callback")
					}
					return errors.New("trigger shutdown")
				},
				Shutdown: func() error {
					if operation == "shutdown" {
						panic("broken callback")
					}
					return nil
				},
			})
			err := await(t, run(&manager, context.Background()))
			var panicErr *PanicError
			if !errors.As(err, &panicErr) || panicErr.Service != "broken" || panicErr.Operation != operation || len(panicErr.Stack) == 0 {
				t.Fatalf("panic 信息丢失: %v", err)
			}
			if !closed {
				t.Fatal("panic 后没有回收依赖")
			}
		})
	}
}

func TestShutdownPanicWaitsForRunBeforeDependency(t *testing.T) {
	var manager Manager
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{}, 2)
	stopDependency, stopProducer := make(chan struct{}), make(chan struct{})
	producerStopping := make(chan struct{})
	var dependencyClosed atomic.Bool
	register(t, &manager, Service{
		Name: "dependency",
		Run: func() error {
			started <- struct{}{}
			select {
			case <-stopDependency:
			case <-release:
			}
			return nil
		},
		Shutdown: func() error {
			dependencyClosed.Store(true)
			close(stopDependency)
			return nil
		},
	})
	register(t, &manager, Service{
		Name: "producer",
		Run: func() error {
			started <- struct{}{}
			select {
			case <-stopProducer:
			case <-release:
			}
			return nil
		},
		Shutdown: func() error {
			close(producerStopping)
			panic("failed before notifying Run")
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&manager, ctx)
	await(t, started)
	await(t, started)
	cancel()
	await(t, producerStopping)
	select {
	case err := <-done:
		t.Fatalf("Shutdown panic 后未等待 Run: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if dependencyClosed.Load() {
		t.Fatal("生产者仍在运行时关闭了依赖")
	}
	close(stopProducer)
	err := await(t, done)
	var panicErr *PanicError
	if errors.Is(err, ErrShutdownTimeout) || !errors.As(err, &panicErr) || panicErr.Service != "producer" || panicErr.Operation != "shutdown" || len(panicErr.Stack) == 0 {
		t.Fatalf("Run 退出后未保留原始 panic 或误报超时: %v", err)
	}
	if !dependencyClosed.Load() {
		t.Fatal("生产者退出后未继续关闭依赖")
	}
}

func TestShutdownPanicTimeoutPreservesCompletedRunError(t *testing.T) {
	const timeout = 50 * time.Millisecond
	manager, err := New(timeout)
	if err != nil {
		t.Fatal(err)
	}
	runFailure := errors.New("service failed before shutdown")
	release, runDone := make(chan struct{}), make(chan struct{})
	defer func() {
		close(release)
		await(t, runDone)
	}()
	var dependencyClosed atomic.Bool
	register(t, manager, Service{
		Name: "failed",
		Run:  func() error { return runFailure },
		Shutdown: func() error {
			dependencyClosed.Store(true)
			return nil
		},
	})
	register(t, manager, Service{
		Name: "producer",
		Run: func() error {
			defer close(runDone)
			<-release
			return nil
		},
		Shutdown: func() error { panic("failed before notifying Run") },
	})
	started := time.Now()
	err = await(t, run(manager, context.Background()))
	var panicErr *PanicError
	if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) || !errors.As(err, &panicErr) || panicErr.Operation != "shutdown" || len(panicErr.Stack) == 0 {
		t.Fatalf("未报告总关闭超时及原始 panic: %v", err)
	}
	if elapsed := time.Since(started); elapsed < timeout {
		t.Fatalf("Shutdown panic 后未等待总关闭期限: %v", elapsed)
	}
	if !errors.Is(err, runFailure) {
		t.Fatalf("Shutdown panic 丢失触发停机的运行错误: %v", err)
	}
	if dependencyClosed.Load() {
		t.Fatal("生产者仍在运行时关闭了依赖")
	}
}

func register(t *testing.T, manager *Manager, service Service) {
	t.Helper()
	if err := manager.Register(service); err != nil {
		t.Fatal(err)
	}
}

func run(manager *Manager, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	return done
}

func await[T any](t *testing.T, event <-chan T) T {
	t.Helper()
	select {
	case value := <-event:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("等待测试事件超时")
		var zero T
		return zero
	}
}
