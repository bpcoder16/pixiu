package apprun

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistrationAndSingleRun(t *testing.T) {
	var m Manager
	if !errors.Is(m.Run(nil), ErrNilContext) || !errors.Is(m.Run(context.Background()), ErrNoServices) {
		t.Fatal("无效运行参数")
	}
	valid := Service{
		Name:   "service",
		Run:    func() error { return nil },
		Stop:   func() error { return nil },
		Wait:   func(context.Context) error { return nil },
		Cancel: func() {},
	}
	for _, field := range []string{"name", "run", "stop", "wait", "cancel"} {
		s := valid
		switch field {
		case "name":
			s.Name = " service "
		case "run":
			s.Run = nil
		case "stop":
			s.Stop = nil
		case "wait":
			s.Wait = nil
		case "cancel":
			s.Cancel = nil
		}
		if !errors.Is(m.Register(s), ErrInvalidService) {
			t.Fatalf("接受无效 %s", field)
		}
	}
	register(t, &m, valid)
	if !errors.Is(m.Register(valid), ErrDuplicateService) {
		t.Fatal("接受重复名称")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(m.Register(valid), ErrStarted) || !errors.Is(m.Run(ctx), ErrStarted) {
		t.Fatal("关闭后允许重新运行")
	}
}

func TestStopAllBeforeWaitingForWork(t *testing.T) {
	var m Manager
	started := make(chan struct{}, 3)
	stopped := make(chan struct{}, 3)
	deadlines := make(chan time.Time, 3)
	release := make(chan struct{})
	for _, name := range []string{"http", "cron", "nats"} {
		register(t, &m, Service{
			Name: name,
			Run: func() error {
				started <- struct{}{}
				<-release
				return nil
			},
			Stop: func() error {
				stopped <- struct{}{}
				return nil
			},
			Wait: func(ctx context.Context) error {
				deadline, ok := ctx.Deadline()
				if !ok {
					t.Error("排空缺少共享期限")
				}
				deadlines <- deadline
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
			Cancel: func() { t.Error("正常排空发生取消") },
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&m, ctx)
	for range 3 {
		await(t, started)
	}
	cancel()
	for range 3 {
		await(t, stopped)
	}
	deadline := await(t, deadlines)
	for range 2 {
		if got := await(t, deadlines); !got.Equal(deadline) {
			t.Fatalf("各服务未共享同一截止时间: %v / %v", deadline, got)
		}
	}
	select {
	case err := <-done:
		t.Fatalf("业务未结束即返回: %v", err)
	default:
	}
	close(release)
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestPreCanceledRunStopsWithoutStarting(t *testing.T) {
	var m Manager
	var stopped atomic.Int32
	for _, name := range []string{"one", "two"} {
		register(t, &m, Service{
			Name: name,
			Run: func() error {
				t.Error("预取消仍运行")
				return nil
			},
			Stop: func() error {
				stopped.Add(1)
				return nil
			},
			Wait:   func(context.Context) error { return nil },
			Cancel: func() {},
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.Run(ctx); err != nil || stopped.Load() != 2 {
		t.Fatalf("预取消未回收: %v", err)
	}
}

func TestErrorsAndPanicsSurviveShutdown(t *testing.T) {
	for _, phase := range []string{"run", "stop", "wait"} {
		t.Run(phase, func(t *testing.T) {
			var m Manager
			failure := errors.New("运行失败")
			stopFailure := errors.New("停收失败")
			register(t, &m, Service{
				Name: "broken",
				Run: func() error {
					if phase == "run" {
						panic("broken")
					}
					return failure
				},
				Stop: func() error {
					if phase == "stop" {
						panic("broken")
					}
					return stopFailure
				},
				Wait: func(context.Context) error {
					if phase == "wait" {
						panic("broken")
					}
					return nil
				},
				Cancel: func() {},
			})
			err := m.Run(context.Background())
			var p *PanicError
			if !errors.As(err, &p) || p.Operation != phase || p.Service != "broken" || len(p.Stack) == 0 {
				t.Fatalf("丢失 panic: %v", err)
			}
			if phase != "run" && !errors.Is(err, failure) {
				t.Fatalf("丢失运行错误: %v", err)
			}
			if phase != "stop" && !errors.Is(err, stopFailure) {
				t.Fatalf("丢失停收错误: %v", err)
			}
		})
	}
}

func TestUnexpectedRunExit(t *testing.T) {
	var m Manager
	register(t, &m, Service{
		Name:   "early",
		Run:    func() error { return nil },
		Stop:   func() error { return nil },
		Wait:   func(context.Context) error { return nil },
		Cancel: func() {},
	})
	if err := m.Run(context.Background()); !errors.Is(err, ErrUnexpectedExit) {
		t.Fatalf("未报告提前退出: %v", err)
	}
}

func TestWaitFailureCancelsBeforeReturn(t *testing.T) {
	for _, phase := range []string{"error", "panic"} {
		t.Run(phase, func(t *testing.T) {
			var m Manager
			failure := errors.New("未确认排空")
			started, stopped := make(chan struct{}), make(chan struct{})
			workCtx, cancelWork := context.WithCancel(context.Background())
			defer cancelWork()
			var canceled atomic.Int32
			register(t, &m, Service{
				Name: "broken",
				Run: func() error {
					close(started)
					<-stopped
					return nil
				},
				Stop: func() error {
					close(stopped)
					return nil
				},
				Wait: func(context.Context) error {
					if phase == "panic" {
						panic("未确认排空")
					}
					return failure
				},
				Cancel: func() {
					canceled.Add(1)
					cancelWork()
				},
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := run(&m, ctx)
			await(t, started)
			cancel()
			err := await(t, done)
			if phase == "panic" {
				var p *PanicError
				if !errors.As(err, &p) || p.Operation != "wait" || len(p.Stack) == 0 {
					t.Fatalf("丢失等待 panic: %v", err)
				}
			} else if !errors.Is(err, failure) {
				t.Fatalf("丢失等待错误: %v", err)
			}
			if errors.Is(err, ErrShutdownTimeout) {
				t.Fatalf("等待失败被误报为超时: %v", err)
			}
			if canceled.Load() != 1 || workCtx.Err() != context.Canceled {
				t.Fatalf("返回前未取消未确认排空的业务: cancel=%d, work=%v", canceled.Load(), workCtx.Err())
			}
		})
	}
}

func TestConcurrentRunRejected(t *testing.T) {
	var m Manager
	started, stop := make(chan struct{}), make(chan struct{})
	register(t, &m, Service{
		Name: "s",
		Run: func() error {
			close(started)
			<-stop
			return nil
		},
		Stop: func() error {
			close(stop)
			return nil
		},
		Wait:   func(context.Context) error { return nil },
		Cancel: func() {},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(&m, ctx)
	await(t, started)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if !errors.Is(m.Run(ctx), ErrStarted) {
				t.Error("接受重复运行")
			}
		})
	}
	wg.Wait()
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
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
