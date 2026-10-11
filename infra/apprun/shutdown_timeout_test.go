package apprun

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

func TestShutdownTimeoutCoversCallbackAndRun(t *testing.T) {
	for _, operation := range []string{"shutdown", "run"} {
		t.Run(operation, func(t *testing.T) {
			const timeout = 50 * time.Millisecond
			manager, err := New(timeout)
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			dependencyStarted, producerStarted := make(chan struct{}), make(chan struct{})
			dependencyDone, producerDone := make(chan struct{}), make(chan struct{})
			shutdownStarted, shutdownDone := make(chan struct{}), make(chan struct{})
			var dependencyClosed atomic.Bool
			shutdownFailure := errors.New("shutdown failed while Run is still active")
			defer func() {
				close(release)
				await(t, dependencyDone)
				await(t, producerDone)
				await(t, shutdownDone)
				if dependencyClosed.Load() {
					t.Error("超时返回后仍继续关闭依赖")
				}
			}()
			register(t, manager, Service{
				Name: "dependency",
				Run: func() error {
					defer close(dependencyDone)
					close(dependencyStarted)
					<-release
					return nil
				},
				Shutdown: func() error {
					dependencyClosed.Store(true)
					return nil
				},
			})
			register(t, manager, Service{
				Name: "producer",
				Run: func() error {
					defer close(producerDone)
					close(producerStarted)
					<-release
					return nil
				},
				Shutdown: func() error {
					defer close(shutdownDone)
					close(shutdownStarted)
					if operation == "shutdown" {
						<-release
						return nil
					}
					return shutdownFailure
				},
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := run(manager, ctx)
			await(t, dependencyStarted)
			await(t, producerStarted)
			started := time.Now()
			cancel()
			await(t, shutdownStarted)
			err = await(t, done)
			if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("未返回总关闭超时: %v", err)
			}
			if operation == "run" && !errors.Is(err, shutdownFailure) {
				t.Fatalf("等待 Run 超时时丢失已完成的关闭错误: %v", err)
			}
			if elapsed := time.Since(started); elapsed < timeout || elapsed > time.Second {
				t.Fatalf("总期限未生效: %v", elapsed)
			}
		})
	}
}

func TestShutdownTimeoutIsSharedByAllServices(t *testing.T) {
	manager, err := New(120 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	started, finished := make(chan struct{}, 3), make(chan struct{}, 3)
	shutdowns := make(chan string, 3)
	release := make(chan struct{})
	defer func() {
		close(release)
		for range 3 {
			await(t, finished)
		}
	}()
	for _, name := range []string{"first", "second", "third"} {
		stop := make(chan struct{})
		register(t, manager, Service{
			Name: name,
			Run: func() error {
				defer func() { finished <- struct{}{} }()
				started <- struct{}{}
				select {
				case <-stop:
				case <-release:
				}
				return nil
			},
			Shutdown: func() error {
				shutdowns <- name
				select {
				case <-time.After(80 * time.Millisecond):
				case <-release:
				}
				close(stop)
				return nil
			},
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(manager, ctx)
	for range 3 {
		await(t, started)
	}
	cancel()
	if err := await(t, done); !errors.Is(err, ErrShutdownTimeout) {
		t.Fatalf("各服务没有共享总期限: %v", err)
	}
	var order []string
	for len(shutdowns) > 0 {
		order = append(order, <-shutdowns)
	}
	if !slices.Equal(order, []string{"third", "second"}) {
		t.Fatalf("逆序关闭或超时停止不正确: %v", order)
	}
}

func TestShutdownTimeoutStartsWhenStopping(t *testing.T) {
	const timeout = 50 * time.Millisecond
	manager, err := New(timeout)
	if err != nil {
		t.Fatal(err)
	}
	started, stop := make(chan struct{}), make(chan struct{})
	register(t, manager, Service{
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
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := run(manager, ctx)
	await(t, started)
	select {
	case err := <-done:
		t.Fatalf("运行期间启动了关闭倒计时: %v", err)
	case <-time.After(2 * timeout):
	}
	cancel()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownTimeoutPreservesCompletedRunError(t *testing.T) {
	manager, err := New(50 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	runFailure := errors.New("service failed before shutdown")
	release, runDone, shutdownDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		close(release)
		await(t, runDone)
		await(t, shutdownDone)
	}()
	register(t, manager, Service{
		Name:     "failed",
		Run:      func() error { return runFailure },
		Shutdown: func() error { return nil },
	})
	register(t, manager, Service{
		Name: "blocked",
		Run: func() error {
			defer close(runDone)
			<-release
			return nil
		},
		Shutdown: func() error {
			defer close(shutdownDone)
			<-release
			return nil
		},
	})
	err = await(t, run(manager, context.Background()))
	if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, runFailure) {
		t.Fatalf("总关闭超时丢失运行错误: %v", err)
	}
}

func TestShutdownTimeoutCollectsReadyShutdownError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ready bool
		err   error
	}{
		{name: "关闭错误已到达", ready: true, err: errors.New("shutdown failed")},
		{name: "关闭 panic 已到达", ready: true, err: &PanicError{
			Service:   "service",
			Operation: "shutdown",
			Value:     "shutdown failed",
		}},
		{name: "关闭成功已到达", ready: true},
		{name: "关闭结果尚未到达"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shutdownDone := make(chan error, 1)
			defer close(shutdownDone)
			if tc.ready {
				shutdownDone <- tc.err
			}
			pending := []*serviceRun{{
				service: Service{Name: "service"},
				done:    make(chan struct{}),
			}}
			// 直接验证选中超时分支后的汇总，稳定覆盖结果同时就绪的边界。
			done := make(chan error, 1)
			go func() {
				done <- shutdownTimedOut(context.Background(), pending, "shutdown", nil, nil, shutdownDone)
			}()
			err := await(t, done)
			if !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("丢失总关闭超时: %v", err)
			}
			if tc.err != nil && !errors.Is(err, tc.err) {
				t.Fatalf("超时返回丢失已到达的关闭错误: %v", err)
			}
		})
	}
}

func TestNewRejectsNegativeShutdownTimeout(t *testing.T) {
	if manager, err := New(-time.Second); err == nil || manager != nil {
		t.Fatalf("构造时未拒绝负数总期限: manager=%v, err=%v", manager, err)
	}
}

func TestNewWithDefaultShutdownTimeout(t *testing.T) {
	manager, err := New(0)
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	register(t, manager, Service{
		Name: "service",
		Run: func() error {
			t.Error("预取消仍启动服务")
			return nil
		},
		Shutdown: func() error {
			closed = true
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := manager.Run(ctx); err != nil {
		t.Fatalf("默认期限未正常关闭服务: %v", err)
	}
	if !closed {
		t.Fatal("默认期限未执行关闭回调")
	}
}
