package taskpool_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/taskpool"
)

func globalTestConfig() taskpool.Config {
	return taskpool.Config{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1}
}

func TestNewDefaultRegistersPool(t *testing.T) {
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	pool, err := taskpool.NewDefault(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := taskpool.Default(); got != pool {
		t.Fatalf("Default() = %p, want %p", got, pool)
	}
	if err := taskpool.Submit(context.Background(), "new-default", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := pool.Stats(); got.Succeeded != 1 {
		t.Errorf("Stats() = %+v, want one completed task", got)
	}
}

func TestNewDefaultFailureKeepsPreviousPool(t *testing.T) {
	previous, err := taskpool.New(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { previous.Shutdown() })
	old := taskpool.Swap(previous)
	t.Cleanup(func() { taskpool.Swap(old) })
	if pool, err := taskpool.NewDefault(taskpool.Config{}); pool != nil || !errors.Is(err, taskpool.ErrInvalidConfig) {
		t.Errorf("NewDefault(Config{}) = %p, %v, want nil and ErrInvalidConfig", pool, err)
	}
	if got := taskpool.Default(); got != previous {
		t.Errorf("创建失败后 Default() = %p, want %p", got, previous)
	}
}

func TestInstancePoolCoexistsWithDefault(t *testing.T) {
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	otherPool, err := taskpool.New(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := taskpool.Default(); got != nil {
		t.Fatalf("New 创建独立池后 Default() = %p, want nil", got)
	}
	defaultPool, err := taskpool.NewDefault(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := taskpool.Default(); got != defaultPool {
		t.Fatalf("NewDefault 后 Default() = %p, want %p", got, defaultPool)
	}
	if err := otherPool.Submit(context.Background(), "other", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Submit(context.Background(), "default", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := otherPool.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Submit(context.Background(), "default-after-other-close", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("另一个池关闭后全局 Submit() = %v", err)
	}
	if err := taskpool.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := otherPool.Stats(); got.Accepted != 1 || got.Succeeded != 1 {
		t.Errorf("other Stats() = %+v, want one completed task", got)
	}
	if got := defaultPool.Stats(); got.Accepted != 2 || got.Succeeded != 2 {
		t.Errorf("default Stats() = %+v, want two completed tasks", got)
	}
}

func TestGlobalWithoutDefault(t *testing.T) {
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	if got := taskpool.Default(); got != nil {
		t.Fatalf("Default() = %p, want nil", got)
	}
	for _, call := range []struct {
		name string
		fn   func() error
	}{
		{name: "Submit", fn: func() error {
			return taskpool.Submit(context.Background(), "job", func(context.Context) error { return nil })
		}},
		{name: "Wait", fn: taskpool.Wait},
		{name: "Shutdown", fn: taskpool.Shutdown},
	} {
		if err := call.fn(); !errors.Is(err, taskpool.ErrNoDefault) {
			t.Errorf("%s() = %v, want ErrNoDefault", call.name, err)
		}
	}
}

func TestGlobalSubmitAndShutdownFollowDefault(t *testing.T) {
	first, err := taskpool.New(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := taskpool.New(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	taskpool.SetDefault(first)
	if got := taskpool.Default(); got != first {
		t.Fatalf("Default() = %p, want %p", got, first)
	}
	if err := taskpool.Submit(context.Background(), "first", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := taskpool.Swap(second); got != first {
		t.Fatalf("Swap() = %p, want %p", got, first)
	}
	if err := taskpool.Submit(context.Background(), "second", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := first.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := first.Stats(); got.Accepted != 1 || got.Succeeded != 1 {
		t.Errorf("first Stats() = %+v, want one completed task", got)
	}
	if got := second.Stats(); got.Accepted != 1 || got.Succeeded != 1 {
		t.Errorf("second Stats() = %+v, want one completed task", got)
	}
	if err := taskpool.Submit(context.Background(), "closed", func(context.Context) error { return nil }); !errors.Is(err, taskpool.ErrClosed) {
		t.Errorf("关闭后 Submit() = %v, want ErrClosed", err)
	}
	taskpool.SetDefault(nil)
	if got := taskpool.Default(); got != nil {
		t.Fatalf("SetDefault(nil) 后 Default() = %p, want nil", got)
	}
	if err := taskpool.Submit(context.Background(), "cleared", func(context.Context) error { return nil }); !errors.Is(err, taskpool.ErrNoDefault) {
		t.Errorf("清除注册后 Submit() = %v, want ErrNoDefault", err)
	}
}

func TestGlobalWait(t *testing.T) {
	pool, err := taskpool.New(globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := taskpool.Swap(pool)
	t.Cleanup(func() { taskpool.Swap(old) })
	if err := taskpool.Submit(context.Background(), "wait", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := taskpool.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := pool.Stats(); got.Succeeded != 1 {
		t.Errorf("Stats() = %+v, want one completed task", got)
	}
}

func TestGlobalShutdownTimeoutAndWaitForCleanup(t *testing.T) {
	cfg := globalTestConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := taskpool.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	old := taskpool.Swap(p)
	t.Cleanup(func() { taskpool.Swap(old) })
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		_ = p.Shutdown()
		_ = p.Wait()
	})
	if err := taskpool.Submit(context.Background(), "cleanup", func(ctx context.Context) error {
		close(started)
		<-release
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	shutdownErr := taskpool.Shutdown()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded", shutdownErr)
	}
	if err := p.Shutdown(); err != shutdownErr {
		t.Fatalf("实例 Shutdown = %v, want saved error %v", err, shutdownErr)
	}
	waited := make(chan error, 1)
	go func() { waited <- taskpool.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("清理完成前 Wait 返回: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case err := <-waited:
		if err != shutdownErr {
			t.Fatalf("全局 Wait = %v, want saved error %v", err, shutdownErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("清理结束后全局 Wait 未返回")
	}
	if err := taskpool.Wait(); err != shutdownErr {
		t.Fatalf("重复全局 Wait = %v, want saved error %v", err, shutdownErr)
	}
	if got := p.Stats(); got.Workers != 0 || got.Running != 0 {
		t.Fatalf("Wait 后 Stats = %+v", got)
	}
}
