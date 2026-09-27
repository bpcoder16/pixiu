package taskpool_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bpcoder16/pixiu/taskpool"
)

func globalTestConfig() taskpool.Config {
	return taskpool.Config{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1}
}

func TestNewDefaultRegistersPool(t *testing.T) {
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	pool, err := taskpool.NewDefault(context.Background(), globalTestConfig())
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
	previous, err := taskpool.New(context.Background(), globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { previous.Shutdown() })
	old := taskpool.Swap(previous)
	t.Cleanup(func() { taskpool.Swap(old) })
	if pool, err := taskpool.NewDefault(nil, globalTestConfig()); pool != nil || !errors.Is(err, taskpool.ErrInvalidConfig) {
		t.Errorf("NewDefault(nil) = %p, %v, want nil and ErrInvalidConfig", pool, err)
	}
	stopCtx, stop := context.WithCancel(context.Background())
	stop()
	if pool, err := taskpool.NewDefault(stopCtx, globalTestConfig()); pool != nil || !errors.Is(err, context.Canceled) {
		t.Errorf("NewDefault(canceled) = %p, %v, want nil and context.Canceled", pool, err)
	}
	if got := taskpool.Default(); got != previous {
		t.Errorf("创建失败后 Default() = %p, want %p", got, previous)
	}
}

func TestInstancePoolCoexistsWithDefault(t *testing.T) {
	old := taskpool.Swap(nil)
	t.Cleanup(func() { taskpool.Swap(old) })
	otherPool, err := taskpool.New(context.Background(), globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	if got := taskpool.Default(); got != nil {
		t.Fatalf("New 创建独立池后 Default() = %p, want nil", got)
	}
	defaultPool, err := taskpool.NewDefault(context.Background(), globalTestConfig())
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
	first, err := taskpool.New(context.Background(), globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	second, err := taskpool.New(context.Background(), globalTestConfig())
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
	stopCtx, stop := context.WithCancel(context.Background())
	defer stop()
	pool, err := taskpool.New(stopCtx, globalTestConfig())
	if err != nil {
		t.Fatal(err)
	}
	old := taskpool.Swap(pool)
	t.Cleanup(func() { taskpool.Swap(old) })
	if err := taskpool.Submit(context.Background(), "wait", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := taskpool.Wait(); err != nil {
		t.Fatal(err)
	}
	if got := pool.Stats(); got.Succeeded != 1 {
		t.Errorf("Stats() = %+v, want one completed task", got)
	}
}
