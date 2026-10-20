package bootstrap_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func registryHeartbeat(ctx context.Context) error { return ctx.Err() }
func registryReport(context.Context) error        { return nil }
func RegistryHeartbeat(context.Context) error     { return nil }

type registryWorker struct{}

func (registryWorker) Run(ctx context.Context) error { return ctx.Err() }
func registryGeneric[T any](ctx context.Context) error {
	return ctx.Err()
}

func TestCronRegistryBatchAndSnapshots(t *testing.T) {
	registry := bootstrap.MustNewCronRegistry(registryHeartbeat, registryReport)
	handlers := registry.Handlers()
	var names []string
	for _, handler := range handlers {
		names = append(names, handler.Name)
	}
	if !slices.Equal(names, []string{"registryHeartbeat", "registryReport"}) {
		t.Fatalf("批量登记的任务名称或顺序错误: %v", names)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handlers[0].Handler(ctx); err != context.Canceled {
		t.Fatalf("未保留第一个 Handler: %v", err)
	}
	if err := handlers[1].Handler(ctx); err != nil {
		t.Fatalf("未保留第二个 Handler: %v", err)
	}

	// 调用方修改快照以及后续追加登记，都不能反向改动已交付的清单。
	handlers[0] = bootstrap.CronHandler{}
	snapshot := registry.Handlers()
	registry.MustRegister(registryReport, "report-alias")
	if len(snapshot) != 2 || snapshot[0].Name != "registryHeartbeat" || snapshot[0].Handler == nil {
		t.Fatalf("注册器与调用方共享了可变清单: %v", snapshot)
	}
	other := bootstrap.MustNewCronRegistry(registryHeartbeat)
	if len(other.Handlers()) != 1 || len(registry.Handlers()) != 3 {
		t.Fatal("不同注册器的登记互相污染")
	}
	if len(bootstrap.MustNewCronRegistry().Handlers()) != 0 {
		t.Fatal("空批量声明应得到空清单")
	}
}

func TestCronRegistryCaseSensitiveNames(t *testing.T) {
	registry := bootstrap.MustNewCronRegistry(registryHeartbeat, RegistryHeartbeat)
	handlers := registry.Handlers()
	if len(handlers) != 2 || handlers[0].Name != "registryHeartbeat" || handlers[1].Name != "RegistryHeartbeat" {
		t.Fatalf("仅大小写不同的函数名应保留为独立任务: %v", handlers)
	}
}

func TestCronRegistryExplicitNames(t *testing.T) {
	var registry bootstrap.CronRegistry
	called := false
	registry.MustRegister(func(ctx context.Context) error {
		called = true
		return ctx.Err()
	}, "stable-name")
	registry.MustRegister(registryWorker{}.Run, "worker")
	registry.MustRegister(registryGeneric[int], "generic")
	registry.MustRegister(registryHeartbeat, "OriginalName")
	if called {
		t.Fatal("登记阶段不应执行业务 Handler")
	}
	handlers := registry.Handlers()
	if handlers[0].Name != "stable-name" || handlers[3].Name != "OriginalName" {
		t.Fatalf("显式名称应按原值保留: %v", handlers)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, handler := range handlers {
		if err := handler.Handler(ctx); err != context.Canceled {
			t.Fatalf("显式命名的 Handler %q 不可调用: %v", handler.Name, err)
		}
	}
	if !called {
		t.Fatal("登记后的闭包未调用")
	}
}

func TestCronRegistryRejectsInvalidRegistration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		register func(*bootstrap.CronRegistry)
		want     string
	}{
		{
			name: "nil",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(nil)
			},
			want: "nil Cron handler",
		},
		{
			name: "empty-override",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryReport, "")
			},
			want: "invalid Cron handler",
		},
		{
			name: "whitespace",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryReport, " report")
			},
			want: "invalid Cron handler",
		},
		{
			name: "multiple-overrides",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryReport, "one", "two")
			},
			want: "at most one",
		},
		{
			name: "duplicate",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryHeartbeat)
			},
			want: "duplicate Cron handler",
		},
		{
			name: "explicit-collision",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryReport, "registryHeartbeat")
			},
			want: "duplicate Cron handler",
		},
		{
			name: "closure",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(func(context.Context) error { return nil })
			},
			want: "explicit name",
		},
		{
			name: "method",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryWorker{}.Run)
			},
			want: "explicit name",
		},
		{
			name: "generic",
			register: func(r *bootstrap.CronRegistry) {
				r.MustRegister(registryGeneric[int])
			},
			want: "explicit name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := bootstrap.MustNewCronRegistry(registryHeartbeat)
			requireCronRegistryPanic(t, tc.want, func() { tc.register(registry) })
			handlers := registry.Handlers()
			if len(handlers) != 1 || handlers[0].Name != "registryHeartbeat" {
				t.Fatal("失败登记不应改变已有清单")
			}
		})
	}
	requireCronRegistryPanic(t, "duplicate Cron handler", func() {
		bootstrap.MustNewCronRegistry(registryHeartbeat, registryHeartbeat)
	})
}

func TestCronRegistryConcurrentSnapshots(t *testing.T) {
	var registry bootstrap.CronRegistry
	var workers sync.WaitGroup
	for i := range 16 {
		workers.Go(func() {
			registry.MustRegister(registryReport, fmt.Sprintf("report-%d", i))
			for _, handler := range registry.Handlers() {
				if handler.Handler == nil || handler.Name == "" {
					t.Error("快照包含尚未完成的登记")
				}
			}
		})
	}
	workers.Wait()
	if len(registry.Handlers()) != 16 {
		t.Fatal("并发登记丢失任务")
	}
}

func requireCronRegistryPanic(t *testing.T, want string, run func()) {
	t.Helper()
	defer func() {
		value := recover()
		if value == nil || !strings.Contains(fmt.Sprint(value), want) {
			t.Errorf("panic = %v，期望包含 %q", value, want)
		}
	}()
	run()
}
