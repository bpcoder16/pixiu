package cronx_test

import (
	"context"
	"slices"
	"testing"

	"github.com/bpcoder16/pixiu/infra/cronx"
)

func registryHeartbeat(ctx context.Context) error { return ctx.Err() }
func registryReport(context.Context) error        { return nil }

func TestCronRegistryBatchAndSnapshots(t *testing.T) {
	registry := cronx.MustNewHandlerRegistry(registryHeartbeat, registryReport)
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
	handlers[0] = cronx.NamedHandler{}
	snapshot := registry.Handlers()
	registry.MustRegister(registryReport, "report-alias")
	if len(snapshot) != 2 || snapshot[0].Name != "registryHeartbeat" || snapshot[0].Handler == nil {
		t.Fatalf("注册器与调用方共享了可变清单: %v", snapshot)
	}
	other := cronx.MustNewHandlerRegistry(registryHeartbeat)
	if len(other.Handlers()) != 1 || len(registry.Handlers()) != 3 {
		t.Fatal("不同注册器的登记互相污染")
	}
	if cronx.MustNewHandlerRegistry().Handlers() != nil {
		t.Fatal("空批量声明应得到空清单")
	}
	var zero cronx.HandlerRegistry
	zero.MustRegister(registryReport)
	if len(zero.Handlers()) != 1 {
		t.Fatal("零值注册表不可用")
	}
}

func TestCronRegistryInvalidRegistration(t *testing.T) {
	for _, tc := range []struct {
		name     string
		register func(*cronx.HandlerRegistry)
		want     string
	}{
		{
			name:     "nil",
			register: func(r *cronx.HandlerRegistry) { r.MustRegister(nil) },
			want:     "cronx: nil Cron handler",
		},
		{
			name:     "multiple-names",
			register: func(r *cronx.HandlerRegistry) { r.MustRegister(registryReport, "one", "two") },
			want:     "cronx: expected at most one Cron handler name",
		},
		{
			name:     "duplicate",
			register: func(r *cronx.HandlerRegistry) { r.MustRegister(registryHeartbeat) },
			want:     `cronx: duplicate Cron handler "registryHeartbeat"`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := cronx.MustNewHandlerRegistry(registryHeartbeat)
			defer func() {
				value := recover()
				err, ok := value.(error)
				if !ok {
					t.Fatalf("非法登记应以 error 值 panic: %T (%v)", value, value)
				}
				if err.Error() != tc.want {
					t.Fatalf("panic = %v，期望 %q", err, tc.want)
				}
				if handlers := registry.Handlers(); len(handlers) != 1 || handlers[0].Name != "registryHeartbeat" {
					t.Fatal("失败登记改变了公开清单")
				}
			}()
			tc.register(registry)
		})
	}
}
