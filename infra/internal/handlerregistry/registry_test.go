package handlerregistry

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type testHandler func(context.Context) error

func heartbeat(ctx context.Context) error { return ctx.Err() }
func Heartbeat(context.Context) error     { return nil }
func genericHandler[T any](ctx context.Context) error {
	return ctx.Err()
}

type worker struct{}

func (worker) Run(ctx context.Context) error { return ctx.Err() }

func TestRegistryNamesOrderAndSnapshots(t *testing.T) {
	var registry Registry[testHandler]
	if registry.Entries() != nil || registry.Snapshot() != nil {
		t.Fatal("零值注册表应返回 nil 快照")
	}
	for _, handler := range []testHandler{heartbeat, Heartbeat} {
		if err := registry.Register(handler, "Test"); err != nil {
			t.Fatal(err)
		}
	}
	entries := registry.Entries()
	names := []string{entries[0].Name, entries[1].Name}
	if !slices.Equal(names, []string{"heartbeat", "Heartbeat"}) {
		t.Fatalf("名称大小写或登记顺序丢失: %v", names)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := entries[0].Handler(ctx); err != context.Canceled {
		t.Fatalf("未保留具名函数: %v", err)
	}

	snapshot := registry.Snapshot()
	entries[0] = Entry[testHandler]{}
	delete(snapshot, "Heartbeat")
	snapshot["injected"] = Heartbeat
	if err := registry.Register(heartbeat, "Test", "alias"); err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 || snapshot["alias"] != nil {
		t.Fatal("后续登记不应改变已交付快照")
	}
	current := registry.Entries()
	if len(current) != 3 || current[0].Name != "heartbeat" || current[2].Name != "alias" {
		t.Fatalf("快照修改影响了登记顺序: %v", current)
	}
	if registry.Snapshot()["Heartbeat"] == nil || registry.Snapshot()["injected"] != nil {
		t.Fatal("map 快照与注册表共享状态")
	}
	var other Registry[testHandler]
	if other.Entries() != nil {
		t.Fatal("注册表实例之间共享状态")
	}
}

func TestRegistryExplicitNamesAndDifferentSignatures(t *testing.T) {
	var registry Registry[testHandler]
	called := false
	for _, entry := range []Entry[testHandler]{
		{
			Name: "closure",
			Handler: func(ctx context.Context) error {
				called = true
				return ctx.Err()
			},
		},
		{
			Name:    "method",
			Handler: worker{}.Run,
		},
		{
			Name:    "generic",
			Handler: genericHandler[int],
		},
	} {
		if err := registry.Register(entry.Handler, "Test", entry.Name); err != nil {
			t.Fatal(err)
		}
	}
	if called {
		t.Fatal("登记不得执行业务函数")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, entry := range registry.Entries() {
		if err := entry.Handler(ctx); err != context.Canceled {
			t.Fatalf("显式命名的函数 %q 不可调用: %v", entry.Name, err)
		}
	}
	if !called {
		t.Fatal("丢失闭包绑定")
	}

	// 内部实现不枚举 cron 或消息处理签名，也不依赖对应 SDK。
	var formatters Registry[func(int) string]
	if err := formatters.Register(func(n int) string {
		return fmt.Sprintf("value=%d", n)
	}, "Formatter", "format"); err != nil {
		t.Fatal(err)
	}
	if got := formatters.Snapshot()["format"](7); got != "value=7" {
		t.Fatalf("不同函数签名未正确保留: %q", got)
	}
}

func TestRegistryRejectsInvalidRegistrationWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler any
		names   []string
		want    string
	}{
		{
			name: "nil",
			want: "nil Test handler",
		},
		{
			name:    "typed-nil",
			handler: testHandler(nil),
			want:    "nil Test handler",
		},
		{
			name:    "non-function",
			handler: 7,
			names:   []string{"number"},
			want:    "must be a function",
		},
		{
			name:    "nil-non-function",
			handler: (*int)(nil),
			names:   []string{"pointer"},
			want:    "must be a function",
		},
		{
			name:    "empty-name",
			handler: heartbeat,
			names:   []string{""},
			want:    `invalid Test handler ""`,
		},
		{
			name:    "space-prefix",
			handler: heartbeat,
			names:   []string{" name"},
			want:    "invalid Test handler",
		},
		{
			name:    "space-suffix",
			handler: heartbeat,
			names:   []string{"name "},
			want:    "invalid Test handler",
		},
		{
			name:    "multiple-names",
			handler: heartbeat,
			names:   []string{"one", "two"},
			want:    "expected at most one Test handler name",
		},
		{
			name:    "duplicate-inferred",
			handler: heartbeat,
			want:    `duplicate Test handler "heartbeat"`,
		},
		{
			name:    "duplicate-explicit",
			handler: Heartbeat,
			names:   []string{"heartbeat"},
			want:    `duplicate Test handler "heartbeat"`,
		},
		{
			name:    "closure",
			handler: func() {},
			want:    "explicit name",
		},
		{
			name:    "method",
			handler: worker{}.Run,
			want:    "explicit name",
		},
		{
			name:    "generic",
			handler: genericHandler[int],
			want:    "explicit name",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var registry Registry[any]
			if err := registry.Register(testHandler(heartbeat), "Test"); err != nil {
				t.Fatal(err)
			}
			err := registry.Register(tc.handler, "Test", tc.names...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v，期望包含 %q", err, tc.want)
			}
			entries := registry.Entries()
			if len(entries) != 1 || entries[0].Name != "heartbeat" || len(registry.Snapshot()) != 1 {
				t.Fatal("失败登记改变了已有清单")
			}
			if err := registry.Register(Heartbeat, "Test"); err != nil {
				t.Fatalf("失败后应仍可登记: %v", err)
			}
		})
	}
}

func TestRegistryConcurrentRegistrationAndSnapshots(t *testing.T) {
	var registry Registry[testHandler]
	var workers sync.WaitGroup
	var winners atomic.Int32
	for i := range 16 {
		workers.Go(func() {
			if err := registry.Register(heartbeat, "Test", "shared"); err == nil {
				winners.Add(1)
			}
			if err := registry.Register(Heartbeat, "Test", fmt.Sprintf("handler-%d", i)); err != nil {
				t.Error(err)
			}
			for _, entry := range registry.Entries() {
				if entry.Name == "" || entry.Handler == nil {
					t.Error("有序快照包含未完成的登记")
				}
			}
			for name, handler := range registry.Snapshot() {
				if name == "" || handler == nil {
					t.Error("map 快照包含未完成的登记")
				}
			}
		})
	}
	workers.Wait()
	entries := registry.Entries()
	if winners.Load() != 1 || len(entries) != 17 || len(registry.Snapshot()) != 17 {
		t.Fatalf("并发查重或登记失败: winners=%d entries=%d", winners.Load(), len(entries))
	}
	seen := make(map[string]bool)
	for _, entry := range entries {
		if seen[entry.Name] {
			t.Fatalf("登记顺序中存在重复项: %q", entry.Name)
		}
		seen[entry.Name] = true
	}
}
