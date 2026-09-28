package sqlitex

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func assertNamedPanic(t *testing.T, want string, lookup func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("命名查询的 panic=%v, want %q", got, want)
		}
	}()
	lookup()
}

func TestNamedRegistryAndLifecycleClose(t *testing.T) {
	ctx := context.Background()
	first, err := New(ctx, Config{Name: "first", DSN: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := New(ctx, Config{Name: "second", DSN: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	registry := named.New[*Client]("sqlitex")
	for name, client := range map[string]*Client{"first": first, "second": second} {
		if _, err := registry.Create(name, func() (*Client, error) {
			return client, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := registry.MustGet("first"); got != first {
		t.Fatalf("按名称获取客户端: got=%p, want=%p", got, first)
	}
	if _, err := registry.Create("first", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	assertNamedPanic(t, `sqlitex: client "missing" is not registered`, func() {
		registry.MustGet("missing")
	})

	var stack lifecycle.Stack
	if err := stack.Register(func() error {
		for _, client := range []*Client{first, second} {
			if err := client.pool.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
				return fmt.Errorf("日志关闭时连接池仍未关闭: %v", err)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := stack.Register(registry.CloseAll); err != nil {
		t.Fatal(err)
	}
	if err := stack.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.CloseAll(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}
	assertNamedPanic(t, "sqlitex: named clients closed", func() {
		registry.MustGet("first")
	})
	if _, err := registry.Create("third", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("关闭后仍能登记客户端")
	}
}

func TestNewNamedRegistersAndRejectsDuplicate(t *testing.T) {
	previous := namedClients
	namedClients = named.New[*Client]("sqlitex")
	t.Cleanup(func() {
		_ = namedClients.CloseAll()
		namedClients = previous
	})
	name := t.Name()
	cfg := Config{Name: name, DSN: ":memory:"}
	client, err := NewNamed(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := Named(name); got != client {
		t.Fatalf("命名客户端未登记: got=%p, want=%p", got, client)
	}
	if _, err := NewNamed(context.Background(), cfg); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := Named(name); got != client {
		t.Fatalf("重复名称替换了已登记客户端: got=%p", got)
	}
}

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	for _, name := range []string{"", " "} {
		if _, err := NewNamed(context.Background(), Config{Name: name, DSN: ":memory:"}); err == nil {
			t.Fatalf("空名称 %q 被接受", name)
		}
	}
	name := t.Name()
	if _, err := NewNamed(context.Background(), Config{Name: name}); err == nil {
		t.Fatal("无效 DSN 配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("sqlitex: client %q is not registered", name), func() {
		Named(name)
	})
}
