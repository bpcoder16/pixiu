package pgsqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

func testNamedClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	config, err := pgx.ParseConfig("postgres://reader@localhost/orders?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool := stdlib.OpenDB(*config)
	t.Cleanup(func() { _ = pool.Close() })
	return testClusterClient(t, nil, pool), pool
}

func assertNamedPanic(t *testing.T, want string, lookup func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("命名查询的 panic = %v, want %q", got, want)
		}
	}()
	lookup()
}

func TestNamedRegistryAndLifecycleClose(t *testing.T) {
	registry := named.New[*Client]("pgsqlx")
	orders, ordersPool := testNamedClient(t)
	users, usersPool := testNamedClient(t)
	for name, client := range map[string]*Client{"orders": orders, "users": users} {
		if _, err := registry.Create(name, func() (*Client, error) {
			return client, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if got := registry.MustGet("orders"); got != orders {
		t.Fatalf("按名称获取客户端: got=%p", got)
	}
	if _, err := registry.Create("orders", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := registry.MustGet("orders"); got != orders {
		t.Fatalf("重复名称改变已有客户端: got=%p", got)
	}
	assertNamedPanic(t, `pgsqlx: client "missing" is not registered`, func() {
		registry.MustGet("missing")
	})

	var stack lifecycle.Stack
	if err := stack.Register(func() error {
		for _, pool := range []*sql.DB{ordersPool, usersPool} {
			if err := pool.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
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
	assertNamedPanic(t, "pgsqlx: named clients closed", func() {
		registry.MustGet("orders")
	})
	if _, err := registry.Create("new", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("关闭后仍能登记客户端")
	}
}

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	if _, err := NewNamed(context.Background(), Config{}); err == nil {
		t.Fatal("空名称被接受")
	}
	if _, err := NewNamed(context.Background(), Config{Name: " \t "}); err == nil || !strings.Contains(err.Error(), "empty database name") {
		t.Fatalf("全空白名称被接受: %v", err)
	}
	name := t.Name()
	if _, err := NewNamed(context.Background(), Config{Name: name}); err == nil {
		t.Fatal("无效端点配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("pgsqlx: client %q is not registered", name), func() {
		Named(name)
	})
}
