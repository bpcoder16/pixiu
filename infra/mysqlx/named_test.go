package mysqlx

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func testNamedClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	connector, err := mysqldriver.NewConnector(mysqldriver.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(connector)
	t.Cleanup(func() { _ = pool.Close() })
	return &Client{pools: []*sql.DB{pool}}, pool
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
	registry := named.New[*Client]("mysqlx")
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
	assertNamedPanic(t, `mysqlx: client "missing" is not registered`, func() {
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
	assertNamedPanic(t, "mysqlx: named clients closed", func() {
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
	name := t.Name()
	if _, err := NewNamed(context.Background(), Config{Name: name}); err == nil {
		t.Fatal("无效端点配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("mysqlx: client %q is not registered", name), func() { Named(name) })
}
