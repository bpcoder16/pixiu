package pgsqlx

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

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

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	if _, err := NewNamed(Config{}); err == nil {
		t.Fatal("空名称被接受")
	}
	if _, err := NewNamed(Config{Name: " \t "}); err == nil || !strings.Contains(err.Error(), "empty database name") {
		t.Fatalf("全空白名称被接受: %v", err)
	}
	name := t.Name()
	if _, err := NewNamed(Config{Name: name}); err == nil {
		t.Fatal("无效端点配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("pgsqlx: client %q is not registered", name), func() {
		Named(name)
	})
}
