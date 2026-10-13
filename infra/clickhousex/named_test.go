package clickhousex

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func testNamedClient(t *testing.T) (*Client, *sql.DB) {
	t.Helper()
	pool := clickhouse.OpenDB(&clickhouse.Options{
		Addr: []string{"127.0.0.1:9000"},
	})
	t.Cleanup(func() { _ = pool.Close() })
	return newTestClientWithPool(t, pool), pool
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
	if _, err := NewNamed(Config{Name: " \t "}); err == nil || err.Error() != "clickhousex: empty database name" {
		t.Fatalf("仅含空白的逻辑库名未被拒绝: %v", err)
	}
	name := t.Name()
	if _, err := NewNamed(Config{Name: name}); err == nil {
		t.Fatal("无效端点配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("clickhousex: client %q is not registered", name), func() {
		Named(name)
	})
}
