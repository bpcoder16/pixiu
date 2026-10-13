package mysqlx

import (
	"database/sql"
	"fmt"
	"testing"

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
	for _, name := range []string{"", " \t"} {
		if client, err := NewNamed(Config{Name: name}); client != nil || err == nil || err.Error() != "mysqlx: empty database name" {
			t.Fatalf("空名称 %q: client=%v err=%v", name, client, err)
		}
	}
	name := t.Name()
	if _, err := NewNamed(Config{Name: name}); err == nil {
		t.Fatal("无效端点配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("mysqlx: client %q is not registered", name), func() { Named(name) })
}
