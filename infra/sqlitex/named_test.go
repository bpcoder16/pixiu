package sqlitex

import (
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
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

func TestNewNamedRegistersAndRejectsDuplicate(t *testing.T) {
	previous := namedClients
	namedClients = named.New[*Client]("sqlitex")
	t.Cleanup(func() {
		_ = namedClients.CloseAll()
		namedClients = previous
	})
	name := t.Name()
	cfg := Config{Name: name, DSN: ":memory:"}
	client, err := NewNamed(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := Named(name); got != client {
		t.Fatalf("命名客户端未登记: got=%p, want=%p", got, client)
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := Named(name); got != client {
		t.Fatalf("重复名称替换了已登记客户端: got=%p", got)
	}
}

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	for _, name := range []string{"", " "} {
		if _, err := NewNamed(Config{Name: name, DSN: ":memory:"}); err == nil {
			t.Fatalf("空名称 %q 被接受", name)
		}
	}
	name := t.Name()
	if _, err := NewNamed(Config{Name: name}); err == nil {
		t.Fatal("无效 DSN 配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("sqlitex: client %q is not registered", name), func() {
		Named(name)
	})
}
