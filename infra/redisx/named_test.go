package redisx

import (
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/redis/go-redis/v9"
)

func assertNamedPanic(t *testing.T, want string, lookup func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("命名查询的 panic = %v, want %q", got, want)
		}
	}()
	lookup()
}

func TestNewNamedRegistersAndRejectsDuplicate(t *testing.T) {
	previous := namedClients
	namedClients = named.New[*Client]("redisx")
	t.Cleanup(func() {
		_ = namedClients.CloseAll()
		namedClients = previous
	})
	name := t.Name()
	dialer, _ := pingDialer("+PONG\r\n", false)
	cfg := Config{
		Name: name,
		Options: redis.Options{
			Addr:            "test",
			Dialer:          dialer,
			Protocol:        2,
			DisableIdentity: true,
			MaxRetries:      -1,
		},
	}
	client, err := NewNamed(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := Named(name); got != client {
		t.Fatalf("命名客户端未登记: got=%p", got)
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := Named(name); got != client {
		t.Fatalf("重复名称替换了已登记客户端: got=%p", got)
	}
}

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	if _, err := NewNamed(Config{}); err == nil {
		t.Fatal("空名称被接受")
	}
	name := t.Name()
	if _, err := NewNamed(Config{Name: name}); err == nil {
		t.Fatal("无效地址配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("redisx: client %q is not registered", name), func() {
		Named(name)
	})
}
