package redisx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
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

func TestNamedRegistryAndLifecycleClose(t *testing.T) {
	registry := named.New[*Client]("redisx")
	cache := &Client{redis: redis.NewClient(&redis.Options{Addr: "test"})}
	session := &Client{redis: redis.NewClient(&redis.Options{Addr: "test"})}
	t.Cleanup(func() {
		_ = cache.Close()
		_ = session.Close()
	})
	for name, client := range map[string]*Client{"cache": cache, "session": session} {
		if _, err := registry.Create(name, func() (*Client, error) {
			return client, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	if got := registry.MustGet("cache"); got != cache {
		t.Fatalf("按名称获取客户端: got=%p", got)
	}
	if _, err := registry.Create("cache", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := registry.MustGet("cache"); got != cache {
		t.Fatalf("重复名称改变已有客户端: got=%p", got)
	}
	assertNamedPanic(t, `redisx: client "missing" is not registered`, func() {
		registry.MustGet("missing")
	})

	var stack lifecycle.Stack
	if err := stack.Register(func() error {
		for _, client := range []*Client{cache, session} {
			if err := client.Client().Ping(context.Background()).Err(); !errors.Is(err, redis.ErrClosed) {
				return fmt.Errorf("日志关闭时 Redis 客户端仍未关闭: %v", err)
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
	assertNamedPanic(t, "redisx: named clients closed", func() {
		registry.MustGet("cache")
	})
	if _, err := registry.Create("new", func() (*Client, error) {
		return &Client{}, nil
	}); err == nil {
		t.Fatal("关闭后仍能登记客户端")
	}
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
	client, err := NewNamed(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := Named(name); got != client {
		t.Fatalf("命名客户端未登记: got=%p", got)
	}
	if _, err := NewNamed(context.Background(), cfg); err == nil {
		t.Fatal("重复名称未被拒绝")
	}
	if got := Named(name); got != client {
		t.Fatalf("重复名称替换了已登记客户端: got=%p", got)
	}
}

func TestNewNamedFailureDoesNotReserveName(t *testing.T) {
	if _, err := NewNamed(context.Background(), Config{}); err == nil {
		t.Fatal("空名称被接受")
	}
	name := t.Name()
	if _, err := NewNamed(context.Background(), Config{Name: name}); err == nil {
		t.Fatal("无效地址配置被接受")
	}
	assertNamedPanic(t, fmt.Sprintf("redisx: client %q is not registered", name), func() {
		Named(name)
	})
}
