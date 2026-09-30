package redisx

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/redis/go-redis/v9"
)

func TestNewDefaultAndCloseAll(t *testing.T) {
	previous := namedClients
	registry := named.New[*Client]("redisx")
	namedClients = registry
	t.Cleanup(func() {
		_ = registry.CloseAll()
		namedClients = previous
	})
	ctx := context.Background()
	assertNamedPanic(t, "redisx: default client is not registered", func() {
		Default()
	})
	for _, name := range []string{"", " \t "} {
		if client, err := NewDefault(ctx, Config{Name: name}); client != nil || err == nil || err.Error() != "redisx: empty name" {
			t.Fatalf("空名称 %q: client=%v err=%v", name, client, err)
		}
	}
	name := t.Name()
	if client, err := NewDefault(nil, Config{Name: name}); client != nil || err == nil || err.Error() != "redisx: nil context" {
		t.Fatalf("nil context: client=%v err=%v", client, err)
	}
	if client, err := NewDefault(ctx, Config{Name: name}); client != nil || err == nil {
		t.Fatalf("无效地址被接受: client=%v err=%v", client, err)
	}
	assertNamedPanic(t, "redisx: default client is not registered", func() {
		Default()
	})
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
	client, err := NewDefault(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if Default() != client || Named(name) != client {
		t.Fatal("默认与命名查询未返回同一实例")
	}
	if err := Default().Client().Ping(ctx).Err(); err != nil {
		t.Fatalf("默认客户端不可用: %v", err)
	}
	for _, duplicateName := range []string{name, "rejected"} {
		if got, err := NewDefault(ctx, Config{Name: duplicateName}); got != nil || err == nil || err.Error() != "redisx: default client is already registered" {
			t.Fatalf("重复默认初始化 = %p, %v", got, err)
		}
	}
	if _, err := NewNamed(ctx, cfg); err == nil || err.Error() != fmt.Sprintf("redisx: client %q is already registered", name) {
		t.Fatalf("默认名称重复登记错误 = %v", err)
	}
	cfg.Name = "session"
	other, err := NewNamed(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	independent, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = independent.Close() })
	if Default() != client || Named(cfg.Name) != other {
		t.Fatal("其他客户端改变了默认实例")
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if err := CloseAll(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}
	for _, shared := range []*Client{client, other} {
		if err := shared.Client().Ping(ctx).Err(); !errors.Is(err, redis.ErrClosed) {
			t.Fatalf("共享客户端未关闭: %v", err)
		}
	}
	if err := independent.Client().Ping(ctx).Err(); err != nil {
		t.Fatalf("CloseAll 影响了独立客户端: %v", err)
	}
	assertNamedPanic(t, "redisx: named clients closed", func() {
		Default()
	})
	assertNamedPanic(t, "redisx: named clients closed", func() {
		Named(name)
	})
	if got, err := NewDefault(ctx, cfg); got != nil || err == nil || err.Error() != "redisx: named clients closed" {
		t.Fatalf("关闭后默认初始化 = %p, %v", got, err)
	}
}
