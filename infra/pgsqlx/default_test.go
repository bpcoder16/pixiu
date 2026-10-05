package pgsqlx

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

func TestDefaultRegistryAndFailedInitialization(t *testing.T) {
	previous := namedClients
	registry := named.New[*Client]("pgsqlx")
	namedClients = registry
	t.Cleanup(func() {
		_ = registry.CloseAll()
		namedClients = previous
	})
	ctx := context.Background()
	assertNamedPanic(t, "pgsqlx: default client is not registered", func() {
		Default()
	})
	for _, name := range []string{"", " \t "} {
		if client, err := NewDefault(Config{Name: name}); client != nil || err == nil || err.Error() != "pgsqlx: empty database name" {
			t.Fatalf("空名称 %q: client=%v err=%v", name, client, err)
		}
	}
	name := t.Name()
	if client, err := NewDefault(Config{Name: name}); client != nil || err == nil {
		t.Fatalf("无效配置被接受: client=%v err=%v", client, err)
	}
	assertNamedPanic(t, "pgsqlx: default client is not registered", func() {
		Default()
	})
	assertNamedPanic(t, fmt.Sprintf("pgsqlx: client %q is not registered", name), func() {
		Named(name)
	})

	// 使用未建连的真实驱动连接池验证模块门面，避免依赖外部数据库。
	client, pool := testNamedClient(t)
	if _, err := registry.CreateDefault(name, func() (*Client, error) {
		return client, nil
	}); err != nil {
		t.Fatal(err)
	}
	if Default() != client || Named(name) != client {
		t.Fatal("默认与命名查询未返回同一实例")
	}
	for _, duplicateName := range []string{name, "rejected"} {
		if got, err := NewDefault(Config{Name: duplicateName}); got != nil || err == nil || err.Error() != "pgsqlx: default client is already registered" {
			t.Fatalf("重复默认初始化 = %p, %v", got, err)
		}
	}
	if _, err := NewNamed(Config{Name: name}); err == nil || err.Error() != fmt.Sprintf("pgsqlx: client %q is already registered", name) {
		t.Fatalf("默认名称重复登记错误 = %v", err)
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if err := pool.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("默认连接池未关闭: %v", err)
	}
	assertNamedPanic(t, "pgsqlx: named clients closed", func() {
		Default()
	})
	assertNamedPanic(t, "pgsqlx: named clients closed", func() {
		Named(name)
	})
	if got, err := NewDefault(Config{Name: "later"}); got != nil || err == nil || err.Error() != "pgsqlx: named clients closed" {
		t.Fatalf("关闭后默认初始化 = %p, %v", got, err)
	}
}
