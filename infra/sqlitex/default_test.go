package sqlitex

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

func TestNewDefaultAndCloseAll(t *testing.T) {
	previous := namedClients
	registry := named.New[*Client]("sqlitex")
	namedClients = registry
	t.Cleanup(func() {
		_ = registry.CloseAll()
		namedClients = previous
	})
	ctx := context.Background()
	assertNamedPanic(t, "sqlitex: default client is not registered", func() {
		Default()
	})
	for _, name := range []string{"", " \t "} {
		if client, err := NewDefault(Config{Name: name, DSN: ":memory:"}); client != nil || err == nil || err.Error() != "sqlitex: empty database name" {
			t.Fatalf("空名称 %q: client=%v err=%v", name, client, err)
		}
	}
	name := t.Name()
	if client, err := NewDefault(Config{Name: name}); client != nil || err == nil {
		t.Fatalf("无效 DSN 被接受: client=%v err=%v", client, err)
	}
	assertNamedPanic(t, "sqlitex: default client is not registered", func() {
		Default()
	})
	cfg := Config{
		Name: name,
		DSN:  ":memory:",
	}
	client, err := NewDefault(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if Default() != client || Named(name) != client {
		t.Fatal("默认与命名查询未返回同一实例")
	}
	if err := Default().DB(ctx).Exec("CREATE TABLE records (id INTEGER)").Error; err != nil {
		t.Fatal(err)
	}
	if err := Default().DB(ctx).Exec("INSERT INTO records VALUES (7)").Error; err != nil {
		t.Fatal(err)
	}
	var id int
	if err := Named(name).DB(ctx).Raw("SELECT id FROM records").Scan(&id).Error; err != nil || id != 7 {
		t.Fatalf("命名查询未共享默认库: id=%d, err=%v", id, err)
	}
	for _, duplicateName := range []string{name, "rejected"} {
		if got, err := NewDefault(Config{Name: duplicateName}); got != nil || err == nil || err.Error() != "sqlitex: default client is already registered" {
			t.Fatalf("重复默认初始化 = %p, %v", got, err)
		}
	}
	if _, err := NewNamed(cfg); err == nil || err.Error() != fmt.Sprintf("sqlitex: client %q is already registered", name) {
		t.Fatalf("默认名称重复登记错误 = %v", err)
	}
	cfg.Name = "other"
	other, err := NewNamed(cfg)
	if err != nil {
		t.Fatal(err)
	}
	independent, err := New(cfg)
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
		if err := shared.pool.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
			t.Fatalf("共享连接池未关闭: %v", err)
		}
	}
	if err := independent.pool.PingContext(ctx); err != nil {
		t.Fatalf("CloseAll 影响了独立客户端: %v", err)
	}
	assertNamedPanic(t, "sqlitex: named clients closed", func() {
		Default()
	})
	assertNamedPanic(t, "sqlitex: named clients closed", func() {
		Named(name)
	})
	if got, err := NewDefault(cfg); got != nil || err == nil || err.Error() != "sqlitex: named clients closed" {
		t.Fatalf("关闭后默认初始化 = %p, %v", got, err)
	}
}
