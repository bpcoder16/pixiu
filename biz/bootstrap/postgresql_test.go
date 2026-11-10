//go:build !pixiu_bootstrap_no_postgresql

package bootstrap_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/postgresqlx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func writePostgreSQLConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "postgresql."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requirePostgreSQLPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func TestPostgreSQLDeclarationsAndUnregisteredModule(t *testing.T) {
	t.Run("declarations", func(t *testing.T) {
		isolatedLog(t, func() {
			for _, name := range []string{"", "../orders", "a.b", "实例"} {
				requirePostgreSQLPanic(t, "invalid PostgreSQL name", func() { bootstrap.MustRegisterPostgreSQL(name, true) })
			}
			bootstrap.MustRegisterPostgreSQL("orders_1-main", true)
			requirePostgreSQLPanic(t, "already registered", func() { bootstrap.MustRegisterPostgreSQL("orders_1-main", false) })
			requirePostgreSQLPanic(t, "default PostgreSQL", func() { bootstrap.MustRegisterPostgreSQL("reports", true) })
			bootstrap.MustRegisterPostgreSQL("reports", false)
			if recoverValue(func() { postgresqlx.Named("orders_1-main") }) == nil {
				t.Fatal("声明阶段不得创建客户端")
			}
		})
	})
	t.Run("unregistered", func(t *testing.T) {
		isolatedLog(t, func() {
			cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
			writePostgreSQLConfig(t, "unused", "invalid: [")
			var resources lifecycle.Stack
			defer resources.Close()
			bootstrap.MustBaseInit(cfg, &resources)
			requirePostgreSQLPanic(t, "initialization has started", func() { bootstrap.MustRegisterPostgreSQL("late", true) })
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := postgresqlx.NewNamed(postgresqlx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
				t.Fatalf("未声明时不应登记 PostgreSQL 关闭: %v", err)
			}
		})
	})
}

func TestPostgreSQLParsesAllFilesBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "postgresql.other.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "identity", content: "name: other\n", want: "name"},
		{name: "default", content: "isDefault: true\n", want: "isdefault"},
		{name: "type", content: "disableStatementCache: enabled\n", want: "disableStatementCache"},
		{name: "duration", content: "initTimeout: later\n", want: "initTimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				// 第一份配置语法正确但无法建连；应先报告第二份文件的错误。
				writePostgreSQLConfig(t, "orders", "database: test\nusername: test\nmaster:\n  host: 127.0.0.1\n  port: 1\n")
				if tc.name != "missing" {
					writePostgreSQLConfig(t, "other", tc.content)
				}
				bootstrap.MustRegisterPostgreSQL("orders", true)
				bootstrap.MustRegisterPostgreSQL("other", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requirePostgreSQLPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "postgresql.other.yaml") || tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("配置错误路径或底层错误链丢失: %v", err)
				}
			})
		})
	}
}

func TestPostgreSQLInitializationConfiguration(t *testing.T) {
	isolatedLog(t, func() {
		loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		if _, err := bootstrap.RunPostgreSQLInitialization("../orders", nil, bootstrap.SeedOptions{}); err == nil {
			t.Fatal("应先拒绝非法实例名")
		}
		if _, err := bootstrap.RunPostgreSQLInitialization("orders", nil, bootstrap.SeedOptions{}); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("迁移配置必须存在: %v", err)
		}
		file := filepath.Join(env.ConfigDirPath(), "migrate.orders.yaml")
		for _, content := range []string{"enabled: invalid\n", "enabled: false\nextra: true\n", "timeout: -1s\n"} {
			if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := bootstrap.RunPostgreSQLInitialization("orders", nil, bootstrap.SeedOptions{}); err == nil {
				t.Fatalf("接受了非法迁移配置: %q", content)
			}
		}
		if err := os.WriteFile(file, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		// 未声明实例，也没有 SQL 文件；默认关闭必须跳过两者。
		if _, err := bootstrap.RunPostgreSQLInitialization("orders", []schemamigrate.Table{{}}, bootstrap.SeedOptions{
			Seeds: []schemamigrate.Seed{{File: "unread.sql"}},
		}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(file, []byte("enabled: true\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.RunPostgreSQLInitialization("orders", nil, bootstrap.SeedOptions{}); err != nil {
			t.Fatalf("空清单不应查询实例: %v", err)
		}
		if _, err := bootstrap.RunPostgreSQLInitialization("orders", nil, bootstrap.SeedOptions{Timeout: -time.Second}); err == nil {
			t.Fatal("应拒绝负数种子期限")
		}
	})
}
