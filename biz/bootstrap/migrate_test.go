package bootstrap_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"github.com/bpcoder16/pixiu/lifecycle"
	"gorm.io/gorm"
)

func TestMigrationConfigIsStrictAndDisabledDoesNotAcquireDatabase(t *testing.T) {
	isolatedLog(t, func() {
		dir := t.TempDir()
		if err := env.Init(env.Config{AppName: "migration-test", RunMode: "test", TimeLocation: "UTC", ConfigDirPath: dir, LocalIP: "127.0.0.1"}); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.RunMySQLMigration("cards", nil); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("文件必须存在: %v", err)
		}
		for _, content := range []string{"enabled: invalid\n", "enabled: false\nextra: true\n", "enabled: false\ntimeout: -1s\n", "mode: apply\nowner: test\n"} {
			if err := os.WriteFile(filepath.Join(dir, "migrate.cards.yaml"), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := bootstrap.RunMySQLMigration("cards", nil); err == nil {
				t.Fatalf("非法配置被接受: %q", content)
			}
		}
		if err := os.WriteFile(filepath.Join(dir, "migrate.cards.yaml"), []byte("{}"), 0600); err != nil {
			t.Fatal(err)
		}
		tables := []schemamigrate.Table{{Model: &struct{ ID uint64 }{}}}
		if _, err := bootstrap.RunMySQLMigration("cards", tables); err != nil {
			t.Fatalf("默认关闭，不应查询未注册的实例: %v", err)
		}
		if err := os.WriteFile(filepath.Join(dir, "migrate.cards.yaml"), []byte("enabled: true\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.RunMySQLMigration("cards", nil); err != nil {
			t.Fatalf("空清单不应取实例: %v", err)
		}
		if _, err := bootstrap.RunMySQLMigration("../cards", nil); err == nil {
			t.Fatal("应拒绝实例路径穿越")
		}
	})
}

func TestInitializationDisabledAndEmptyDoNotAcquireDatabase(t *testing.T) {
	isolatedLog(t, func() {
		dir := t.TempDir()
		if err := env.Init(env.Config{AppName: "seed-test", RunMode: "test", TimeLocation: "UTC", ConfigDirPath: dir, LocalIP: "127.0.0.1"}); err != nil {
			t.Fatal(err)
		}
		file := filepath.Join(dir, "migrate.cards.yaml")
		if err := os.WriteFile(file, []byte("enabled: false\n"), 0600); err != nil {
			t.Fatal(err)
		}
		options := bootstrap.SeedOptions{Seeds: []schemamigrate.Seed{{File: "unread.sql"}}}
		if _, err := bootstrap.RunMySQLInitialization("cards", nil, options); err != nil {
			t.Fatalf("disabled must not read seed files or get a database: %v", err)
		}
		if err := os.WriteFile(file, []byte("enabled: true\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.RunMySQLInitialization("cards", nil, bootstrap.SeedOptions{}); err != nil {
			t.Fatalf("empty lists must not get a database: %v", err)
		}
		if _, err := bootstrap.RunMySQLInitialization("cards", nil, bootstrap.SeedOptions{Timeout: -time.Second}); err == nil {
			t.Fatal("negative seed timeout must fail")
		}
	})
}

type bootstrapSeedOption struct {
	ID uint
}

func TestInitializationSeedsUseIndependentCallerTimeout(t *testing.T) {
	for _, timeout := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(timeout.String(), func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				server := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "cards", mysqlConfigYAML(server.port))
				bootstrap.MustRegisterMySQL("cards", true)
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "migrate.cards.yaml"), []byte("enabled: true\ntimeout: 1ns\n"), 0600); err != nil {
					t.Fatal(err)
				}
				options := bootstrap.SeedOptions{
					Files:   fstest.MapFS{"options.sql": &fstest.MapFile{Data: []byte("INSERT INTO bootstrap_seed_options (id) VALUES (1);")}},
					Seeds:   []schemamigrate.Seed{{Model: &bootstrapSeedOption{}, File: "options.sql"}},
					Timeout: timeout,
				}
				queries := 0
				// 协议桩负责连接与事务；查询回调观察真实启动入口传递的 Context。
				db := mysqlx.Named("cards").MasterDB(context.Background())
				if err := db.Callback().Query().Replace("gorm:query", func(tx *gorm.DB) {
					queries++
					deadline, ok := tx.Statement.Context.Deadline()
					want := timeout
					if want == 0 {
						want = 2 * time.Minute
					}
					remaining := time.Until(deadline)
					if !ok || remaining <= 0 || remaining > want || remaining < want-time.Second {
						t.Errorf("seed deadline = %v, want independent budget %v", remaining, want)
					}
					if timeout > 0 {
						<-tx.Statement.Context.Done()
						tx.AddError(tx.Statement.Context.Err())
						return
					}
					*tx.Statement.Dest.(*int64) = 1
					tx.RowsAffected = 1
				}); err != nil {
					t.Fatal(err)
				}
				// 没有表声明也允许初始化已存在表的 seeds。
				report, err := bootstrap.RunMySQLInitialization("cards", nil, options)
				if timeout == 0 && err != nil || timeout > 0 && !errors.Is(err, context.DeadlineExceeded) || queries != 1 || len(report.Tables) != 0 {
					t.Fatalf("seed-only initialization: report=%+v queries=%d err=%v", report, queries, err)
				}
				// 结构迁移失败不能继续查询或执行 seeds。
				if _, err := bootstrap.RunMySQLInitialization("cards", []schemamigrate.Table{{}}, options); err == nil || queries != 1 {
					t.Fatalf("migration failure must stop seeds: queries=%d err=%v", queries, err)
				}
			})
		})
	}
}
