package bootstrap_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
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
