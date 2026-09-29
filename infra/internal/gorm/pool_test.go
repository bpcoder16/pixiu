package gorm

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	gormsqlite "gorm.io/driver/sqlite"
	gormlib "gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestOpenKeepsDefaultsAndAllowsExtraConfig(t *testing.T) {
	diagnostic := logger.Default.LogMode(logger.Silent)
	newPool := func() *sql.DB {
		pool, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pool.Close() })
		return pool
	}

	legacyPool := newPool()
	legacy, err := Open(legacyPool, gormsqlite.New(gormsqlite.Config{Conn: legacyPool}), diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	if !legacy.Config.DisableAutomaticPing || legacy.Config.SkipDefaultTransaction || legacy.Config.Logger != diagnostic {
		t.Fatalf("旧调用配置不兼容: %+v", legacy.Config)
	}

	configuredPool := newPool()
	configured, err := Open(configuredPool, gormsqlite.New(gormsqlite.Config{Conn: configuredPool}), diagnostic, func(config *gormlib.Config) {
		config.SkipDefaultTransaction = true
		config.DisableAutomaticPing = false
		config.Logger = logger.Default
	})
	if err != nil {
		t.Fatal(err)
	}
	if !configured.Config.SkipDefaultTransaction || !configured.Config.DisableAutomaticPing || configured.Config.Logger != diagnostic {
		t.Fatalf("扩展配置未保留共享约束: %+v", configured.Config)
	}
}
