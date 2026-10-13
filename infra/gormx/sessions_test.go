package gormx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bpcoder16/pixiu/infra/gormx"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestSessionsKeepRoutesAndRequestsIndependent(t *testing.T) {
	master := newTestDB(t, "master")
	slave := newTestDB(t, "slave")
	ready := false
	provider := func(db *gorm.DB) func(context.Context) *gorm.DB {
		return func(ctx context.Context) *gorm.DB {
			if !ready {
				t.Fatal("构造 Sessions 不应获取数据库会话")
			}
			return db.WithContext(ctx)
		}
	}
	sessions := gormx.NewSessions(provider(master), provider(slave))
	ready = true
	for _, tc := range []struct {
		name string
		db   func(context.Context) *gorm.DB
	}{
		{"master", sessions.MasterDB},
		{"slave", sessions.SlaveDB},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			var row struct{ Value string }
			if err := tc.db(ctx).Table("records").Where("id = ?", 1).Take(&row).Error; err != nil || row.Value != tc.name {
				t.Fatalf("查询应使用对应数据库: value=%q err=%v", row.Value, err)
			}
			var count int64
			if err := tc.db(ctx).Table("records").Count(&count).Error; err != nil || count != 2 {
				t.Fatalf("查询条件不应跨会话保留: count=%d err=%v", count, err)
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if err := tc.db(canceled).Exec("SELECT 1").Error; !errors.Is(err, context.Canceled) {
				t.Fatalf("查询应使用本次 Context: %v", err)
			}
			if err := tc.db(ctx).Exec("SELECT 1").Error; err != nil {
				t.Fatalf("取消不应影响后续请求: %v", err)
			}
		})
	}
}

func TestSessionsRejectMissingDependenciesAndContext(t *testing.T) {
	provider := func(context.Context) *gorm.DB {
		t.Fatal("无效调用不应获取数据库会话")
		return nil
	}
	for _, tc := range []struct {
		name string
		run  func()
	}{
		{"nil master", func() { gormx.NewSessions(nil, provider) }},
		{"nil slave", func() { gormx.NewSessions(provider, nil) }},
		{"nil master context", func() { gormx.NewSessions(provider, provider).MasterDB(nil) }},
		{"nil slave context", func() { gormx.NewSessions(provider, provider).SlaveDB(nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("缺少必需依赖或 Context 时应 panic")
				}
			}()
			tc.run()
		})
	}
}

func newTestDB(t *testing.T, value string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := db.Exec("CREATE TABLE records (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO records (id, value) VALUES (1, ?), (2, ?)", value, value).Error; err != nil {
		t.Fatal(err)
	}
	return db
}
