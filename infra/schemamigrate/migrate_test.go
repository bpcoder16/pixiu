package schemamigrate_test

import (
	"context"
	"errors"
	"testing"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"github.com/bpcoder16/pixiu/infra/sqlitex"
	"gorm.io/gorm"
)

func testDB(t *testing.T) *gorm.DB {
	t.Helper()
	client, err := sqlitex.New(sqlitex.Config{Name: "migration-test", DSN: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client.DB(context.Background())
}

func provider(db *gorm.DB) func(context.Context) *gorm.DB {
	return func(ctx context.Context) *gorm.DB { return db.WithContext(ctx) }
}

func TestApplyUsesAutoMigrateOnEveryRun(t *testing.T) {
	type cardV1 struct {
		ID   uint64 `gorm:"primaryKey"`
		Name string `gorm:"index:idx_name"`
		Age  int    `gorm:"check:age_valid,age >= 0"`
	}
	type cardV2 struct {
		ID   uint64 `gorm:"primaryKey"`
		Name string `gorm:"index:idx_name"`
		Age  int    `gorm:"check:age_valid,age >= 0"`
		Note string
	}
	db := testDB(t)
	ctx := context.Background()
	tables := []schemamigrate.Table{{Model: &cardV1{}, Name: "cards"}}
	for range 2 {
		report, err := schemamigrate.Apply(ctx, provider(db), tables)
		if err != nil || len(report.Tables) != 1 || report.Tables[0].Action != "applied" {
			t.Fatalf("AutoMigrate: %+v %v", report, err)
		}
	}
	if err := db.Table("cards").Create(&cardV1{Name: "existing", Age: 18}).Error; err != nil {
		t.Fatal(err)
	}
	tables[0].Model = &cardV2{}
	if _, err := schemamigrate.Apply(ctx, provider(db), tables); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn("cards", "note") {
		t.Fatal("Model 增列未生效")
	}
	if err := db.Migrator().DropIndex("cards", "idx_name"); err != nil {
		t.Fatal(err)
	}
	if _, err := schemamigrate.Apply(ctx, provider(db), tables); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasIndex("cards", "idx_name") {
		t.Fatal("每次调用应由 AutoMigrate 检查实际结构并补齐索引")
	}
	var count int64
	if err := db.Table("cards").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("已有数据应保留: %d %v", count, err)
	}
	if db.Migrator().HasTable("pixiu_schema_states") {
		t.Fatal("简化版不创建迁移状态表")
	}
}

type relatedParent struct {
	ID uint64 `gorm:"primaryKey"`
}
type relatedChild struct {
	ID       uint64 `gorm:"primaryKey"`
	ParentID uint64
	Parent   relatedParent `gorm:"foreignKey:ParentID"`
}

func TestApplyKeepsGORMRelationshipAndTableNaming(t *testing.T) {
	db := testDB(t)
	if _, err := schemamigrate.Apply(context.Background(), provider(db), []schemamigrate.Table{{Model: &relatedChild{}}}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasTable(&relatedParent{}) || !db.Migrator().HasTable(&relatedChild{}) {
		tables, _ := db.Migrator().GetTables()
		t.Fatalf("未指定物理表名时保留 GORM 的关联迁移，实际表: %v", tables)
	}
}

func TestApplyEmptyListAndErrors(t *testing.T) {
	if _, err := schemamigrate.Apply(context.Background(), func(context.Context) *gorm.DB {
		t.Fatal("空清单不应获取数据库")
		return nil
	}, nil); err != nil {
		t.Fatal(err)
	}
	want := errors.New("database unavailable")
	db := testDB(t)
	db.AddError(want)
	if _, err := schemamigrate.Apply(context.Background(), provider(db), []schemamigrate.Table{{Model: &relatedParent{}}}); !errors.Is(err, want) {
		t.Fatalf("错误链丢失: %v", err)
	}
}

func TestApplyValidatesDeclarationsBeforeDDL(t *testing.T) {
	db := testDB(t)
	tables := []schemamigrate.Table{{Model: &relatedParent{}, Name: "valid"}, {Model: nil, Name: "invalid"}}
	if _, err := schemamigrate.Apply(context.Background(), provider(db), tables); err == nil {
		t.Fatal("应拒绝 nil Model")
	}
	if db.Migrator().HasTable("valid") {
		t.Fatal("基础声明校验应先于 DDL")
	}
}

func TestApplyCanceledContextDoesNotCreateTable(t *testing.T) {
	db := testDB(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := schemamigrate.Apply(ctx, provider(db), []schemamigrate.Table{{Model: &relatedParent{}, Name: "canceled"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("应保留取消错误: %v", err)
	}
	if db.Migrator().HasTable("canceled") {
		t.Fatal("取消后不应开始迁移")
	}
}
