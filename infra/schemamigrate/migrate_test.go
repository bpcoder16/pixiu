package schemamigrate_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"github.com/bpcoder16/pixiu/infra/sqlitex"
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
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
	ID   uint64 `gorm:"primaryKey"`
	Name string
}
type relatedChild struct {
	ID       uint64 `gorm:"primaryKey"`
	ParentID uint64
	Parent   relatedParent `gorm:"foreignKey:ParentID"`
	Tags     []relatedTag  `gorm:"many2many:related_child_tags"`
}

type relatedTag struct {
	ID uint64 `gorm:"primaryKey"`
}

func TestApplyOnlyMigratesDeclaredTables(t *testing.T) {
	for _, tc := range []struct {
		name     string
		table    schemamigrate.Table
		wantName string
	}{
		{
			name:     "default name",
			table:    schemamigrate.Table{Model: &relatedChild{}},
			wantName: "related_children",
		},
		{
			name: "shard",
			table: schemamigrate.Table{
				Model: &relatedChild{},
				Name:  "children_01",
			},
			wantName: "children_01",
		},
		{
			name:     "unregistered qualified association",
			table:    schemamigrate.Table{Model: &qualifiedChild{}},
			wantName: "qualified_children",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			for range 2 {
				report, err := schemamigrate.Apply(context.Background(), provider(db), []schemamigrate.Table{tc.table})
				if err != nil || len(report.Tables) != 1 || report.Tables[0].Name != tc.wantName {
					t.Fatalf("应只迁移登记的表: %+v %v", report, err)
				}
				if !db.Migrator().HasColumn(tc.wantName, "parent_id") {
					t.Fatal("关联 ID 仍应作为普通列迁移")
				}
				if db.Table(tc.wantName).Migrator().HasConstraint(tc.table.Model, "Parent") {
					t.Fatal("不应自动创建关联外键")
				}
			}
			for _, name := range []string{"related_parents", "related_tags", "related_child_tags"} {
				if db.Migrator().HasTable(name) {
					t.Fatalf("不应自动创建未登记的关联表或中间表: %s", name)
				}
			}
			// 调用方直接使用 GORM 时仍保留原有的关联迁移行为。
			if err := db.AutoMigrate(&relatedChild{}); err != nil {
				t.Fatal(err)
			}
			if !db.Migrator().HasTable(&relatedParent{}) || !db.Migrator().HasTable("related_child_tags") {
				t.Fatal("Apply 不应修改调用方的关联迁移配置")
			}
		})
	}
}

func TestApplyDoesNotChangeUnregisteredExistingTable(t *testing.T) {
	db := testDB(t)
	if err := db.Exec("CREATE TABLE related_parents (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := schemamigrate.Apply(context.Background(), provider(db), []schemamigrate.Table{{Model: &relatedChild{}}}); err != nil {
		t.Fatal(err)
	}
	if db.Migrator().HasColumn(&relatedParent{}, "Name") {
		t.Fatal("不应给未登记的已有表增列")
	}
	tables := []schemamigrate.Table{{Model: &relatedChild{}}, {Model: &relatedParent{}}}
	report, err := schemamigrate.Apply(context.Background(), provider(db), tables)
	if err != nil || len(report.Tables) != 2 || report.Tables[0].Name != "related_children" || report.Tables[1].Name != "related_parents" {
		t.Fatalf("关联表显式登记后应按清单顺序迁移: %+v %v", report, err)
	}
	if !db.Migrator().HasColumn(&relatedParent{}, "Name") {
		t.Fatal("显式登记后应更新已有表")
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

type qualifiedParent struct {
	ID uint64 `gorm:"primaryKey"`
}

func (qualifiedParent) TableName() string { return "testdb.parents" }

type qualifiedChild struct {
	ID       uint64 `gorm:"primaryKey"`
	ParentID uint64
	Parent   qualifiedParent
}

func TestApplyRejectsQualifiedTableNamesBeforeDDL(t *testing.T) {
	for _, tc := range []struct {
		name   string
		table  schemamigrate.Table
		prefix string
	}{
		{name: "explicit", table: schemamigrate.Table{Model: &relatedParent{}, Name: "testdb.parents"}},
		{name: "model", table: schemamigrate.Table{Model: &qualifiedParent{}}},
		{name: "naming", table: schemamigrate.Table{Model: &relatedParent{}}, prefix: "testdb."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := testDB(t)
			db.NamingStrategy = schema.NamingStrategy{TablePrefix: tc.prefix}
			report, err := schemamigrate.Apply(context.Background(), provider(db), []schemamigrate.Table{
				{Model: &relatedParent{}, Name: "untouched"},
				tc.table,
			})
			if err == nil || !strings.Contains(err.Error(), "table name") || len(report.Tables) != 0 {
				t.Fatalf("带库名的表应在整批 DDL 前拒绝: %+v %v", report, err)
			}
			tables, err := db.Migrator().GetTables()
			if err != nil || len(tables) != 0 {
				t.Fatalf("预检失败不应创建任何表: %v %v", tables, err)
			}
		})
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
