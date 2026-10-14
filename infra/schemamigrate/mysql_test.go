package schemamigrate

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
)

type queryLog struct {
	logger.Interface
	mu      sync.Mutex
	queries []string
}

func (l *queryLog) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	query, _ := fc()
	l.mu.Lock()
	l.queries = append(l.queries, query)
	l.mu.Unlock()
}

func (l *queryLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	queries := l.queries
	l.queries = nil
	return queries
}

// 每次测试仅创建和删除随机命名的专用数据库，绝不迁移 DSN 指向的已有业务表。
func liveDB(t *testing.T) (*gorm.DB, *queryLog) {
	t.Helper()
	dsn := os.Getenv("PIXIU_SCHEMA_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 PIXIU_SCHEMA_MYSQL_DSN；MySQL DDL/锁集成测试需真实 MySQL 8+")
	}
	cfg, err := driver.ParseDSN(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ParseTime = true
	cfg.DBName = ""
	admin, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	adminPool, _ := admin.DB()
	name := fmt.Sprintf("pixiu_schema_test_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE DATABASE " + "`" + name + "`" + " CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci").Error; err != nil {
		_ = adminPool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP DATABASE " + "`" + name + "`").Error; err != nil {
			t.Errorf("清理测试数据库: %v", err)
		}
		_ = adminPool.Close()
	})
	cfg.DBName = name
	log := &queryLog{Interface: logger.Default.LogMode(logger.Silent)}
	db, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: log})
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	t.Cleanup(func() { _ = pool.Close() })
	return db, log
}

func TestMySQLAutoMigrateShardsAndComments(t *testing.T) {
	type card struct {
		ID   uint64 `gorm:"primaryKey"`
		Name string `gorm:"size:64;uniqueIndex"`
	}
	db, log := liveDB(t)
	ctx := context.Background()
	master := func(ctx context.Context) *gorm.DB { return db.WithContext(ctx) }
	tables := []Table{
		{
			Model:     &card{},
			Name:      "cards_01",
			Comment:   `卡片'注释\路径? @Name @Alias @Raw`,
			Charset:   "utf8mb4",
			Collation: "utf8mb4_0900_ai_ci",
		},
		{Model: &card{}, Name: "cards_02", Comment: "第二张表"},
	}
	if report, err := Apply(ctx, master, tables); err != nil || len(report.Tables) != 2 {
		t.Fatalf("迁移失败: %+v %v", report, err)
	}
	if !db.Migrator().HasTable("cards_01") || !db.Migrator().HasTable("cards_02") || db.Migrator().HasTable("cards") {
		t.Fatal("分表名称未正确隔离")
	}
	log.take()
	if _, err := Apply(ctx, master, tables); err != nil {
		t.Fatal(err)
	}
	for _, query := range log.take() {
		if strings.HasPrefix(query, "ALTER TABLE") && strings.Contains(query, " COMMENT = ") {
			t.Fatalf("未变化的表注释不应重复执行 DDL: %s", query)
		}
		if strings.Contains(query, "pixiu_schema_states") || strings.Contains(query, "GET_LOCK") {
			t.Fatalf("不应依赖指纹状态或迁移锁: %s", query)
		}
	}
	tables[0].Comment = `新注释'\? @Name @Alias @Raw`
	// 在同一连接上更改 SQL mode，验证注释转义不依赖连接池恰好归还同一连接。
	if err := db.Connection(func(conn *gorm.DB) error {
		var original string
		if err := conn.Raw("SELECT @@SESSION.sql_mode").Scan(&original).Error; err != nil {
			return err
		}
		defer conn.Exec("SET SESSION sql_mode = ?", original)
		if err := conn.Exec("SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'").Error; err != nil {
			return err
		}
		_, err := Apply(ctx, func(ctx context.Context) *gorm.DB { return conn.WithContext(ctx) }, tables)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var comment string
	if err := db.Raw("SELECT TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='cards_01'").Scan(&comment).Error; err != nil || comment != tables[0].Comment {
		t.Fatalf("表注释不匹配: %q %v", comment, err)
	}
	if db.Migrator().HasTable("pixiu_schema_states") {
		t.Fatal("不应创建状态表")
	}
}

func TestMySQLMigrationFailureReturnsPartialReport(t *testing.T) {
	type card struct {
		ID uint64 `gorm:"primaryKey"`
	}
	db, _ := liveDB(t)
	report, err := Apply(context.Background(), func(ctx context.Context) *gorm.DB { return db.WithContext(ctx) }, []Table{
		{Model: &card{}, Name: "first"},
		{Model: &card{}, Name: "broken", Charset: "invalid_charset"},
		{Model: &card{}, Name: "later"},
	})
	if err == nil || len(report.Tables) != 2 || report.Tables[0].Action != "applied" || report.Tables[1].Action != "failed" {
		t.Fatalf("部分成功及失败信息不正确: %+v %v", report, err)
	}
	if !db.Migrator().HasTable("first") || db.Migrator().HasTable("later") {
		t.Fatal("应保留已完成结果，并在失败后停止")
	}
}

type migrationParent struct {
	ID    uint64 `gorm:"primaryKey"`
	Title string `gorm:"size:64"`
}

type migrationChild struct {
	ID       uint64 `gorm:"primaryKey"`
	ParentID uint64
	Parent   migrationParent
}

func TestMySQLOnlyMigratesDeclarationsWithOwnOptions(t *testing.T) {
	for _, configured := range []int{0, 1} {
		t.Run([]string{"child configured", "parent configured"}[configured], func(t *testing.T) {
			db, _ := liveDB(t)
			master := func(ctx context.Context) *gorm.DB { return db.WithContext(ctx) }
			// 关联表可以后登记；每张表只使用自己的建表选项。
			tables := []Table{{Model: &migrationChild{}}, {Model: &migrationParent{}}}
			tables[configured].Charset = "latin1"
			tables[configured].Collation = "latin1_swedish_ci"
			if _, err := Apply(context.Background(), master, tables[:1]); err != nil {
				t.Fatal(err)
			}
			if db.Migrator().HasTable(&migrationParent{}) {
				t.Fatal("不应创建未登记的关联表")
			}
			for range 2 {
				report, err := Apply(context.Background(), master, tables)
				if err != nil || len(report.Tables) != 2 || report.Tables[0].Name != "migration_children" || report.Tables[1].Name != "migration_parents" {
					t.Fatalf("应按清单顺序迁移显式登记的表: %+v %v", report, err)
				}
			}
			for i, name := range []string{"migration_children", "migration_parents"} {
				want := "utf8mb4_0900_ai_ci"
				if i == configured {
					want = "latin1_swedish_ci"
				}
				var actual string
				if err := db.Raw("SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?", name).Scan(&actual).Error; err != nil || actual != want {
					t.Fatalf("表 %s 排序规则应为 %s，实际为 %q: %v", name, want, actual, err)
				}
			}
			if db.Migrator().HasConstraint(&migrationChild{}, "Parent") {
				t.Fatal("重复迁移已有表也不应自动创建关联外键")
			}
		})
	}
}

func TestMySQLRejectsCurrentDatabasePrefixBeforeSQL(t *testing.T) {
	db, log := liveDB(t)
	db.NamingStrategy = schema.NamingStrategy{TablePrefix: db.Migrator().CurrentDatabase() + "."}
	log.take()
	report, err := Apply(context.Background(), func(ctx context.Context) *gorm.DB { return db.WithContext(ctx) }, []Table{
		{Model: &migrationParent{}, Name: "untouched"},
		{Model: &migrationParent{}, Comment: "带当前库前缀的表"},
	})
	if err == nil || !strings.Contains(err.Error(), "table name") || len(report.Tables) != 0 {
		t.Fatalf("当前库前缀也应在预检时拒绝: %+v %v", report, err)
	}
	if queries := log.take(); len(queries) != 0 {
		t.Fatalf("表名预检不应查询当前库或执行 DDL: %v", queries)
	}
}
