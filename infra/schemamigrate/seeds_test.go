package schemamigrate_test

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type seedOption struct {
	ID        uint `gorm:"primaryKey"`
	Name      string
	Enabled   bool
	DeletedAt gorm.DeletedAt
}

func seedDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "seed.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	if err := db.AutoMigrate(&seedOption{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func seedSQL(sql string) fs.FS {
	return fstest.MapFS{"options.sql": &fstest.MapFile{Data: []byte(sql)}}
}

func seedRows(t *testing.T, db *gorm.DB) []seedOption {
	t.Helper()
	var rows []seedOption
	if err := db.Unscoped().
		Order("id").
		Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestSeedsReconcileCountsWithinScope(t *testing.T) {
	for _, initialCount := range []int{0, 1, 2, 3} {
		t.Run(string(rune('0'+initialCount)), func(t *testing.T) {
			db := seedDB(t)
			if err := db.Create(&seedOption{Name: "other", Enabled: true}).Error; err != nil {
				t.Fatal(err)
			}
			for range initialCount {
				if err := db.Create(&seedOption{Name: "old"}).Error; err != nil {
					t.Fatal(err)
				}
			}
			files := seedSQL("-- comment\r\n\nINSERT INTO seed_options (name, enabled) VALUES ('one;still one', false);\r\ninsert into seed_options (name, enabled) VALUES ('two', false);\n")
			seeds := []schemamigrate.Seed{{Model: &seedOption{}, Where: map[string]any{"enabled": false}, File: "options.sql"}}
			for range 2 {
				// 历史条件必须隔离，否则会把已有数据误判成空范围。
				if err := schemamigrate.ApplySeeds(context.Background(), provider(db.Where("id < 0")), files, seeds); err != nil {
					t.Fatal(err)
				}
			}
			rows := seedRows(t, db)
			if len(rows) != 3 || rows[0].Name != "other" || !rows[0].Enabled {
				t.Fatalf("must retain other scopes and reconcile only disabled rows: %+v", rows)
			}
			if initialCount == 2 {
				if rows[1].Name != "old" || rows[2].Name != "old" {
					t.Fatalf("matching counts must retain existing content: %+v", rows)
				}
			} else if rows[1].Name != "one;still one" || rows[2].Name != "two" {
				t.Fatalf("mismatching counts must reload the SQL: %+v", rows)
			}
		})
	}
}

func TestSeedsWithoutWhereReplacePhysicalRows(t *testing.T) {
	db := seedDB(t)
	old := seedOption{Name: "soft deleted"}
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Delete(&old).Error; err != nil {
		t.Fatal(err)
	}
	seeds := []schemamigrate.Seed{{Model: &seedOption{}, File: "options.sql"}}
	if err := schemamigrate.ApplySeeds(context.Background(), provider(db), seedSQL("INSERT INTO seed_options (name) VALUES ('unused');"), seeds); err != nil {
		t.Fatal(err)
	}
	if rows := seedRows(t, db); len(rows) != 1 || rows[0].ID != old.ID || !rows[0].DeletedAt.Valid {
		t.Fatalf("matching physical count must include soft-deleted rows: %+v", rows)
	}
	files := seedSQL("INSERT INTO seed_options (name) VALUES ('one');\nINSERT INTO seed_options (name) VALUES ('two');")
	// Model 仅提供表结构，不应把其主键值混入删除范围。
	seeds[0].Model = &seedOption{ID: 999}
	if err := schemamigrate.ApplySeeds(context.Background(), provider(db), files, seeds); err != nil {
		t.Fatal(err)
	}
	rows := seedRows(t, db)
	if len(rows) != 2 || rows[0].Name != "one" || rows[1].Name != "two" {
		t.Fatalf("empty Where must replace physical rows, including soft-deleted rows: %+v", rows)
	}
	if db.AllowGlobalUpdate {
		t.Fatal("seed deletion must not change the caller's global update protection")
	}
}

type seedCategory struct {
	ID   uint
	Name string
}

func TestSeedsKeepDifferentTableSessionsIsolated(t *testing.T) {
	db := seedDB(t)
	if err := db.AutoMigrate(&seedCategory{}); err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{
		"options.sql":    &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name, enabled) VALUES ('option', false);")},
		"categories.sql": &fstest.MapFile{Data: []byte("INSERT INTO seed_categories (name) VALUES ('category');")},
	}
	seeds := []schemamigrate.Seed{
		{Model: &seedOption{}, Where: map[string]any{"enabled": false}, File: "options.sql"},
		{Model: &seedCategory{}, File: "categories.sql"},
	}
	for range 2 {
		if err := schemamigrate.ApplySeeds(context.Background(), provider(db), files, seeds); err != nil {
			t.Fatal(err)
		}
	}
	var categories []seedCategory
	if err := db.Find(&categories).Error; err != nil {
		t.Fatal(err)
	}
	if rows := seedRows(t, db); len(rows) != 1 || rows[0].Name != "option" || len(categories) != 1 || categories[0].Name != "category" {
		t.Fatalf("table names and scope conditions must not leak between seeds: options=%+v categories=%+v", rows, categories)
	}
}

func TestSeedsRollbackDeletionAndStopAfterFailure(t *testing.T) {
	db := seedDB(t)
	old := seedOption{Name: "original", Enabled: false}
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	files := fstest.MapFS{
		"first.sql":  &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name, enabled) VALUES ('first', true);")},
		"failed.sql": &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name, enabled) VALUES ('replacement', false);\nINSERT INTO missing_table (name) VALUES ('failure');")},
		"last.sql":   &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name) VALUES ('last');")},
	}
	seeds := []schemamigrate.Seed{
		{Model: &seedOption{}, Where: map[string]any{"enabled": true}, File: "first.sql"},
		{Model: &seedOption{}, Where: map[string]any{"enabled": false}, File: "failed.sql"},
		{Model: &seedOption{}, Where: map[string]any{"name": "last"}, File: "last.sql"},
	}
	err := schemamigrate.ApplySeeds(context.Background(), provider(db), files, seeds)
	if err == nil || !strings.Contains(err.Error(), "failed.sql") || !strings.Contains(err.Error(), "line 2") {
		t.Fatalf("failure must identify its file and line: %v", err)
	}
	rows := seedRows(t, db)
	if len(rows) != 2 || rows[0].ID != old.ID || rows[0].Name != "original" || rows[1].Name != "first" {
		t.Fatalf("failed entry must restore deleted rows, retain earlier commits and stop: %+v", rows)
	}
}

func TestSeedsRejectInvalidFilesWithoutLosingData(t *testing.T) {
	for _, sql := range []string{
		"-- empty\n",
		"UPDATE seed_options SET name = 'bad';",
		"INSERT INTO seed_options (name)\nVALUES ('multiline');",
		"INSERT INTO seed_options (name) VALUES ('one'), ('two'), ('three');",
		"INSERT INTO seed_options (name, enabled) VALUES ('wrong scope', true);",
	} {
		t.Run(sql, func(t *testing.T) {
			db := seedDB(t)
			for range 2 {
				if err := db.Create(&seedOption{Name: "old"}).Error; err != nil {
					t.Fatal(err)
				}
			}
			seeds := []schemamigrate.Seed{{Model: &seedOption{}, Where: map[string]any{"enabled": false}, File: "options.sql"}}
			if err := schemamigrate.ApplySeeds(context.Background(), provider(db), seedSQL(sql), seeds); err == nil {
				t.Fatal("invalid seed must fail")
			}
			rows := seedRows(t, db)
			if len(rows) != 2 || rows[0].Name != "old" || rows[1].Name != "old" {
				t.Fatalf("invalid seeds must preserve existing data: %+v", rows)
			}
		})
	}
}

func TestSeedsInputErrorsAndContext(t *testing.T) {
	db := seedDB(t)
	files := seedSQL("INSERT INTO seed_options (name) VALUES ('new');")
	seeds := []schemamigrate.Seed{{Model: &seedOption{}, File: "options.sql"}}
	if err := schemamigrate.ApplySeeds(context.Background(), nil, nil, nil); err != nil {
		t.Fatalf("empty list must not acquire a database: %v", err)
	}
	for _, tc := range []struct {
		name   string
		ctx    context.Context
		master func(context.Context) *gorm.DB
		files  fs.FS
		seeds  []schemamigrate.Seed
	}{
		{name: "context", master: provider(db), files: files, seeds: seeds},
		{name: "provider", ctx: context.Background(), files: files, seeds: seeds},
		{name: "session", ctx: context.Background(), master: func(context.Context) *gorm.DB { return nil }, files: files, seeds: seeds},
		{name: "files", ctx: context.Background(), master: provider(db), seeds: seeds},
		{name: "model", ctx: context.Background(), master: provider(db), files: files, seeds: []schemamigrate.Seed{{File: "options.sql"}}},
		{name: "dry run", ctx: context.Background(), master: provider(db.Session(&gorm.Session{DryRun: true})), files: files, seeds: seeds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := schemamigrate.ApplySeeds(tc.ctx, tc.master, tc.files, tc.seeds); err == nil {
				t.Fatal("must reject invalid input")
			}
		})
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := schemamigrate.ApplySeeds(context.Background(), provider(tx), files, seeds); err == nil {
			t.Fatal("external transactions cannot guarantee independent commits")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	err := schemamigrate.ApplySeeds(context.Background(), provider(db), fstest.MapFS{}, seeds)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("file errors must preserve their cause: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := schemamigrate.ApplySeeds(ctx, provider(db), files, seeds); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause lost: %v", err)
	}
	if len(seedRows(t, db)) != 0 {
		t.Fatal("invalid inputs must not insert rows")
	}
}

func TestSeedDeadlineRollsBackReplacement(t *testing.T) {
	db := seedDB(t)
	old := seedOption{Name: "old", Enabled: false}
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	reachedPartialInsert := false
	if err := db.Callback().Raw().Before("gorm:raw").Register("seed:deadline", func(tx *gorm.DB) {
		if !strings.Contains(tx.Statement.SQL.String(), "'two'") {
			return
		}
		// 第二条 INSERT 前检查事务内视图，证明旧数据已删除且第一条已插入。
		var rows []seedOption
		if err := tx.Session(&gorm.Session{NewDB: true}).
			Where("enabled = ?", false).
			Find(&rows).Error; err != nil {
			tx.AddError(err)
			return
		}
		reachedPartialInsert = len(rows) == 1 && rows[0].Name == "one"
		<-tx.Statement.Context.Done()
		tx.AddError(tx.Statement.Context.Err())
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	files := fstest.MapFS{
		"first.sql":   &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name, enabled) VALUES ('first', true);")},
		"timeout.sql": &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name, enabled) VALUES ('one', false);\nINSERT INTO seed_options (name, enabled) VALUES ('two', false);")},
		"last.sql":    &fstest.MapFile{Data: []byte("INSERT INTO seed_options (name) VALUES ('last');")},
	}
	seeds := []schemamigrate.Seed{
		{Model: &seedOption{}, Where: map[string]any{"enabled": true}, File: "first.sql"},
		{Model: &seedOption{}, Where: map[string]any{"enabled": false}, File: "timeout.sql"},
		{Model: &seedOption{}, Where: map[string]any{"name": "last"}, File: "last.sql"},
	}
	err := schemamigrate.ApplySeeds(ctx, provider(db), files, seeds)
	if !reachedPartialInsert || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "timeout.sql") {
		t.Fatalf("must time out after deletion and a successful insert: reached=%v err=%v", reachedPartialInsert, err)
	}
	rows := seedRows(t, db)
	if len(rows) != 2 || rows[0].ID != old.ID || rows[0].Name != "old" || rows[1].Name != "first" {
		t.Fatalf("timeout must restore deleted data, roll back partial inserts, retain prior commits and stop: %+v", rows)
	}
}

func TestSeedDeleteFailurePreservesScope(t *testing.T) {
	db := seedDB(t)
	old := seedOption{Name: "old"}
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_seed_delete BEFORE DELETE ON seed_options
BEGIN SELECT RAISE(ABORT, 'seed delete rejected'); END`).Error; err != nil {
		t.Fatal(err)
	}
	files := seedSQL("INSERT INTO seed_options (name) VALUES ('one');\nINSERT INTO seed_options (name) VALUES ('two');")
	err := schemamigrate.ApplySeeds(context.Background(), provider(db), files, []schemamigrate.Seed{{Model: &seedOption{}, File: "options.sql"}})
	if err == nil || !strings.Contains(err.Error(), "seed delete rejected") || !strings.Contains(err.Error(), "options.sql") {
		t.Fatalf("delete failure must identify its file and cause: %v", err)
	}
	if rows := seedRows(t, db); len(rows) != 1 || rows[0].ID != old.ID || rows[0].Name != "old" {
		t.Fatalf("delete failure must preserve old rows and prevent insertion: %+v", rows)
	}
}

type seedDeferredItem struct {
	ID       uint
	ParentID uint
}

func TestSeedCommitFailureRestoresDeletedRows(t *testing.T) {
	db := seedDB(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	// SQLite 的外键开关属于连接，固定单连接使延迟约束只在 COMMIT 时失败。
	pool.SetMaxOpenConns(1)
	pool.SetMaxIdleConns(1)
	for _, sql := range []string{
		"PRAGMA foreign_keys = ON",
		"CREATE TABLE seed_parents (id INTEGER PRIMARY KEY)",
		"CREATE TABLE seed_deferred_items (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES seed_parents(id) DEFERRABLE INITIALLY DEFERRED)",
		"INSERT INTO seed_parents (id) VALUES (1)",
		"INSERT INTO seed_deferred_items (id, parent_id) VALUES (1, 1)",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
	}
	files := seedSQL("INSERT INTO seed_deferred_items (id, parent_id) VALUES (2, 999);\nINSERT INTO seed_deferred_items (id, parent_id) VALUES (3, 999);")
	err = schemamigrate.ApplySeeds(context.Background(), provider(db), files, []schemamigrate.Seed{{Model: &seedDeferredItem{}, File: "options.sql"}})
	if err == nil || !strings.Contains(err.Error(), "FOREIGN KEY constraint failed") || !strings.Contains(err.Error(), "options.sql") {
		t.Fatalf("commit failure must identify its file and cause: %v", err)
	}
	var rows []seedDeferredItem
	if err := db.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != 1 || rows[0].ParentID != 1 {
		t.Fatalf("commit failure must restore deletion and roll back all inserts: %+v", rows)
	}
}
