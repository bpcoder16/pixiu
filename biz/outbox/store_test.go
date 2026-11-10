package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"github.com/bpcoder16/pixiu/logit"
	driver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 只在随机命名的隔离数据库中迁移和写入，不修改 DSN 原有数据库。
func liveStore(t *testing.T, backend Backend, mysqlLocation ...*time.Location) (*Store, *gorm.DB) {
	t.Helper()
	key := "PIXIU_OUTBOX_MYSQL_DSN"
	if backend == PostgreSQL {
		key = "PIXIU_OUTBOX_POSTGRESQL_DSN"
	}
	dsn := os.Getenv(key)
	if dsn == "" {
		t.Skip("未设置 " + key + "；真实数据库验收未运行")
	}
	name := fmt.Sprintf("pixiu_outbox_test_%d", time.Now().UnixNano())
	config := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
	var admin, db *gorm.DB
	var err error
	created := false
	if backend == MySQL {
		cfg, parseErr := driver.ParseDSN(dsn)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		cfg.DBName = ""
		cfg.ParseTime = true
		cfg.Loc, err = time.LoadLocation("Asia/Shanghai")
		if err != nil {
			t.Fatal(err)
		}
		if len(mysqlLocation) > 0 {
			cfg.Loc = mysqlLocation[0]
		}
		if cfg.Params == nil {
			cfg.Params = make(map[string]string)
		}
		// TIMESTAMP 由会话转换，驱动的解析时区必须与每条连接保持一致。
		cfg.Params["time_zone"] = "'" + time.Now().In(cfg.Loc).Format("-07:00") + "'"
		admin, err = gorm.Open(mysql.Open(cfg.FormatDSN()), config)
		if err == nil {
			err = admin.Exec("CREATE DATABASE " + name + " CHARACTER SET utf8mb4").Error
			created = err == nil
		}
		if err == nil {
			cfg.DBName = name
			db, err = gorm.Open(mysql.Open(cfg.FormatDSN()), config)
		}
	} else {
		cfg, parseErr := pgx.ParseConfig(dsn)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		cfg.Database = "postgres"
		pool := stdlib.OpenDB(*cfg)
		admin, err = gorm.Open(postgres.New(postgres.Config{Conn: pool}), config)
		if err == nil {
			err = admin.Exec("CREATE DATABASE " + name).Error
			created = err == nil
		}
		if err == nil {
			cfg.Database = name
			pool = stdlib.OpenDB(*cfg)
			db, err = gorm.Open(postgres.New(postgres.Config{Conn: pool}), config)
		}
	}
	if admin != nil && admin.Statement != nil {
		t.Cleanup(func() {
			if db != nil {
				pool, _ := db.DB()
				_ = pool.Close()
			}
			if created {
				if err := admin.Exec("DROP DATABASE IF EXISTS " + name).Error; err != nil {
					t.Error(err)
				}
			}
			pool, _ := admin.DB()
			_ = pool.Close()
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	_, err = schemamigrate.Apply(context.Background(), db.WithContext, []schemamigrate.Table{{Model: &Record{}}})
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewStore(StoreConfig{
		Backend:  backend,
		MasterDB: db.WithContext,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, db
}

func eachDatabase(t *testing.T, test func(*testing.T, *Store, *gorm.DB)) {
	t.Helper()
	for _, backend := range []Backend{MySQL, PostgreSQL} {
		t.Run(string(backend), func(t *testing.T) {
			store, db := liveStore(t, backend)
			test(t, store, db)
		})
	}
}

func task() Task {
	return Task{
		Kind:    "test.publish",
		Version: 1,
		Payload: []byte(`{"amount":1}`),
	}
}

func enqueueTask(t *testing.T, s *Store, db *gorm.DB, value Task) int64 {
	t.Helper()
	var id int64
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		id, err = s.EnqueueTx(tx, value)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if id <= 0 {
		t.Fatalf("登记必须返回数据库生成的正 ID: %d", id)
	}
	return id
}

func enqueue(t *testing.T, s *Store, db *gorm.DB) int64 {
	t.Helper()
	return enqueueTask(t, s, db, task())
}

func TestDatabaseAtomicityAndSnapshot(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := logit.WithContextLogID(context.Background())
		if _, err := s.EnqueueTx(db, task()); !errors.Is(err, ErrNotTransaction) {
			t.Fatalf("普通会话必须拒绝: %v", err)
		}
		rollback := errors.New("业务回滚")
		var rolledBack, committed int64
		err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var err error
			rolledBack, err = s.EnqueueTx(tx, task())
			if err != nil {
				return err
			}
			return rollback
		})
		if !errors.Is(err, rollback) || rolledBack <= 0 {
			t.Fatalf("回滚前应取得自增 ID: %d %v", rolledBack, err)
		}
		if _, err := s.Get(ctx, rolledBack); !errors.Is(err, ErrNotFound) {
			t.Fatalf("回滚后不应留下任务: %v", err)
		}
		value := task()
		err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			// 历史 Select/Omit/Table 不得污染 Outbox，也不得从事务中逃逸。
			chained := tx.Table("not_a_real_table").Select("id").Omit("payload")
			var err error
			committed, err = s.EnqueueTx(chained, value)
			return err
		})
		if err != nil || committed <= rolledBack {
			t.Fatalf("自增 ID 应允许回滚留下间隙: %d %d %v", rolledBack, committed, err)
		}
		value.Payload[0] = 'X'
		record, err := s.Get(ctx, committed)
		logID, _ := logit.LogIDFromContext(ctx)
		if err != nil || record.ID != committed || record.Status != Pending || string(record.Payload) != string(task().Payload) || record.LogID != logID {
			t.Fatalf("入表快照错误: %+v %v", record, err)
		}
		if time.Since(record.CreatedAt.Time).Abs() > 10*time.Second || record.CreatedAt.Location() != time.UTC {
			t.Fatalf("非 UTC 驱动不应使时间偏移: %v", record.CreatedAt)
		}
		// 不再使用业务唯一键；重复登记同一内容也生成独立任务。
		next := enqueue(t, s, db)
		if next <= committed {
			t.Fatalf("重复登记必须生成新 ID: %d <= %d", next, committed)
		}
	})
}

func TestConfigurationRejectsInvalidInputs(t *testing.T) {
	provider := func(context.Context) *gorm.DB { panic("构造器不应取数据库") }
	s, err := NewStore(StoreConfig{
		Backend:  MySQL,
		MasterDB: provider,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.EnqueueTx(nil, task()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("nil 事务必须拒绝")
	}
	for _, handlers := range []map[HandlerKey]Handler{
		nil,
		{{Kind: "test", Version: 0}: func(context.Context, Delivery) error { return nil }},
		{{Kind: "test", Version: 1}: nil},
	} {
		if _, err := NewWorker(s, handlers); err == nil {
			t.Fatal("非法 Handler 应拒绝")
		}
	}
}

func TestMasterUsesCurrentContext(t *testing.T) {
	// 只构造合法 GORM 会话，不查询数据库；覆盖缓存基础会话的 Context 重绑定。
	db, err := gorm.Open(mysql.New(mysql.Config{SkipInitializeWithVersion: true}), &gorm.Config{
		DisableAutomaticPing: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	oldContext, cancel := context.WithCancel(context.Background())
	cancel()
	cached := db.WithContext(oldContext)
	store, err := NewStore(StoreConfig{
		Backend: MySQL,
		MasterDB: func(context.Context) *gorm.DB {
			return cached
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	type requestKey struct{}
	ctx := context.WithValue(context.Background(), requestKey{}, "current")
	session, err := store.master(ctx)
	if err != nil {
		t.Fatalf("旧会话的 Context 不应阻止绑定本次 Context: %v", err)
	}
	if session.Statement.Context.Err() != nil || session.Statement.Context.Value(requestKey{}) != "current" {
		t.Fatal("本次操作未使用新的 Context")
	}
	if !errors.Is(cached.Statement.Context.Err(), context.Canceled) {
		t.Fatal("不应修改缓存基础会话的 Context")
	}
}

func TestEnqueueCancelledContextRollsBack(t *testing.T) {
	eachDatabase(t, func(t *testing.T, store *Store, db *gorm.DB) {
		err := db.Transaction(func(tx *gorm.DB) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err := store.EnqueueTx(tx.WithContext(ctx), task())
			return err
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("数据库操作必须传播取消: %v", err)
		}
		var count int64
		if err := db.Model(&Record{}).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("取消的事务不得留下任务: %d %v", count, err)
		}
	})
}

func TestRegisteredSchemaAndRepeatedMigration(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := context.Background()
		id := enqueue(t, s, db)
		if _, err := schemamigrate.Apply(ctx, db.WithContext, []schemamigrate.Table{
			{Model: &Record{}, Comment: "事务发件箱"},
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, id); err != nil {
			t.Fatalf("重复迁移不得破坏任务: %v", err)
		}
		columns, err := db.Migrator().ColumnTypes(&Record{})
		if err != nil {
			t.Fatal(err)
		}
		if len(columns) != 12 {
			t.Fatalf("列数错误: %d", len(columns))
		}
		for _, column := range columns {
			name := column.Name()
			nullable, ok := column.Nullable()
			wantNullable := slices.Contains([]string{"expires_at", "finished_at"}, name)
			if !ok || nullable != wantNullable {
				t.Errorf("%s NULL 属性错误: %v/%v", name, nullable, ok)
			}
			if name == "id" {
				wantType := "bigint"
				if s.cfg.Backend == PostgreSQL {
					wantType = "int8"
				}
				if column.DatabaseTypeName() != wantType {
					t.Errorf("ID 必须是 64 位整数: %s", column.DatabaseTypeName())
				}
				if auto, ok := column.AutoIncrement(); !ok || !auto {
					t.Errorf("ID 必须由数据库自增: %v/%v", auto, ok)
				}
			}
			if name == "payload" {
				wantType := "bytea"
				if s.cfg.Backend == MySQL {
					wantType = "longblob"
				}
				if column.DatabaseTypeName() != wantType {
					t.Errorf("%s 类型错误: %s", name, column.DatabaseTypeName())
				}
			}
			if slices.Contains([]string{"created_at", "updated_at", "expires_at", "finished_at"}, name) {
				wantType := "timestamptz"
				if s.cfg.Backend == MySQL {
					wantType = "timestamp"
				}
				if column.DatabaseTypeName() != wantType {
					t.Errorf("%s 时间类型错误: %s, want %s", name, column.DatabaseTypeName(), wantType)
				}
			}
		}
		var precision []struct {
			DatetimePrecision int
		}
		precisionQuery := "SELECT datetime_precision FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'pixiu_outbox' AND column_name IN ('created_at','updated_at','expires_at','finished_at')"
		if s.cfg.Backend == PostgreSQL {
			precisionQuery = "SELECT datetime_precision FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'pixiu_outbox' AND column_name IN ('created_at','updated_at','expires_at','finished_at')"
		}
		if err := db.Raw(precisionQuery).Scan(&precision).Error; err != nil {
			t.Fatal(err)
		}
		if len(precision) != 4 {
			t.Fatalf("时间列数错误: %+v", precision)
		}
		for _, column := range precision {
			if column.DatetimePrecision != 6 {
				t.Errorf("时间精度错误: %d", column.DatetimePrecision)
			}
		}
		var collations []struct {
			ColumnName    string
			CollationName string
		}
		query := "SELECT column_name, collation_name FROM information_schema.columns WHERE table_schema = DATABASE() AND table_name = 'pixiu_outbox' AND column_name IN ('kind','status')"
		wantCollation := "ascii_bin"
		if s.cfg.Backend == PostgreSQL {
			query = "SELECT column_name, collation_name FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'pixiu_outbox' AND column_name IN ('kind','status')"
			wantCollation = "C"
		}
		if err := db.Raw(query).Scan(&collations).Error; err != nil {
			t.Fatal(err)
		}
		if len(collations) != 2 {
			t.Fatalf("缺少标识列: %+v", collations)
		}
		for _, column := range collations {
			if column.CollationName != wantCollation {
				t.Errorf("%s 排序规则错误: %s", column.ColumnName, column.CollationName)
			}
		}
		if next := enqueue(t, s, db); next <= id {
			t.Fatalf("重复迁移不得重置序列: %d <= %d", next, id)
		}
		indexes, err := db.Migrator().GetIndexes(&Record{})
		if err != nil {
			t.Fatal(err)
		}
		for name, columns := range map[string][]string{
			"pixiu_outbox_ready_idx":    {"status", "id"},
			"pixiu_outbox_finished_idx": {"status", "finished_at", "id"},
		} {
			found := slices.ContainsFunc(indexes, func(index gorm.Index) bool {
				return index.Name() == name && slices.Equal(index.Columns(), columns)
			})
			if s.cfg.Backend == PostgreSQL {
				// 当前 GORM PG GetIndexes 按表列顺序返回；以系统目录的真实索引定义验收。
				var definition string
				if err := db.Raw("SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND tablename = 'pixiu_outbox' AND indexname = ?", name).Scan(&definition).Error; err != nil {
					t.Fatal(err)
				}
				found = strings.Contains(definition, "("+strings.Join(columns, ", ")+")")
			}
			if !found {
				t.Errorf("索引缺失或列顺序错误: %s", name)
			}
		}
	})
}

func TestBusinessAndOutboxRollbackTogether(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		type businessRecord struct {
			ID int64 `gorm:"primaryKey"`
		}
		if err := db.AutoMigrate(&businessRecord{}); err != nil {
			t.Fatal(err)
		}
		var rolledBack, outer int64
		rollback := errors.New("回滚业务与任务")
		err := db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&businessRecord{ID: 1}).Error; err != nil {
				return err
			}
			var err error
			rolledBack, err = s.EnqueueTx(tx, task())
			if err != nil {
				return err
			}
			return rollback
		})
		if !errors.Is(err, rollback) {
			t.Fatal(err)
		}
		var count int64
		if err := db.Model(&businessRecord{}).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("业务未回滚: %d %v", count, err)
		}
		if _, err := s.Get(context.Background(), rolledBack); !errors.Is(err, ErrNotFound) {
			t.Fatalf("任务未回滚: %v", err)
		}
		err = db.Transaction(func(tx *gorm.DB) error {
			if err := tx.Create(&businessRecord{ID: 2}).Error; err != nil {
				return err
			}
			err := tx.Transaction(func(inner *gorm.DB) error {
				var err error
				rolledBack, err = s.EnqueueTx(inner, task())
				if err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				return err
			}
			outer, err = s.EnqueueTx(tx, task())
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Model(&businessRecord{}).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("外层业务未提交: %d %v", count, err)
		}
		if _, err := s.Get(context.Background(), rolledBack); !errors.Is(err, ErrNotFound) {
			t.Fatalf("保存点回滚未移除任务: %v", err)
		}
		if _, err := s.Get(context.Background(), outer); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEnqueueRejectsInvalidTransactionAndTask(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		err := db.Transaction(func(tx *gorm.DB) error {
			for _, value := range []Task{
				{Kind: "BAD", Version: 1},
				{Kind: strings.Repeat("a", 65), Version: 1},
				{Kind: "test.publish", Version: 0},
				{Kind: "test.publish", Version: 1, Payload: make([]byte, (1<<20)+1)},
			} {
				if _, err := s.EnqueueTx(tx, value); !errors.Is(err, ErrInvalidArgument) {
					t.Errorf("非法任务未在入库前拒绝: %v", err)
				}
			}
			if _, err := s.EnqueueTx(tx.Session(&gorm.Session{DryRun: true}), task()); !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("DryRun 必须拒绝: %v", err)
			}
			wrongConfig := s.cfg
			wrongConfig.Backend = MySQL
			if s.cfg.Backend == MySQL {
				wrongConfig.Backend = PostgreSQL
			}
			wrong, _ := NewStore(wrongConfig)
			if _, err := wrong.EnqueueTx(tx, task()); !errors.Is(err, ErrBackendMismatch) {
				t.Errorf("方言不匹配必须拒绝: %v", err)
			}
			_, err := s.EnqueueTx(tx, task())
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		var count int64
		if err := db.Model(&Record{}).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("非法任务产生副作用: %d %v", count, err)
		}
	})
}
