package outbox

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	driver "github.com/go-sql-driver/mysql"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 无数据库时仍检查真实迁移生成的 DDL，防止默认值和可空声明退回旧式 TIMESTAMP 行为。
func TestRecordTimestampMigrationDDL(t *testing.T) {
	for _, tc := range []struct {
		name      string
		dialector gorm.Dialector
		columns   []string
	}{
		{
			name:      "mysql",
			dialector: mysql.New(mysql.Config{SkipInitializeWithVersion: true}),
			columns: []string{
				"`created_at` timestamp(6) not null default current_timestamp(6)",
				"`updated_at` timestamp(6) not null default current_timestamp(6)",
				"`expires_at` timestamp(6) null default null",
				"`finished_at` timestamp(6) null default null",
			},
		},
		{
			name:      "postgresql",
			dialector: postgres.New(postgres.Config{}),
			columns: []string{
				`"created_at" timestamptz(6) not null default current_timestamp(6)`,
				`"updated_at" timestamptz(6) not null default current_timestamp(6)`,
				`"expires_at" timestamptz(6) default null`,
				`"finished_at" timestamptz(6) default null`,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			db, err := gorm.Open(tc.dialector, &gorm.Config{
				DisableAutomaticPing: true,
				DryRun:               true,
				Logger: logger.New(log.New(&output, "", 0), logger.Config{
					LogLevel: logger.Info,
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			pool, err := db.DB()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = pool.Close() })
			if err := db.Migrator().CreateTable(&Record{}); err != nil {
				t.Fatal(err)
			}
			ddl := strings.ToLower(output.String())
			for _, column := range tc.columns {
				if !strings.Contains(ddl, column) {
					t.Errorf("迁移缺少时间列定义 %q: %s", column, ddl)
				}
			}
			if strings.Contains(ddl, "on update") {
				t.Fatalf("时间更新必须由 Store 控制，不能隐式 ON UPDATE: %s", ddl)
			}
		})
	}
}

func TestMySQLTimestampDefaultsAndRepeatedMigration(t *testing.T) {
	requireBackend(t, MySQL)
	const sqlMode = "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION"
	dsn := os.Getenv("PIXIU_OUTBOX_MYSQL_DSN")
	if dsn == "" {
		t.Skip("未设置 PIXIU_OUTBOX_MYSQL_DSN；真实数据库验收未运行")
	}
	for _, mode := range []string{"0", "1"} {
		t.Run("explicit_defaults_for_timestamp="+mode, func(t *testing.T) {
			cfg, err := driver.ParseDSN(dsn)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Params == nil {
				cfg.Params = make(map[string]string)
			}
			// 仅临时测试连接使用这组参数；liveStore 创建隔离库，不修改实例或业务连接。
			cfg.Params["explicit_defaults_for_timestamp"] = mode
			cfg.Params["sql_mode"] = "'" + sqlMode + "'"
			t.Setenv("PIXIU_OUTBOX_MYSQL_DSN", cfg.FormatDSN())
			store, db := liveStore(t, MySQL, time.UTC)
			ctx := context.Background()
			id := enqueue(t, store, db)
			record, err := store.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if record.ExpiresAt != nil || record.FinishedAt != nil {
				t.Fatalf("未设置的期限和完成时间必须为 NULL: %+v", record)
			}
			if time.Since(record.CreatedAt.Time).Abs() > 10*time.Second || !record.CreatedAt.Equal(record.UpdatedAt.Time) {
				t.Fatalf("创建和更新时间必须使用数据库时钟: %+v", record)
			}

			// 使用过去的明确时刻，避免同一秒内的隐式自动更新被误判为未变化。
			baseline := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
			if err := db.Model(&Record{}).Where("id = ?", id).Updates(map[string]any{
				"created_at": baseline,
				"updated_at": baseline,
			}).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Model(&Record{}).Where("id = ?", id).Update("last_error", "保留时间列").Error; err != nil {
				t.Fatal(err)
			}
			if _, err := schemamigrate.Apply(ctx, db.WithContext, []schemamigrate.Table{{Model: &Record{}}}); err != nil {
				t.Fatalf("重复迁移失败: %v", err)
			}
			record, err = store.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if !record.CreatedAt.Equal(baseline) || !record.UpdatedAt.Equal(baseline) || record.ExpiresAt != nil || record.FinishedAt != nil {
				t.Fatalf("无关字段更新和重复迁移不得改变时间列: %+v", record)
			}
			worker := testWorker(t, store, func(context.Context, Delivery) error {
				return nil
			})
			if err := worker.Send(ctx, id); err != nil {
				t.Fatal(err)
			}
			record, err = store.Get(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if record.Status != Succeeded || !record.CreatedAt.Equal(baseline) || !record.UpdatedAt.After(baseline) || record.FinishedAt == nil || record.ExpiresAt != nil {
				t.Fatalf("发送成功应显式回写更新和完成时间，保持创建时间和空期限: %+v", record)
			}
			var actual struct {
				ExplicitDefaults string
				SQLMode          string
			}
			if err := db.Raw("SELECT @@SESSION.explicit_defaults_for_timestamp AS explicit_defaults, @@SESSION.sql_mode AS sql_mode").Scan(&actual).Error; err != nil {
				t.Fatal(err)
			}
			if actual.ExplicitDefaults != mode || actual.SQLMode != sqlMode {
				t.Fatalf("迁移和发送不得修改会话参数: got %+v, want %s / %s", actual, mode, sqlMode)
			}
		})
	}
}
