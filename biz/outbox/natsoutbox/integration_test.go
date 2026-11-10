package natsoutbox

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 每个端到端用例创建独立数据库，测试结束只删除自己创建的库。
func integrationDB(t *testing.T, backend outbox.Backend, dsn string) *gorm.DB {
	t.Helper()
	name := fmt.Sprintf("pixiu_outbox_e2e_%d", time.Now().UnixNano())
	opener := func(database string) (*gorm.DB, error) {
		cfg := &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)}
		if backend == outbox.MySQL {
			parsed, err := mysqldriver.ParseDSN(dsn)
			if err != nil {
				return nil, err
			}
			parsed.DBName = database
			parsed.ParseTime = true
			return gorm.Open(mysql.Open(parsed.FormatDSN()), cfg)
		}
		parsed, err := pgx.ParseConfig(dsn)
		if err != nil {
			return nil, err
		}
		if database == "" {
			database = "postgres"
		}
		parsed.Database = database
		return gorm.Open(postgres.New(postgres.Config{Conn: stdlib.OpenDB(*parsed)}), cfg)
	}
	admin, err := opener("")
	if err != nil {
		t.Fatal(err)
	}
	adminPool, _ := admin.DB()
	t.Cleanup(func() { _ = adminPool.Close() })
	if err := admin.Exec("CREATE DATABASE " + name).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := admin.Exec("DROP DATABASE " + name).Error; err != nil {
			t.Error(err)
		}
	})
	db, err := opener(name)
	if err != nil {
		t.Fatal(err)
	}
	pool, _ := db.DB()
	t.Cleanup(func() { _ = pool.Close() })
	if _, err := schemamigrate.Apply(context.Background(), db.WithContext, []schemamigrate.Table{{Model: &outbox.Record{}}}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestDatabaseToJetStreamRecovery(t *testing.T) {
	for backend, key := range map[outbox.Backend]string{
		outbox.MySQL:      "PIXIU_OUTBOX_MYSQL_DSN",
		outbox.PostgreSQL: "PIXIU_OUTBOX_POSTGRESQL_DSN",
	} {
		t.Run(string(backend), func(t *testing.T) {
			dsn := os.Getenv(key)
			if dsn == "" {
				t.Skip("未设置 " + key + "；端到端数据库测试未运行")
			}
			if !isolatedNamedClients(t) {
				return
			}
			db := integrationDB(t, backend, dsn)
			s, err := server.NewServer(&server.Options{
				Host:      "127.0.0.1",
				Port:      -1,
				JetStream: true,
				StoreDir:  t.TempDir(),
			})
			if err != nil {
				t.Fatal(err)
			}
			s.Start()
			t.Cleanup(func() {
				s.Shutdown()
				s.WaitForShutdown()
			})
			if !s.ReadyForConnections(5 * time.Second) {
				t.Fatal("NATS 未就绪")
			}
			client, err := natsx.NewNamed(natsx.Config{
				Name: "events",
				URLs: []string{s.ClientURL()},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = natsx.CloseAll() })
			ctx := context.Background()
			stream, err := client.JetStream().CreateStream(ctx, jetstream.StreamConfig{
				Name:     "EVENTS",
				Subjects: []string{"events.created"},
				Storage:  jetstream.FileStorage,
			})
			if err != nil {
				t.Fatal(err)
			}
			store, err := outbox.NewStore(outbox.StoreConfig{
				Backend:  backend,
				MasterDB: db.WithContext,
			})
			if err != nil {
				t.Fatal(err)
			}
			handler := NewJetStreamHandler("events")
			var calls atomic.Int64
			worker, err := outbox.NewWorker(store, map[outbox.HandlerKey]outbox.Handler{
				{Kind: JetStreamKind, Version: JetStreamVersion}: func(ctx context.Context, d outbox.Delivery) error {
					err := handler(ctx, d)
					if err == nil && calls.Add(1) == 1 {
						// Broker 已确认但调用结果丢失，补偿必须保持事件身份。
						return errors.New("模拟确认结果丢失")
					}
					return err
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			message := JetStreamMessage{
				Client:  "events",
				Subject: "events.created",
				Data:    []byte("original"),
			}
			rollback := errors.New("业务回滚")
			var id int64
			err = db.Transaction(func(tx *gorm.DB) error {
				var err error
				id, err = EnqueueJetStreamTx(store, tx, message)
				if err != nil {
					return err
				}
				return rollback
			})
			if !errors.Is(err, rollback) {
				t.Fatal(err)
			}
			if _, err := store.Get(ctx, id); !errors.Is(err, outbox.ErrNotFound) {
				t.Fatal(err)
			}
			if calls.Load() != 0 {
				t.Fatal("未提交任务不能发送")
			}
			if err := db.Transaction(func(tx *gorm.DB) error {
				var err error
				id, err = EnqueueJetStreamTx(store, tx, message)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			message.Data[0] = 'X'
			if err := worker.Send(ctx, id); err == nil {
				t.Fatal("应报告模拟失败")
			}
			if err := worker.RetryPending(ctx, 10); err != nil {
				t.Fatal(err)
			}
			record, err := store.Get(ctx, id)
			if err != nil || record.Status != outbox.Succeeded || record.Attempts != 2 {
				t.Fatalf("补偿状态错误: %+v %v", record, err)
			}

			info, err := stream.Info(ctx)
			if err != nil || info.State.Msgs != 1 || calls.Load() != 2 {
				t.Fatalf("补偿未去重: %+v %v, calls=%d", info, err, calls.Load())
			}
			stored, err := stream.GetMsg(ctx, 1)
			if err != nil || string(stored.Data) != "original" || stored.Header.Get("Nats-Msg-Id") != strconv.FormatInt(id, 10) {
				t.Fatalf("入表快照不稳定: %+v %v", stored, err)
			}
		})
	}
}
