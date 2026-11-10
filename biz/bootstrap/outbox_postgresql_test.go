//go:build !pixiu_bootstrap_no_postgresql

package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/postgresqlx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/jackc/pgx/v5/pgproto3"
	"gorm.io/gorm"
)

func TestPostgreSQLOutboxUsesNamedMaster(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		done := make(chan struct{})
		stop := make(chan struct{})
		t.Cleanup(func() {
			close(stop)
			_ = listener.Close()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Error("PostgreSQL 装配测试连接未关闭")
			}
		})
		// 复用真实 pgx 驱动，只模拟握手和 Ping；装配不需要已迁移的任务表。
		go func() {
			defer close(done)
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(deadline)
			go func() {
				select {
				case <-stop:
					_ = conn.Close()
				case <-done:
				}
			}()
			server := pgproto3.NewBackend(conn, conn)
			if _, err := server.ReceiveStartupMessage(); err != nil {
				return
			}
			server.Send(&pgproto3.AuthenticationOk{})
			server.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if server.Flush() != nil {
				return
			}
			for {
				message, err := server.Receive()
				if err != nil {
					return
				}
				if _, ok := message.(*pgproto3.Query); !ok {
					return
				}
				server.Send(&pgproto3.EmptyQueryResponse{})
				server.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
				if server.Flush() != nil {
					return
				}
			}
		}()
		writePostgreSQLConfig(t, "orders", fmt.Sprintf("database: test\nusername: test\nsslMode: disable\nmaster:\n  host: 127.0.0.1\n  port: %d\n", listener.Addr().(*net.TCPAddr).Port))
		called := false
		bootstrap.MustRegisterOutbox(outbox.PostgreSQL, "orders", func() map[outbox.HandlerKey]outbox.Handler {
			called = true
			_ = postgresqlx.Named("orders")
			return map[outbox.HandlerKey]outbox.Handler{
				{Kind: "test.publish", Version: 1}: func(context.Context, outbox.Delivery) error { return nil },
			}
		})
		bootstrap.MustRegisterPostgreSQL("orders", false)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		if !called || outbox.DefaultWorker() == nil {
			t.Fatal("应在 PostgreSQL 就绪后装配默认 Outbox")
		}
		want := errors.New("PostgreSQL 主库查询错误")
		db := postgresqlx.Named("orders").MasterDB(context.Background())
		if err := db.Callback().Query().Before("gorm:query").Register("outbox_test:postgresql", func(tx *gorm.DB) {
			tx.AddError(want)
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := outbox.Get(context.Background(), 1); !errors.Is(err, want) {
			t.Fatalf("默认 Store 的方言或主库绑定错误: %v", err)
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
	})
}
