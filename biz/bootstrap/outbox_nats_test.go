//go:build !pixiu_bootstrap_no_nats && !pixiu_bootstrap_no_mysql

package bootstrap_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 只模拟连接与 Ping，不模拟发布；避免内嵌 NATS 服务端后台采样与 time.Local 初始化竞争。
func newOutboxNATSServer(t *testing.T) string {
	t.Helper()
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
			t.Error("NATS 装配测试连接未关闭")
		}
	})
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
		if _, err := io.WriteString(conn, "INFO {\"server_id\":\"outbox-test\",\"proto\":1,\"headers\":true}\r\n"); err != nil {
			return
		}
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			if scanner.Text() == "PING" {
				if _, err := io.WriteString(conn, "PONG\r\n"); err != nil {
					return
				}
			}
		}
	}()
	return "nats://" + listener.Addr().String()
}

func TestOutboxJetStreamRegistration(t *testing.T) {
	isolatedLog(t, func() {
		requireMySQLPanic(t, "at least one NATS client", func() {
			bootstrap.MustRegisterJetStreamOutbox(outbox.MySQL, "orders")
		})
		for _, name := range []string{"", " events", "events ", "a.b", "库"} {
			requireMySQLPanic(t, "invalid Outbox NATS name", func() {
				bootstrap.MustRegisterJetStreamOutbox(outbox.MySQL, "orders", name)
			})
		}
		bootstrap.MustRegisterJetStreamOutbox(outbox.MySQL, "orders", "events", "audit")
		if recoverValue(func() { outbox.DefaultStore() }) == nil || recoverValue(func() { outbox.DefaultWorker() }) == nil {
			t.Fatal("快捷入口不应在声明时构造默认实例")
		}
		requireMySQLPanic(t, "Outbox is already registered", func() {
			bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", outboxHandlers)
		})
		requireMySQLPanic(t, "Outbox is already registered", func() {
			bootstrap.MustRegisterJetStreamOutbox(outbox.MySQL, "other", "events")
		})
	})
}

func TestOutboxJetStreamBuildsAfterNATS(t *testing.T) {
	for _, missingClient := range []bool{false, true} {
		t.Run(fmt.Sprintf("missingClient=%v", missingClient), func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				mysql := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "orders", mysqlConfigYAML(mysql.port))
				names := []string{"events", "audit"}
				bootstrap.MustRegisterJetStreamOutbox(outbox.MySQL, "orders", names...)
				// 声明必须保存名称快照，依赖声明可以晚于 Outbox。
				names[0] = "changed"
				bootstrap.MustRegisterMySQL("orders", false)
				for _, name := range []string{"events", "audit"} {
					if missingClient && name == "audit" {
						continue
					}
					url := newOutboxNATSServer(t)
					writeNATSConfig(t, name, fmt.Sprintf("urls: [%q]\n", url))
					bootstrap.MustRegisterNATS(name, false)
				}
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				if missingClient {
					if value == nil || !strings.Contains(fmt.Sprint(value), `"audit"`) {
						t.Fatalf("缺少任一命名客户端应在装配时失败: %v", value)
					}
					if recoverValue(func() { outbox.DefaultStore() }) == nil {
						t.Fatal("Handler 构造失败不应发布默认 Store")
					}
				} else {
					if value != nil || outbox.DefaultWorker() == nil {
						t.Fatalf("应在 NATS 就绪后装配默认 Worker: %v", value)
					}
					// 只校验参数，不查询尚未迁移的表；构造器不能自动发起发送。
					if err := outbox.Send(context.Background(), 0); err != outbox.ErrInvalidArgument {
						t.Fatal(err)
					}
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				mysql.requireClosed(t)
			})
		})
	}
}
