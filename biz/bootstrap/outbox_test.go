//go:build !pixiu_bootstrap_no_mysql

package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"gorm.io/gorm"
)

func outboxHandlers() map[outbox.HandlerKey]outbox.Handler {
	return map[outbox.HandlerKey]outbox.Handler{
		{Kind: "test.publish", Version: 1}: func(context.Context, outbox.Delivery) error { return nil },
		{Kind: "test.audit", Version: 2}:   func(context.Context, outbox.Delivery) error { return nil },
	}
}

func TestOutboxRegistration(t *testing.T) {
	isolatedLog(t, func() {
		factory := func() map[outbox.HandlerKey]outbox.Handler {
			t.Fatal("声明阶段不得调用 Handler 工厂")
			return nil
		}
		for _, name := range []string{"", " orders", "orders ", "a.b", "../orders", "库"} {
			requireMySQLPanic(t, "invalid Outbox database name", func() {
				bootstrap.MustRegisterOutbox(outbox.MySQL, name, factory)
			})
		}
		requireMySQLPanic(t, "backend", func() { bootstrap.MustRegisterOutbox("sqlite", "orders", factory) })
		requireMySQLPanic(t, "nil Outbox handler factory", func() {
			bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", nil)
		})
		bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", factory)
		requireMySQLPanic(t, "Outbox is already registered", func() {
			bootstrap.MustRegisterOutbox(outbox.MySQL, "other", factory)
		})
		if recoverValue(func() { outbox.DefaultStore() }) == nil || recoverValue(func() { outbox.DefaultWorker() }) == nil {
			t.Fatal("声明阶段不得发布默认实例")
		}
	})
}

func TestOutboxBuildsAfterDatabaseAndSharesMaster(t *testing.T) {
	for _, withoutLog := range []bool{false, true} {
		t.Run(fmt.Sprintf("withoutLog=%v", withoutLog), func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				server := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "orders", mysqlConfigYAML(server.port))
				calls := 0
				bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", func() map[outbox.HandlerKey]outbox.Handler {
					calls++
					pool, err := mysqlx.Named("orders").MasterDB(context.Background()).DB()
					if err != nil || pool.Ping() != nil {
						t.Fatal("Handler 工厂应在共享主库就绪后执行")
					}
					return outboxHandlers()
				})
				bootstrap.MustRegisterMySQL("orders", false)
				var resources lifecycle.Stack
				defer resources.Close()
				if withoutLog {
					bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
				} else {
					bootstrap.MustBaseInit(cfg, &resources)
				}
				if calls != 1 || outbox.DefaultStore() == nil || outbox.DefaultWorker() == nil {
					t.Fatal("应只构造一次默认 Store 与 Worker")
				}
				requireMySQLPanic(t, "initialization has started", func() {
					bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", outboxHandlers)
				})
				// 查询回调检查实际绑定的共享主库和请求 context，无需协议桩模拟任务表。
				type requestKey struct{}
				ctx := context.WithValue(context.Background(), requestKey{}, "request")
				want := errors.New("共享主库查询错误")
				db := mysqlx.Named("orders").MasterDB(ctx)
				if err := db.Callback().Query().Before("gorm:query").Register("outbox_test:master", func(tx *gorm.DB) {
					if tx.Statement.Context.Value(requestKey{}) != "request" {
						t.Error("未保留本次请求 context")
					}
					tx.AddError(want)
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := outbox.Get(ctx, 1); !errors.Is(err, want) {
					t.Fatalf("默认 Store 未复用指定主库: %v", err)
				}
				if err := outbox.Send(ctx, 1); !errors.Is(err, want) {
					t.Fatalf("默认 Worker 未绑定该 Store: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				server.requireClosed(t)
			})
		})
	}
}

func TestOutboxInitializationFailures(t *testing.T) {
	for _, scenario := range []string{"missing-database", "invalid-handlers", "factory-panic", "existing-default"} {
		t.Run(scenario, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				server := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "orders", mysqlConfigYAML(server.port))
				bootstrap.MustRegisterMySQL("orders", false)
				name := "orders"
				if scenario == "missing-database" {
					name = "missing"
				}
				var existing *outbox.Store
				if scenario == "existing-default" {
					var err error
					existing, err = outbox.NewDefaultStore(outbox.StoreConfig{
						Backend:  outbox.MySQL,
						MasterDB: func(context.Context) *gorm.DB { panic("不得替换已有默认 Store") },
					})
					if err != nil {
						t.Fatal(err)
					}
				}
				cause := errors.New("Handler 构造失败")
				called := false
				bootstrap.MustRegisterOutbox(outbox.MySQL, name, func() map[outbox.HandlerKey]outbox.Handler {
					called = true
					if scenario == "factory-panic" {
						panic(cause)
					}
					if scenario == "invalid-handlers" {
						return nil
					}
					return outboxHandlers()
				})
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				if value == nil {
					t.Fatal("装配失败应中止启动")
				}
				switch scenario {
				case "missing-database":
					if called || !strings.Contains(fmt.Sprint(value), "missing") {
						t.Fatalf("依赖缺失应先于工厂调用失败: %v", value)
					}
				case "factory-panic":
					if value != cause || recoverValue(func() { outbox.DefaultStore() }) == nil {
						t.Fatal("工厂 panic 应原样传播，且不提前发布 Store")
					}
				case "existing-default":
					if outbox.DefaultStore() != existing || !strings.Contains(fmt.Sprint(value), "already registered") {
						t.Fatal("不得覆盖已注册的默认实例")
					}
				}
				if recoverValue(func() { outbox.DefaultWorker() }) == nil {
					t.Fatal("装配失败不得发布默认 Worker")
				}
				requireMySQLPanic(t, "initialization has started", func() {
					bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", outboxHandlers)
				})
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				server.requireClosed(t)
			})
		})
	}
}
