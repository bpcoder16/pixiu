package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func requireMySQLPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func writeMySQLConfig(t *testing.T, name, content string) {
	t.Helper()
	path := filepath.Join(env.ConfigDirPath(), "mysql."+name+".yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mysqlEndpointYAML(port int) string {
	return fmt.Sprintf("  host: \"127.0.0.1\"\n  port: %d\n", port)
}

func mysqlConfigYAML(port int) string {
	return `database: "test"
username: "test"
readTimeout: "1s"
writeTimeout: "1s"
pool:
  maxOpenConns: 7
  maxIdleConns: 2
master:
` + mysqlEndpointYAML(port)
}

func TestMySQLRegistrationRejectsInvalidDeclarations(t *testing.T) {
	for _, name := range []string{"", " ", " orders", "orders ", "../orders", "a/b", `a\b`, "a.b", "库", "a\x00b"} {
		t.Run(fmt.Sprintf("name=%q", name), func(t *testing.T) {
			isolatedLog(t, func() {
				// 尚未发布环境；注册只能校验声明，不应读取文件或建连。
				requireMySQLPanic(t, "invalid MySQL name", func() {
					bootstrap.MustRegisterMySQL(name, true)
				})
				bootstrap.MustRegisterMySQL("orders_1-read", true)
			})
		})
	}
	for _, duplicateDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("default-conflict=%v", duplicateDefault), func(t *testing.T) {
			isolatedLog(t, func() {
				bootstrap.MustRegisterMySQL("orders", true)
				name, want := "orders", "already registered"
				if duplicateDefault {
					name, want = "reports", "default MySQL"
				}
				requireMySQLPanic(t, want, func() { bootstrap.MustRegisterMySQL(name, duplicateDefault) })
				if duplicateDefault {
					// 失败的声明不占用名称。
					bootstrap.MustRegisterMySQL("reports", false)
				}
				if recoverValue(func() { mysqlx.Named("orders") }) == nil {
					t.Fatal("声明阶段不应创建客户端")
				}
			})
		})
	}
}

func TestMySQLRegistrationClosesWhenBaseInitStarts(t *testing.T) {
	for _, validLog := range []bool{false, true} {
		t.Run(fmt.Sprintf("valid-log=%v", validLog), func(t *testing.T) {
			isolatedLog(t, func() {
				format := "text"
				if !validLog {
					format = "invalid"
				}
				cfg := loadLogConfig(t, "debug", "log:\n  format: "+format+"\n")
				// 未声明的文件即使损坏也不应加载。
				writeMySQLConfig(t, "unused", "invalid: [")
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				if (value == nil) != validLog {
					t.Fatalf("日志初始化结果不符: %v", value)
				}
				requireMySQLPanic(t, "initialization has started", func() { bootstrap.MustRegisterMySQL("late", true) })
				requireMySQLPanic(t, "initialization has started", func() {
					bootstrap.MustBaseInit(cfg, &resources)
				})
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				// 没有 MySQL 声明时不应登记 CloseAll、封闭 mysqlx 自己的注册表。
				if _, err := mysqlx.NewNamed(mysqlx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
					t.Fatalf("未声明 MySQL 不应关闭其模块: %v", err)
				}
			})
		})
	}
}

func TestMySQLConfigFailureBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "mysql.reports.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "name-in-file", content: "name: other\n", want: "name"},
		{name: "default-in-file", content: "isDefault: true\n", want: "isdefault"},
		{name: "old-endpoint-fields", content: "master:\n  database: test\n", want: "database"},
		{name: "shared-pool-unknown", content: "pool:\n  typo: 2\n", want: "typo"},
		{name: "old-endpoint-pool", content: "master:\n  pool:\n    typo: 2\n", want: "pool"},
		{name: "wrong-type", content: "master:\n  port: invalid\n", want: "port"},
		{name: "duration", content: "initTimeout: later\n", want: "initTimeout"},
		{name: "syntax", content: "master: [", want: "mysql.reports.yaml"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				server := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "orders", mysqlConfigYAML(server.port))
				if tc.name != "missing" {
					writeMySQLConfig(t, "reports", tc.content)
				}
				bootstrap.MustRegisterMySQL("orders", true)
				bootstrap.MustRegisterMySQL("reports", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireMySQLPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "reports") || !strings.Contains(err.Error(), env.ConfigDirPath()) {
					t.Fatalf("配置错误缺少实例和路径: %v", err)
				}
				if tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失文件应保留错误链: %v", err)
				}
				if server.connected.Load() {
					t.Fatal("应先解析全部文件，再建立首个实例连接")
				}
			})
		})
	}
}

func TestMySQLRejectsInvalidConnectionSettings(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(string) string
		want string
	}{
		{
			name: "missing-host",
			edit: func(s string) string { return strings.Replace(s, `host: "127.0.0.1"`, `host: ""`, 1) },
			want: "requires host",
		},
		{
			name: "pool-limit",
			edit: func(s string) string { return strings.Replace(s, "maxIdleConns: 2", "maxIdleConns: 8", 1) },
			want: "max idle connections exceed",
		},
		{
			name: "negative-timeout",
			edit: func(s string) string { return "initTimeout: -1s\n" + s },
			want: "negative initialization timeout",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				server := newBootstrapMySQLServer(t, "8.0.36", false)
				writeMySQLConfig(t, "orders", tc.edit(mysqlConfigYAML(server.port)))
				bootstrap.MustRegisterMySQL("orders", true)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireMySQLPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "mysql.orders.yaml") || server.connected.Load() {
					t.Fatalf("连接配置错误应携带路径且不建连: %v", err)
				}
			})
		})
	}
}

func TestMySQLInstancesRouteAndCloseBeforeLogs(t *testing.T) {
	for _, useDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("default=%v", useDefault), func(t *testing.T) {
			isolatedLog(t, func() {
				template, err := os.ReadFile("conf.example/mysql.example.yaml")
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				master := newBootstrapMySQLServer(t, "8.0.36-master", false)
				slave := newBootstrapMySQLServer(t, "8.0.36-slave", false)
				reports := newBootstrapMySQLServer(t, "8.0.36-reports", false)
				content := strings.Replace(string(template), "port: 3306", fmt.Sprintf("port: %d", master.port), 1)
				content = strings.Replace(content, "slaves: []", "slaves:\n-\n"+mysqlEndpointYAML(slave.port), 1)
				writeMySQLConfig(t, "orders", content)
				writeMySQLConfig(t, "reports", mysqlConfigYAML(reports.port))
				// 原配置名称可供其他调用方使用，不应影响 MySQL 初始化。
				unrelatedFile := filepath.Join(t.TempDir(), "unrelated.yaml")
				if err := os.WriteFile(unrelatedFile, []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
				const ordersConfigName = "pixiu.biz.bootstrap.mysql.orders"
				if err := configx.Load[struct{}](ordersConfigName, unrelatedFile); err != nil {
					t.Fatal(err)
				}
				bootstrap.MustRegisterMySQL("orders", useDefault)
				bootstrap.MustRegisterMySQL("reports", false)
				// 文件解析必须使用已发布的配置目录，不受工作目录改变影响。
				t.Chdir(t.TempDir())
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				if _, err := configx.Get[struct{}](ordersConfigName); err != nil {
					t.Fatalf("MySQL 初始化不应改变已有命名配置: %v", err)
				}
				if err := configx.Load[struct{}]("pixiu.biz.bootstrap.mysql.reports", unrelatedFile); err != nil {
					t.Fatalf("MySQL 初始化不应注册文件配置: %v", err)
				}
				orders := mysqlx.Named("orders")
				if useDefault {
					if mysqlx.Default() != orders {
						t.Fatal("默认实例与命名实例应共享连接池")
					}
				} else if recoverValue(func() { mysqlx.Default() }) == nil {
					t.Fatal("不得自动将首个实例设为默认")
				}
				ctx := context.Background()
				for _, db := range []struct {
					name string
					read bool
					want string
				}{
					{name: "orders", want: "8.0.36-master"},
					{name: "orders", read: true, want: "8.0.36-slave"},
					{name: "reports", read: true, want: "8.0.36-reports"},
				} {
					client := mysqlx.Named(db.name)
					session := client.MasterDB(ctx)
					if db.read {
						session = client.SlaveDB(ctx)
					}
					var version string
					if err := session.Raw("SELECT VERSION()").Row().Scan(&version); err != nil || version != db.want {
						t.Fatalf("主从路由错误: version=%q err=%v", version, err)
					}
				}
				pool, err := orders.MasterDB(ctx).DB()
				if err != nil {
					t.Fatal(err)
				}
				driver := orders.MasterDB(ctx).Dialector.(*gormmysql.Dialector).DSNConfig
				if pool.Stats().MaxOpenConnections != 100 || driver.ReadTimeout != 5*time.Second || driver.Loc.String() != "Asia/Shanghai" || driver.Params["time_zone"] != "'+08:00'" {
					t.Fatal("模板中的连接池、超时或会话时区未正确生效")
				}
				wantCloseErr := errors.New("worker close failed")
				if err := resources.Register(func() error {
					if err := pool.PingContext(ctx); err != nil {
						t.Errorf("业务资源关闭时数据库应仍然可用: %v", err)
					}
					logit.Info(ctx, "worker closed with database available")
					return wantCloseErr
				}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := resources.Close(); !errors.Is(err, wantCloseErr) {
						t.Fatalf("关闭失败应汇总且不阻止其他资源关闭: %v", err)
					}
				}
				if err := pool.PingContext(ctx); err == nil || recoverValue(func() { mysqlx.Named("orders") }) == nil {
					t.Fatal("资源栈关闭后连接池或注册表仍可用")
				}
				for _, server := range []*bootstrapMySQLServer{master, slave, reports} {
					server.requireClosed(t)
				}
				path := filepath.Join(env.RootDirPath(), "log/bootstrap-test.info.log")
				data, err := os.ReadFile(path)
				if err != nil || !strings.Contains(string(data), "worker closed with database available") {
					t.Fatalf("资源关闭期间日志丢失: %v", err)
				}
				requireLogFileOpen(t, path, false)
				if stats := logit.Default().(logit.WriteErrorStats); stats.WriteErrors() != 0 {
					t.Fatalf("关闭期间日志提前关闭: %v", stats.LastWriteError())
				}
			})
		})
	}
}

func TestMySQLPartialInitializationLeavesCompletedClientsForCaller(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
		orders := newBootstrapMySQLServer(t, "8.0.36", false)
		reports := newBootstrapMySQLServer(t, "8.0.36", false)
		stalled := newBootstrapMySQLServer(t, "8.0.36", true)
		writeMySQLConfig(t, "orders", mysqlConfigYAML(orders.port))
		writeMySQLConfig(t, "reports", "initTimeout: 200ms\n"+mysqlConfigYAML(reports.port)+"slaves:\n-\n"+mysqlEndpointYAML(stalled.port))
		bootstrap.MustRegisterMySQL("orders", true)
		bootstrap.MustRegisterMySQL("reports", false)
		var resources lifecycle.Stack
		defer resources.Close()
		err := requireMySQLPanic(t, "reports", func() { bootstrap.MustBaseInit(cfg, &resources) })
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "mysql.reports.yaml") {
			t.Fatalf("初始化失败应保留路径和超时错误链: %v", err)
		}
		reports.requireClosed(t)
		stalled.requireClosed(t)
		pool, err := mysqlx.Default().MasterDB(context.Background()).DB()
		if err != nil || pool.Ping() != nil {
			t.Fatalf("此前成功的客户端应交由应用关闭: %v", err)
		}
		logit.Info(context.Background(), "initialization failed; cleaning up")
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		orders.requireClosed(t)
		if pool.Ping() == nil {
			t.Fatal("部分启动失败泄露已完成实例的连接池")
		}
	})
}

func TestMySQLSharedConfigAndSQLLogDefaults(t *testing.T) {
	for _, tc := range []struct {
		name              string
		flags             string
		wantSQL           bool
		wantInterpolation bool
	}{
		{name: "omitted", wantSQL: true, wantInterpolation: true},
		{name: "explicit-true", flags: "logSQL: true\ninterpolateSQL: true\n", wantSQL: true, wantInterpolation: true},
		{name: "disabled", flags: "logSQL: false\ninterpolateSQL: false\n"},
		{name: "placeholders", flags: "interpolateSQL: false\n", wantSQL: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				master := newBootstrapMySQLServer(t, "8.0.36", false)
				slave := newBootstrapMySQLServer(t, "8.0.36", false)
				content := tc.flags + "password: shared-secret\ncharset: latin1\nlocation: UTC\ntlsConfig: \"false\"\ndialTimeout: 2s\n" + mysqlConfigYAML(master.port) + "slaves:\n-\n" + mysqlEndpointYAML(slave.port)
				writeMySQLConfig(t, "shared", content)
				bootstrap.MustRegisterMySQL("shared", true)
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				client := mysqlx.Default()
				for _, db := range []*gorm.DB{client.MasterDB(context.Background()), client.SlaveDB(context.Background())} {
					driver := db.Dialector.(*gormmysql.Dialector).DSNConfig
					pool, err := db.DB()
					if err != nil {
						t.Fatal(err)
					}
					if driver.DBName != "test" || driver.User != "test" || driver.Passwd != "shared-secret" || driver.Loc.String() != "UTC" || driver.TLSConfig != "false" || !strings.Contains(driver.FormatDSN(), "charset=latin1") || driver.Timeout != 2*time.Second || driver.ReadTimeout != time.Second || driver.WriteTimeout != time.Second || pool.Stats().MaxOpenConnections != 7 {
						t.Fatalf("主从未使用相同公共配置: driver=%s pool=%+v", driver.Addr, pool.Stats())
					}
					// 用实际初始化的 Logger 验证参数日志；DryRun 不要求模拟服务支持预处理协议。
					if err := db.Session(&gorm.Session{DryRun: true}).Exec("UPDATE sample SET value = ?", "shared-log-marker").Error; err != nil {
						t.Fatal(err)
					}
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(filepath.Join(env.RootDirPath(), "log/bootstrap-test.info.log"))
				if err != nil {
					t.Fatal(err)
				}
				if strings.Contains(string(data), "UPDATE sample") != tc.wantSQL || strings.Contains(string(data), "shared-log-marker") != tc.wantInterpolation {
					t.Fatalf("SQL 日志开关未生效: %s", data)
				}
			})
		})
	}
}
