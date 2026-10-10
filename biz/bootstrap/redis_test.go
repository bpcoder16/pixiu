package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

func requireRedisPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func writeRedisConfig(t *testing.T, name, content string) {
	t.Helper()
	path := filepath.Join(env.ConfigDirPath(), "redis."+name+".yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func redisConfigYAML(port int) string {
	return fmt.Sprintf("host: 127.0.0.1\nport: %d\nmaxRetries: -1\ndialTimeout: 200ms\nreadTimeout: 200ms\nwriteTimeout: 200ms\n", port)
}

func TestRedisRegistrationRejectsInvalidDeclarations(t *testing.T) {
	isolatedLog(t, func() {
		for _, name := range []string{"", " ", " cache", "cache ", "../cache", "a/b", "a\\b", "a.b", "库", "a\x00b"} {
			requireRedisPanic(t, "invalid Redis name", func() { bootstrap.MustRegisterRedis(name, true) })
		}
		// 未发布环境；声明不得读文件或建立连接。
		bootstrap.MustRegisterRedis("cache_1-main", true)
		requireRedisPanic(t, "already registered", func() { bootstrap.MustRegisterRedis("cache_1-main", false) })
		requireRedisPanic(t, "default Redis", func() { bootstrap.MustRegisterRedis("session", true) })
		bootstrap.MustRegisterRedis("session", false)
		if recoverValue(func() { redisx.Named("cache_1-main") }) == nil {
			t.Fatal("声明阶段不应创建客户端")
		}
	})
}

func TestRedisRegistrationClosesWhenBaseInitStarts(t *testing.T) {
	for _, format := range []string{"text", "invalid"} {
		t.Run(format, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: "+format+"\n")
				writeRedisConfig(t, "unused", "invalid: [")
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				if (value == nil) != (format == "text") {
					t.Fatalf("初始化结果不符: %v", value)
				}
				requireRedisPanic(t, "initialization has started", func() { bootstrap.MustRegisterRedis("late", true) })
				requireRedisPanic(t, "initialization has started", func() { bootstrap.MustBaseInit(cfg, &resources) })
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := redisx.NewNamed(redisx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
					t.Fatalf("未声明 Redis 不应关闭其模块: %v", err)
				}
			})
		})
	}
}

func TestRedisConfigFailureBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "redis.session.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "identity", content: "name: other\n", want: "name"},
		{name: "default", content: "isDefault: true\n", want: "isdefault"},
		{name: "pool-field", content: "pool:\n  typo: 2\n", want: "typo"},
		{name: "type", content: "port: invalid\n", want: "port"},
		{name: "duration", content: "readTimeout: later\n", want: "readTimeout"},
		{name: "syntax", content: "host: [", want: "redis.session.yaml"},
		{name: "missing-host", content: "{}", want: "host"},
		{name: "blank-host", content: "host: ' '\n", want: "host"},
		{name: "port-range", content: "host: localhost\nport: 65536\n", want: "port"},
		{name: "database", content: "host: localhost\ndb: -1\n", want: "db"},
		{name: "retries", content: "host: localhost\nmaxRetries: -2\n", want: "maxRetries"},
		{name: "negative-read-timeout", content: "host: localhost\nreadTimeout: -1s\n", want: "readTimeout"},
		{name: "pool-size", content: "host: localhost\npool:\n  size: -1\n", want: "pool.size"},
		{name: "pool-duration", content: "host: localhost\npool:\n  timeout: -1s\n", want: "pool.timeout"},
		{name: "threshold", content: "host: localhost\nslowThreshold: -1s\n", want: "slowThreshold"},
		{name: "unsupported-total-deadline", content: "host: localhost\ninitTimeout: 1s\n", want: "inittimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				server := newBootstrapRedisServer(t, "cache", "+PONG\r\n")
				writeRedisConfig(t, "cache", redisConfigYAML(server.port))
				if tc.name != "missing" {
					writeRedisConfig(t, "session", tc.content)
				}
				bootstrap.MustRegisterRedis("cache", true)
				bootstrap.MustRegisterRedis("session", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireRedisPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "redis.session.yaml") || !strings.Contains(err.Error(), env.ConfigDirPath()) {
					t.Fatalf("配置错误缺少实例和路径: %v", err)
				}
				if tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失文件应保留错误链: %v", err)
				}
				if server.connected.Load() {
					t.Fatal("应先解析、校验全部 Redis 文件，再连接首个 Redis 实例")
				}
			})
		})
	}
}

func TestRedisInstancesRouteAndCloseWithMySQL(t *testing.T) {
	for _, useDefault := range []bool{false, true} {
		t.Run(fmt.Sprintf("default=%v", useDefault), func(t *testing.T) {
			isolatedLog(t, func() {
				template, err := os.ReadFile("conf.example/redis.example.yaml")
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadLogConfig(t, "release", "log:\n  format: text\n")
				cache := newBootstrapRedisServer(t, "cache-value", "+PONG\r\n")
				session := newBootstrapRedisServer(t, "session-value", "+PONG\r\n")
				database := newBootstrapMySQLServer(t, "8.0.36", false)
				content := strings.Replace(string(template), "port: 6379", fmt.Sprintf("port: %d", cache.port), 1)
				writeRedisConfig(t, "cache", content)
				writeRedisConfig(t, "session", redisConfigYAML(session.port)+"username: worker\npassword: test-secret\ndb: 2\n")
				writeMySQLConfig(t, "cache", mysqlConfigYAML(database.port))
				bootstrap.MustRegisterMySQL("cache", true)
				bootstrap.MustRegisterRedis("cache", useDefault)
				bootstrap.MustRegisterRedis("session", false)
				// 同名的配置注册项不应被读取或覆盖。
				unrelated := filepath.Join(t.TempDir(), "unrelated.yaml")
				if err := os.WriteFile(unrelated, []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := configx.Load[struct{}]("pixiu.biz.bootstrap.redis.cache", unrelated); err != nil {
					t.Fatal(err)
				}
				t.Chdir(t.TempDir())
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				if err := configx.Load[struct{}]("pixiu.biz.bootstrap.redis.session", unrelated); err != nil {
					t.Fatalf("Redis 文件不应注册全局配置: %v", err)
				}
				shared := redisx.Named("cache")
				if useDefault {
					if redisx.Default() != shared {
						t.Fatal("默认与命名实例应共享连接池")
					}
				} else if recoverValue(func() { redisx.Default() }) == nil {
					t.Fatal("不得自动将首个 Redis 实例设为默认")
				}
				ctx := context.Background()
				for _, name := range []string{"cache", "session"} {
					if value, err := redisx.Named(name).Client().Get(ctx, name+"-marker").Result(); err != nil || value != name+"-value" {
						t.Fatalf("实例路由错误: value=%q err=%v", value, err)
					}
				}
				if err := shared.Client().Get(ctx, "missing").Err(); !errors.Is(err, redis.Nil) {
					t.Fatalf("应保留 Redis 未命中语义: %v", err)
				}
				opts := shared.Client().Options()
				if opts.PoolSize != 100 || opts.MaxActiveConns != 0 || opts.MinIdleConns != 10 || opts.MaxIdleConns != 20 || opts.PoolTimeout != 4*time.Second || opts.ConnMaxIdleTime != 30*time.Minute || opts.ConnMaxLifetime != 0 || opts.DialTimeout != 3*time.Second || opts.ReadTimeout != 3*time.Second || opts.WriteTimeout != 3*time.Second || opts.MaxRetries != 0 || !opts.ContextTimeoutEnabled {
					t.Fatal("模板中的池、超时或重试配置未生效")
				}
				opts = redisx.Named("session").Client().Options()
				if opts.Username != "worker" || opts.Password != "test-secret" || opts.DB != 2 {
					t.Fatal("认证和 DB 配置未生效")
				}
				wantCloseErr := errors.New("worker close failed")
				if err := resources.Register(func() error {
					if err := shared.Client().Ping(ctx).Err(); err != nil {
						t.Errorf("业务资源关闭时 Redis 应仍可用: %v", err)
					}
					pool, err := mysqlx.Default().MasterDB(ctx).DB()
					if err != nil || pool.Ping() != nil {
						t.Errorf("业务资源关闭时 MySQL 应仍可用: %v", err)
					}
					logit.Info(ctx, "worker closed with Redis available")
					return wantCloseErr
				}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := resources.Close(); !errors.Is(err, wantCloseErr) {
						t.Fatalf("关闭错误应汇总且不阻止回收: %v", err)
					}
				}
				cache.requireClosed(t)
				session.requireClosed(t)
				database.requireClosed(t)
				// 在故意向已关闭客户端发命令前检查，避免该误用触发 Hook 写入已关闭日志。
				if stats := logit.Default().(logit.WriteErrorStats); stats.WriteErrors() != 0 {
					t.Fatalf("日志提前关闭: %v", stats.LastWriteError())
				}
				if err := shared.Client().Ping(ctx).Err(); !errors.Is(err, redis.ErrClosed) || recoverValue(func() { redisx.Named("cache") }) == nil {
					t.Fatalf("关闭后客户端或注册表仍可用: %v", err)
				}
				infoPath := filepath.Join(env.RootDirPath(), "log/bootstrap-test.info.log")
				data, err := os.ReadFile(infoPath)
				if err != nil || !strings.Contains(string(data), "worker closed with Redis available") {
					t.Fatalf("关闭期间日志丢失: %v", err)
				}
				requireLogFileOpen(t, infoPath, false)
				if !strings.Contains(string(data), "cache-marker") || strings.Contains(string(data), "session-marker") {
					t.Fatalf("release 模式的 Info 文件应记录已开启的正常命令: %s", data)
				}
				data, err = os.ReadFile(filepath.Join(env.RootDirPath(), "log/bootstrap-test.debug.log"))
				if err != nil || strings.Contains(string(data), "cache-marker") {
					t.Fatalf("正常命令不应写入 Debug 文件: %s, %v", data, err)
				}
			})
		})
	}
}

func TestRedisPartialInitializationLeavesCompletedClientsForCaller(t *testing.T) {
	for _, pingReply := range []string{"", "-NOPERM ping denied\r\n"} {
		t.Run(fmt.Sprintf("reply=%q", pingReply), func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				cache := newBootstrapRedisServer(t, "cache", "+PONG\r\n")
				failed := newBootstrapRedisServer(t, "", pingReply)
				writeRedisConfig(t, "cache", redisConfigYAML(cache.port))
				writeRedisConfig(t, "session", redisConfigYAML(failed.port))
				bootstrap.MustRegisterRedis("cache", true)
				bootstrap.MustRegisterRedis("session", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireRedisPanic(t, "redis.session.yaml", func() { bootstrap.MustBaseInit(cfg, &resources) })
				if pingReply == "" {
					var timeout net.Error
					if !errors.As(err, &timeout) || !timeout.Timeout() {
						t.Fatalf("应保留超时错误链: %v", err)
					}
				}
				failed.requireClosed(t)
				if recoverValue(func() { redisx.Named("session") }) == nil {
					t.Fatal("失败实例不应发布")
				}
				if err := redisx.Default().Client().Ping(context.Background()).Err(); err != nil {
					t.Fatalf("成功实例应交给应用栈关闭: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				cache.requireClosed(t)
			})
		})
	}
}
