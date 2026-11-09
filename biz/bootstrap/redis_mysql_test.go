//go:build !pixiu_bootstrap_no_redis && !pixiu_bootstrap_no_mysql

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
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

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
				infoPath := filepath.Join(env.RootDirPath(), "log/bootstrap-test/biz/info.log")
				data, err := os.ReadFile(infoPath)
				if err != nil || !strings.Contains(string(data), "worker closed with Redis available") {
					t.Fatalf("关闭期间日志丢失: %v", err)
				}
				requireLogFileOpen(t, infoPath, false)
				if !strings.Contains(string(data), "cache-marker") || strings.Contains(string(data), "session-marker") {
					t.Fatalf("release 模式的 Info 文件应记录已开启的正常命令: %s", data)
				}
				data, err = os.ReadFile(filepath.Join(env.RootDirPath(), "log/bootstrap-test/biz/debug.log"))
				if err != nil || strings.Contains(string(data), "cache-marker") {
					t.Fatalf("正常命令不应写入 Debug 文件: %s, %v", data, err)
				}
			})
		})
	}
}
