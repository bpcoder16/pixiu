//go:build !pixiu_bootstrap_no_redis

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

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
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
		{name: "pool-duration", content: "host: localhost\npool:\n  timeout: -1s\n", want: "pool.timeout"},
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

func TestRedisClientConfigFailureDuringInitialization(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "pool-size", content: "pool:\n  size: -1\n", want: "redisx: invalid PoolSize"},
		{name: "max-active", content: "pool:\n  maxActiveConns: -1\n", want: "redisx: invalid MaxActiveConns"},
		{name: "min-idle", content: "pool:\n  minIdleConns: -1\n", want: "redisx: invalid MinIdleConns"},
		{name: "max-idle", content: "pool:\n  maxIdleConns: -1\n", want: "redisx: invalid MaxIdleConns"},
		{name: "threshold", content: "slowThreshold: -1s\n", want: "redisx: negative slow threshold"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				cache := newBootstrapRedisServer(t, "cache", "+PONG\r\n")
				session := newBootstrapRedisServer(t, "session", "+PONG\r\n")
				writeRedisConfig(t, "cache", redisConfigYAML(cache.port))
				writeRedisConfig(t, "session", redisConfigYAML(session.port)+tc.content)
				bootstrap.MustRegisterRedis("cache", true)
				bootstrap.MustRegisterRedis("session", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireRedisPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "initialize instance") || !strings.Contains(err.Error(), "redis.session.yaml") {
					t.Fatalf("客户端校验应在初始化阶段报告实例和路径: %v", err)
				}
				if session.connected.Load() || recoverValue(func() { redisx.Named("session") }) == nil {
					t.Fatal("无效客户端配置不应建立连接或发布实例")
				}
				if err := redisx.Default().Client().Ping(context.Background()).Err(); err != nil {
					t.Fatalf("此前成功的实例应由应用栈管理: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				cache.requireClosed(t)
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
