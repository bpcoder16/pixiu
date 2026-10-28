package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/biz/lockx/redislock"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestRedisLockRegistration(t *testing.T) {
	isolatedLog(t, func() {
		for _, name := range []string{"", " ", " lock", "lock ", "a/b", "a.b", "锁"} {
			requireRedisPanic(t, "invalid RedisLock name", func() {
				bootstrap.MustRegisterRedisLock(name, true, "cache", redislock.Config{})
			})
		}
		requireRedisPanic(t, "invalid Redis name", func() {
			bootstrap.MustRegisterRedisLock("default", true, "", redislock.Config{})
		})
		// 声明可先于 Redis；不读取锁配置文件，不建立连接，也不提前发布执行器。
		bootstrap.MustRegisterRedisLock("default", true, "cache", redislock.Config{})
		requireRedisPanic(t, "already registered", func() {
			bootstrap.MustRegisterRedisLock("default", false, "cache", redislock.Config{})
		})
		requireRedisPanic(t, "default RedisLock", func() {
			bootstrap.MustRegisterRedisLock("slow", true, "cache", redislock.Config{})
		})
		bootstrap.MustRegisterRedisLock("slow", false, "cache", redislock.Config{})
		if recoverValue(func() { lockx.Default() }) == nil || recoverValue(func() { lockx.Named("slow") }) == nil {
			t.Fatal("声明阶段不能发布锁执行器")
		}
	})
}

func TestRedisLockInstancesShareRedisAndKeepConfig(t *testing.T) {
	for _, withoutLog := range []bool{false, true} {
		t.Run(fmt.Sprintf("withoutLog=%v", withoutLog), func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				var mu sync.Mutex
				tokens := make(map[string]string)
				var ttls []string
				server := newBootstrapRedisServer(t, "", "+PONG\r\n", func(args []string) string {
					mu.Lock()
					defer mu.Unlock()
					switch strings.ToUpper(args[0]) {
					case "SET":
						ttls = append(ttls, args[5])
						if tokens[args[1]] != "" {
							return "$-1\r\n"
						}
						tokens[args[1]] = args[2]
						return "+OK\r\n"
					case "EVAL":
						if tokens[args[3]] == args[4] {
							delete(tokens, args[3])
							return ":1\r\n"
						}
						return ":0\r\n"
					}
					return "+OK\r\n"
				})
				writeRedisConfig(t, "cache", redisConfigYAML(server.port))
				fastConfig := redislock.Config{TTL: time.Second}
				bootstrap.MustRegisterRedisLock("default", true, "cache", fastConfig)
				bootstrap.MustRegisterRedisLock("slow", false, "cache", redislock.Config{TTL: time.Minute})
				fastConfig.TTL = time.Hour
				// Redis 无需是默认实例；仅凭引用的名称解析依赖。
				bootstrap.MustRegisterRedis("cache", false)
				var resources lifecycle.Stack
				defer resources.Close()
				if withoutLog {
					bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
				} else {
					bootstrap.MustBaseInit(cfg, &resources)
				}
				if lockx.Default() != lockx.Named("default") || lockx.Default() == lockx.Named("slow") {
					t.Fatal("默认和命名实例绑定错误")
				}
				requireRedisPanic(t, "initialization has started", func() {
					bootstrap.MustRegisterRedisLock("late", false, "cache", redislock.Config{})
				})
				const key = "business:shared-key"
				err := lockx.Default().Do(context.Background(), key, func(ctx context.Context) error {
					executed, err := lockx.Named("slow").TryDo(ctx, key, func(context.Context) error {
						t.Error("同一 Redis 的相同 key 必须互斥，实例名不得改变 key")
						return nil
					})
					if executed || err != nil {
						t.Errorf("竞争结果错误: executed=%v err=%v", executed, err)
					}
					return nil
				})
				if err != nil {
					t.Fatal(err)
				}
				workErr := errors.New("业务失败")
				executed, err := lockx.Named("slow").TryDo(context.Background(), key, func(context.Context) error {
					return workErr
				})
				if !executed || !errors.Is(err, workErr) {
					t.Fatalf("释放后应可获取，并保留业务错误: executed=%v err=%v", executed, err)
				}
				mu.Lock()
				if strings.Join(ttls, ",") != "1000,60000,60000" || len(tokens) != 0 {
					t.Errorf("各实例配置应独立且按值保存，每次执行后释放: ttls=%v tokens=%v", ttls, tokens)
				}
				mu.Unlock()
				if err := redisx.Named("cache").Client().Ping(context.Background()).Err(); err != nil {
					t.Fatalf("执行器不得关闭共享 Redis: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				server.requireClosed(t)
			})
		})
	}
}

func TestRedisLockInitializationFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  redislock.Config
		want string
	}{
		{name: "missing-redis", want: "missing"},
		{name: "retry-enabled", want: "MaxRetries=-1"},
		{name: "ttl", cfg: redislock.Config{TTL: -1}, want: "negative ttl"},
		{name: "wait", cfg: redislock.Config{WaitTimeout: -1}, want: "negative wait timeout"},
		{name: "interval", cfg: redislock.Config{RetryInterval: -1}, want: "negative retry interval"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				server := newBootstrapRedisServer(t, "", "+PONG\r\n")
				content := redisConfigYAML(server.port)
				if tc.name == "retry-enabled" {
					content = strings.ReplaceAll(content, "maxRetries: -1", "maxRetries: 0")
				}
				writeRedisConfig(t, "cache", content)
				bootstrap.MustRegisterRedis("cache", true)
				redisName := "cache"
				if tc.name == "missing-redis" {
					redisName = "missing"
				}
				bootstrap.MustRegisterRedisLock("broken", true, redisName, tc.cfg)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireRedisPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), "broken") {
					t.Fatalf("错误应标识锁实例: %v", err)
				}
				if recoverValue(func() { lockx.Default() }) == nil || recoverValue(func() { lockx.Named("broken") }) == nil {
					t.Fatal("失败的锁实例不得发布")
				}
				requireRedisPanic(t, "initialization has started", func() {
					bootstrap.MustRegisterRedisLock("late", false, "cache", redislock.Config{})
				})
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				server.requireClosed(t)
			})
		})
	}
}
