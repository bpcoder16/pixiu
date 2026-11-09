//go:build !pixiu_bootstrap_no_redis

package bootstrap_test

import (
	"context"
	"testing"

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestMustBaseInitWithoutLogitInitializesRegisteredResources(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "")
		server := newBootstrapRedisServer(t, "cache-value", "+PONG\r\n")
		writeRedisConfig(t, "cache", redisConfigYAML(server.port))
		bootstrap.MustRegisterRedis("cache", true)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
		client := redisx.Named("cache")
		value, err := client.Client().Get(context.Background(), "key").Result()
		if err != nil || value != "cache-value" || redisx.Default() != client {
			t.Fatalf("已声明的 Redis 未正常初始化: value=%q, err=%v", value, err)
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		server.requireClosed(t)
	})
}

func TestBaseInitEntrypointsShareStartupState(t *testing.T) {
	for _, tc := range []struct {
		name string
		init func(*appconfig.AppConfig, *lifecycle.Stack)
		fail bool
	}{
		{name: "with-logit", init: bootstrap.MustBaseInit},
		{name: "with-logit-failed", init: bootstrap.MustBaseInit, fail: true},
		{name: "without-logit", init: bootstrap.MustBaseInitWithoutLogit},
		{name: "without-logit-failed", init: bootstrap.MustBaseInitWithoutLogit, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				if tc.fail {
					bootstrap.MustRegisterRedis("missing", true)
				}
				var resources lifecycle.Stack
				defer resources.Close()
				if tc.fail {
					requireRedisPanic(t, "redis.missing.yaml", func() { tc.init(cfg, &resources) })
				} else {
					tc.init(cfg, &resources)
				}
				requireRedisPanic(t, "initialization has started", func() { bootstrap.MustBaseInit(cfg, &resources) })
				requireRedisPanic(t, "initialization has started", func() { bootstrap.MustBaseInitWithoutLogit(cfg, &resources) })
				requireRedisPanic(t, "initialization has started", func() { bootstrap.MustRegisterRedis("late", false) })
			})
		})
	}
}
