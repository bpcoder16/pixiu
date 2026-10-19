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

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func TestMustBaseInitRejectsMissingArguments(t *testing.T) {
	for _, name := range []string{"config", "resources"} {
		t.Run(name, func(t *testing.T) {
			cfg := &appconfig.AppConfig{}
			resources := &lifecycle.Stack{}
			switch name {
			case "config":
				cfg = nil
			case "resources":
				resources = nil
			}
			defer func() {
				if value := recover(); value == nil {
					t.Fatal("缺少启动参数应 panic")
				}
			}()
			bootstrap.MustBaseInit(cfg, resources)
		})
	}
}

func TestMustBaseInitWithoutLogitRejectsMissingArguments(t *testing.T) {
	for _, name := range []string{"config", "resources"} {
		t.Run(name, func(t *testing.T) {
			cfg := &appconfig.AppConfig{}
			resources := &lifecycle.Stack{}
			want := "bootstrap: nil app config"
			if name == "config" {
				cfg = nil
			} else {
				resources = nil
				want = "bootstrap: nil resource stack"
			}
			value := recoverValue(func() { bootstrap.MustBaseInitWithoutLogit(cfg, resources) })
			if fmt.Sprint(value) != want {
				t.Fatalf("参数错误不符: got %v, want %s", value, want)
			}
		})
	}
}

func TestMustBaseInitWithoutLogitPreservesLogging(t *testing.T) {
	for _, tc := range []struct {
		name       string
		logYAML    string
		configured bool
	}{
		{name: "omitted"},
		{
			name:       "valid-with-existing-loggers",
			logYAML:    "log:\n  format: json\n  names: [worker]\n  hookStd: true\n",
			configured: true,
		},
		{
			name:       "invalid-with-existing-loggers",
			logYAML:    "log:\n  format: invalid\n  names: [worker]\n  rotate:\n    maxFiles: -1\n",
			configured: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				time.Local = time.UTC
				cfg := loadLogConfig(t, "debug", tc.logYAML)
				files := make(map[string]string)
				if tc.configured {
					for _, name := range []string{"default", "worker"} {
						path := filepath.Join(t.TempDir(), name+".log")
						writer, err := logit.OpenFile(path)
						if err != nil {
							t.Fatal(err)
						}
						logger := logit.MustNew(logit.OptWriter(writer))
						defer logit.Close(logger)
						if name == "default" {
							logit.SetDefault(logger)
						} else {
							logit.SetNamed(name, logger)
						}
						files[name] = path
					}
				}
				wantDefault, wantNamed := logit.Default(), logit.Named("worker")
				streams := []*os.File{os.Stdout, os.Stderr}
				before := make([]os.FileInfo, len(streams))
				for i, stream := range streams {
					var err error
					before[i], err = stream.Stat()
					if err != nil {
						t.Fatal(err)
					}
				}
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
				if time.Local != env.TimeLocation() {
					t.Fatal("跳过日志初始化仍应应用环境时区")
				}
				if logit.Default() != wantDefault || logit.Named("worker") != wantNamed {
					t.Fatal("不应替换默认或命名 Logger")
				}
				for i, stream := range streams {
					after, err := stream.Stat()
					if err != nil || !os.SameFile(before[i], after) {
						t.Fatalf("不应重定向 %s: %v", stream.Name(), err)
					}
				}
				if _, err := os.Stat(filepath.Join(env.RootDirPath(), "log")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("不应创建日志目录: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				// 资源栈不接管自管日志；关闭栈后仍应能写入原有目标。
				logit.Info(context.Background(), "default remains open")
				logit.Named("worker").Info(context.Background(), "worker remains open")
				for name, path := range files {
					data, err := os.ReadFile(path)
					if err != nil || !strings.Contains(string(data), name+" remains open") {
						t.Fatalf("自管 Logger 已被关闭或替换: %s, %v, %s", name, err, data)
					}
				}
			})
		})
	}
}

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
