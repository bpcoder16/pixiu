package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/cronx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func RegistryHeartbeat(context.Context) error { return nil }

func writeCronConfig(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "cron.yaml"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestCronConfigurationAndBinding(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		handler := cronx.NamedHandler{
			Name:    "job",
			Handler: func(context.Context) error { return nil },
		}
		for _, content := range []string{
			"unknown: true\n",
			"jobs:\n  - name: missing\n    every: 1s\n",
			"jobs:\n  - name: job\n    every: 1s\n    typo: true\n",
			"jobs:\n  - name: job\n    every: 0s\n    cron: '* * * * * *'\n",
			"jobs:\n  - name: job\n    every: 1s\n    cron: ''\n",
			"jobs:\n  - name: job\n    randomEvery: {min: 1s, max: 2s, typo: true}\n",
			"jobs:\n  - name: job\n    every: 1s\n    successLevel: warning\n",
			"lock: {}\n",
			"lock: {redisName: cache, keyPrefix: 'test:', ttl: -1s}\n",
			"lock: {redisName: cache, keyPrefix: 'test:', ttl: 1s, renewInterval: 1s}\n",
			"lock: {redisName: cache, keyPrefix: 'test:', typo: true}\n",
			"lock: {redisName: cache, keyPrefix: 'test:'}\njobs:\n  - name: job\n    every: 1s\n",
		} {
			writeCronConfig(t, content)
			s, err := bootstrap.NewCronScheduler(handler)
			if err == nil {
				_ = s.Shutdown()
				t.Fatalf("无效配置被接受: %s", content)
			}
			if !strings.Contains(err.Error(), "cron.yaml") {
				t.Fatalf("应包含配置路径: %v", err)
			}
		}
		writeCronConfig(t, "{}\n")
		for _, handlers := range [][]cronx.NamedHandler{{handler, handler}, {{Name: "job"}}, {{
			Name:    " job",
			Handler: handler.Handler,
		}}} {
			if s, err := bootstrap.NewCronScheduler(handlers...); err == nil {
				_ = s.Shutdown()
				t.Fatal("应拒绝非法 Handler 声明")
			}
		}
		writeCronConfig(t, "lock: {redisName: missing, keyPrefix: 'test:'}\n")
		if _, err := bootstrap.NewCronScheduler(); err == nil || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("未初始化 Redis 应返回错误: %v", err)
		}
		if err := os.Remove(filepath.Join(env.ConfigDirPath(), "cron.yaml")); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.NewCronScheduler(); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("应保留文件错误链: %v", err)
		}
	})
}

func TestCronRegistryCaseSensitiveBinding(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		registry := cronx.MustNewHandlerRegistry(RegistryHeartbeat)
		for _, name := range []string{"RegistryHeartbeat", "registryHeartbeat", "registryheartbeat"} {
			t.Run(name, func(t *testing.T) {
				writeCronConfig(t, fmt.Sprintf("jobs:\n  - name: %s\n    every: 1s\n", name))
				s, err := bootstrap.NewCronScheduler(registry.Handlers()...)
				if s != nil {
					if closeErr := s.Shutdown(); closeErr != nil {
						t.Fatal(closeErr)
					}
					s.Wait()
				}
				if name == "RegistryHeartbeat" {
					if err != nil {
						t.Fatalf("与函数名完全一致的配置应绑定成功: %v", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), "unknown Cron handler") {
					t.Fatalf("大小写不一致应拒绝绑定: name=%q err=%v", name, err)
				}
			})
		}
	})
}

func TestCronMinimumInterval(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		for _, tc := range []struct {
			name     string
			schedule string
			wantErr  bool
		}{
			{"every-without-unit", "every: 1", true},
			{"every-below-minimum", "every: 99.999999ms", true},
			{"every-at-minimum", "every: 100ms", false},
			{"random-without-unit", "randomEvery: {min: 1, max: 2}", true},
			{"random-below-minimum", "randomEvery: {min: 99.999999ms, max: 200ms}", true},
			{"random-at-minimum", "randomEvery: {min: 100ms, max: 100.000001ms}", false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				writeCronConfig(t, "jobs:\n  - name: job\n    "+tc.schedule+"\n")
				s, err := bootstrap.NewCronScheduler(cronx.NamedHandler{
					Name:    "job",
					Handler: func(context.Context) error { return nil },
				})
				if s != nil {
					t.Cleanup(func() {
						if err := s.Shutdown(); err != nil {
							t.Error(err)
						}
						s.Wait()
					})
				}
				if tc.wantErr {
					if err == nil || !strings.Contains(err.Error(), "cron.yaml") || !strings.Contains(err.Error(), "100ms") || s != nil {
						t.Fatalf("应拒绝短间隔并保留配置路径和下限: schedulerNil=%v err=%v", s == nil, err)
					}
				} else if err != nil {
					t.Fatalf("恰好 100ms 应允许构造: %v", err)
				}
			})
		}
	})
}

func TestCronLockedTimeoutValidatedByScheduler(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		server := newBootstrapRedisServer(t, "", "+PONG\r\n")
		writeRedisConfig(t, "cron-lock", redisConfigYAML(server.port))
		bootstrap.MustRegisterRedis("cron-lock", false)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		for _, timeout := range []string{"0s", "-1s"} {
			writeCronConfig(t, fmt.Sprintf("lock: {redisName: cron-lock, keyPrefix: 'test:'}\njobs:\n  - name: job\n    every: 1s\n    timeout: %s\n", timeout))
			s, err := bootstrap.NewCronScheduler(cronx.NamedHandler{
				Name:    "job",
				Handler: func(context.Context) error { return nil },
			})
			if err == nil {
				_ = s.Shutdown()
				t.Fatalf("启用锁时应拒绝 timeout=%s", timeout)
			}
			if !strings.Contains(err.Error(), "cron.yaml") || !strings.Contains(err.Error(), "cronx: job \"job\": timeout") {
				t.Fatalf("超时校验应来自 cronx 并保留配置路径，实际: %v", err)
			}
		}
	})
}

func TestCronStandaloneDoesNotNeedHTTPAndShutdownBeforeRun(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeCronConfig(t, "lock: null\njobs:\n  - name: job\n    every: 100ms\n")
		ran := make(chan struct{}, 1)
		var unused atomic.Bool
		handler := cronx.NamedHandler{
			Name: "job",
			Handler: func(context.Context) error {
				select {
				case ran <- struct{}{}:
				default:
				}
				return nil
			},
		}
		s, err := bootstrap.NewCronScheduler(handler, cronx.NamedHandler{
			Name: "unused",
			Handler: func(context.Context) error {
				unused.Store(true)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		service := bootstrap.CronService(s)
		defer s.Shutdown()
		result := make(chan error, 1)
		go func() { result <- service.Run() }()
		for range 3 {
			select {
			case <-ran:
			case <-time.After(3 * time.Second):
				t.Fatal("Cron 未独立执行")
			}
		}
		if err := service.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		s.Wait()
		if unused.Load() {
			t.Fatal("代码中注册但未配置的任务不应执行")
		}
		another, err := bootstrap.NewCronScheduler(handler)
		if err != nil {
			t.Fatal(err)
		}
		service = bootstrap.CronService(another)
		if err := service.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := service.Run(); err != nil {
			t.Fatalf("关闭早于启动应视为正常退出: %v", err)
		}
	})
}

func TestCronAllJobsCommentedOut(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeCronConfig(t, "jobs:\n  # - name: job\n  #   every: 100ms\n")
		var called atomic.Bool
		s, err := bootstrap.NewCronScheduler(cronx.NamedHandler{
			Name: "job",
			Handler: func(context.Context) error {
				called.Store(true)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = s.Shutdown()
			s.Wait()
		}()
		service := bootstrap.CronService(s)
		result := make(chan error, 1)
		go func() { result <- service.Run() }()
		select {
		case err := <-result:
			t.Fatalf("零任务调度器不应自行退出: %v", err)
		case <-time.After(30 * time.Millisecond):
		}
		if err := service.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		s.Wait()
		if called.Load() {
			t.Fatal("已注释的任务不应执行")
		}
	})
}
