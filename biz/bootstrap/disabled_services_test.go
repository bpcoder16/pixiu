package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/gin-gonic/gin"
)

func TestStartFiltersNamedServices(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "disabledServices: [http:aaa, cron, websocket:app]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustRegisterWebSocket("app2", func() (*realtime.Server, error) {
			return realtime.NewServer(exampleRealtimeConfig())
		})
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)

		if _, err := bootstrap.NewHTTPService(http.NotFoundHandler()); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("http:aaa 不应禁用默认 HTTP 入口: %v", err)
		}
		if _, err := bootstrap.NewWebSocketService("app"); !errors.Is(err, bootstrap.ErrServiceDisabled) {
			t.Fatalf("禁用的 WebSocket 实例应在读取专属文件前跳过: %v", err)
		}
		addr := wsAddress(t)
		writeWSConfig(t, "app2", addr)
		ws, err := bootstrap.NewWebSocketService("app2")
		if err != nil {
			t.Fatal(err)
		}
		runWSService(t, ws, addr)

		ran := make(chan string, 3)
		stopped := make(chan string, 3)
		runErr := errors.New("active service exited")
		var factories []bootstrap.ServiceFactory
		for _, name := range []string{"http:aaa", "cron:jobs", "http:aaa2"} {
			factories = append(factories, func() (apprun.Service, error) {
				return apprun.Service{
					Name: name,
					Run: func() error {
						ran <- name
						return runErr
					},
					Stop: func() error {
						stopped <- name
						return nil
					},
					Wait:   func(context.Context) error { return nil },
					Cancel: func() {},
				}, nil
			})
		}
		if err := bootstrap.Start(factories...); !errors.Is(err, runErr) {
			t.Fatalf("应启动未禁用的服务: %v", err)
		}
		if len(ran) != 1 || <-ran != "http:aaa2" {
			t.Fatal("仅 http:aaa2 应进入 Run")
		}
		stopCount := make(map[string]int)
		close(stopped)
		for name := range stopped {
			stopCount[name]++
		}
		for _, name := range []string{"http:aaa", "cron:jobs", "http:aaa2"} {
			if stopCount[name] != 1 {
				t.Fatalf("已构造服务 %s 应回收一次，实际 %d", name, stopCount[name])
			}
		}
	})
}

func TestStartSkipsDisabledServiceTypes(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "disabledServices: [http, websocket]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		for _, name := range []string{"app", "admin"} {
			bootstrap.MustRegisterWebSocket(name, func() (*realtime.Server, error) {
				return realtime.NewServer(exampleRealtimeConfig())
			})
		}
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
		clear(cfg.DisabledServices) // 调用方修改配置不能重新启用服务。
		factories := []bootstrap.ServiceFactory{
			func() (apprun.Service, error) {
				return bootstrap.NewHTTPService(http.NotFoundHandler())
			},
			func() (apprun.Service, error) {
				svc, err := bootstrap.NewGinHTTPService(func(*gin.Engine) { t.Fatal("禁用 HTTP 时不应注册路由") })
				if err != nil {
					return svc, fmt.Errorf("Gin: %w", err)
				}
				return svc, nil
			},
		}
		for _, name := range []string{"app", "admin"} {
			factories = append(factories, func() (apprun.Service, error) {
				return bootstrap.NewWebSocketService(name)
			})
		}
		if err := bootstrap.Start(factories...); !errors.Is(err, apprun.ErrNoServices) {
			t.Fatalf("未提供禁用服务的配置时应跳过所有服务，得到 %v", err)
		}
		for _, name := range []string{"app", "admin"} {
			if realtime.Named(name).Ready() {
				t.Fatalf("WebSocket %q 应已登记但未启动", name)
			}
		}

		factories = append(factories, func() (apprun.Service, error) {
			return bootstrap.NewCronService()
		})
		err := bootstrap.StartWithConfig(bootstrap.StartConfig{ShutdownTimeout: 5 * time.Second}, factories...)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("启用的 Cron 应报告缺失配置，得到 %v", err)
		}
		failure := errors.New("factory cleanup failed")
		err = bootstrap.Start(func() (apprun.Service, error) {
			svc, err := bootstrap.NewHTTPService(http.NotFoundHandler())
			return svc, errors.Join(err, failure)
		})
		if !errors.Is(err, failure) || !errors.Is(err, bootstrap.ErrServiceDisabled) {
			t.Fatalf("禁用服务时应保留工厂的回收错误，得到 %v", err)
		}
	})
}

func TestStartRunsHTTPWithCronDisabled(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "log:\n  format: text\n  hookStd: false\ndisabledServices: [cron]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		// 占住地址，让 HTTP 的 Run 返回监听错误，证明跳过 Cron 后继续运行了 HTTP。
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		writeHTTPConfig(t, "addr: "+ln.Addr().String()+"\n")
		err = bootstrap.Start(
			func() (apprun.Service, error) { return bootstrap.NewCronService() },
			func() (apprun.Service, error) { return bootstrap.NewHTTPService(http.NotFoundHandler()) },
		)
		if !errors.Is(err, syscall.EADDRINUSE) {
			t.Fatalf("应运行 HTTP 并保留监听错误，得到 %v", err)
		}
	})
}

func TestStartHandlesJoinedDisabledErrors(t *testing.T) {
	disabled := bootstrap.ErrServiceDisabled
	nextErr := errors.New("next factory called")
	mixed := fmt.Errorf("setup: %w", errors.Join(disabled, errors.Join(disabled, errors.New("factory cleanup failed"))))
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"禁用且回收成功", errors.Join(disabled, nil), nextErr},
		{"嵌套合并且全部禁用", fmt.Errorf("setup: %w", errors.Join(disabled, errors.Join(disabled, disabled))), nextErr},
		{"嵌套合并含回收失败", mixed, mixed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := bootstrap.Start(
				func() (apprun.Service, error) { return apprun.Service{}, tc.err },
				func() (apprun.Service, error) { return apprun.Service{}, nextErr },
			)
			if got != tc.want {
				t.Fatalf("Start() = %v，期望 %v", got, tc.want)
			}
		})
	}
}
