package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestStartFiltersNamedServices(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "disabledServices: [http:aaa, cron, nats-consumer:orders]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)

		names := []string{"http:aaa", "cron:jobs", "nats-consumer:orders", "http:aaa2"}
		ran := make(chan string, len(names))
		stopped := make(chan string, len(names))
		runErr := errors.New("active service exited")
		var factories []bootstrap.ServiceFactory
		for _, name := range names {
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
		for _, name := range names {
			if stopCount[name] != 1 {
				t.Fatalf("已构造服务 %s 应回收一次，实际 %d", name, stopCount[name])
			}
		}
	})
}

func TestStartSkipsDisabledServiceTypes(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "disabledServices: [http]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
		clear(cfg.DisabledServices) // 调用方修改配置不能重新启用服务。
		httpService := func() (apprun.Service, error) {
			return apprun.Service{Name: "http"}, nil
		}
		if err := bootstrap.Start(httpService); !errors.Is(err, apprun.ErrNoServices) {
			t.Fatalf("已禁用的服务应跳过，得到 %v", err)
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
