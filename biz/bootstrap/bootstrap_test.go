package bootstrap_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestMustInitRejectsStoppedContext(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelExpired := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelExpired()

	for _, tc := range []struct {
		name string
		ctx  context.Context
		want error
	}{
		{name: "canceled", ctx: canceled, want: context.Canceled},
		{name: "expired", ctx: expired, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var resources lifecycle.Stack
			defer func() {
				value := recover()
				err, ok := value.(error)
				if !ok || !errors.Is(err, tc.want) {
					t.Fatalf("启动错误应保留 context 错误链: got %v, want %v", value, tc.want)
				}
			}()
			bootstrap.MustInit(tc.ctx, &httpconfig.AppConfig{}, &resources)
		})
	}
}

func TestMustInitRejectsMissingArguments(t *testing.T) {
	for _, name := range []string{"context", "config", "resources"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := &httpconfig.AppConfig{}
			resources := &lifecycle.Stack{}
			switch name {
			case "context":
				ctx = nil
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
			bootstrap.MustInit(ctx, cfg, resources)
		})
	}
}
