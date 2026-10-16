package bootstrap_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/cronx"
)

func TestCronMixedShutdownOrder(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		writeCronConfig(t, "drainTimeout: 1s\nshutdownTimeout: 2s\njobs:\n  - name: job\n    every: 100ms\n")
		entered := make(chan struct{}, 1)
		var running atomic.Int32
		scheduler, err := bootstrap.NewCronScheduler(bootstrap.CronHandler{
			Name: "job",
			Handler: func(ctx context.Context) error {
				running.Add(1)
				defer running.Add(-1)
				select {
				case entered <- struct{}{}:
				default:
				}
				time.Sleep(80 * time.Millisecond)
				if ctx.Err() != nil {
					t.Error("正常排空不应取消任务")
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer scheduler.Shutdown()
		service, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		defer service.Shutdown()
		shutdown := service.Shutdown
		service.Shutdown = func() error {
			if running.Load() != 0 {
				t.Error("必须先排空 Cron 再关 HTTP")
			}
			return shutdown()
		}
		manager, err := bootstrap.NewAppManager(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(service); err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(bootstrap.CronService(scheduler)); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- manager.Run(ctx) }()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("未启动混合任务")
		}
		cancel()
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if err := scheduler.Run(); !errors.Is(err, cronx.ErrClosed) {
			t.Fatalf("Cron 未关闭: %v", err)
		}
	})
}

func TestCronMixedHTTPFailureClosesScheduler(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer occupied.Close()
		writeHTTPConfig(t, "addr: "+occupied.Addr().String()+"\n")
		writeCronConfig(t, "{}\n")
		scheduler, err := bootstrap.NewCronScheduler()
		if err != nil {
			t.Fatal(err)
		}
		defer scheduler.Shutdown()
		httpService, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		defer httpService.Shutdown()
		manager, err := bootstrap.NewAppManager(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(httpService); err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(bootstrap.CronService(scheduler)); err != nil {
			t.Fatal(err)
		}
		var listenErr *net.OpError
		if err := manager.Run(context.Background()); !errors.As(err, &listenErr) {
			t.Fatalf("应保留监听失败: %v", err)
		}
		if err := scheduler.Run(); !errors.Is(err, cronx.ErrClosed) {
			t.Fatalf("启动失败未回收 Cron: %v", err)
		}
	})
}
