package bootstrap_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/cronx"
)

func TestCronMixedStopsHTTPBeforeCronDrains(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		writeCronConfig(t, "jobs:\n  - name: job\n    every: 100ms\n")
		entered := make(chan struct{}, 1)
		release := make(chan struct{})
		finish := sync.OnceFunc(func() { close(release) })
		defer finish()
		var running atomic.Int32
		cronService, err := bootstrap.NewCronService(cronx.NamedHandler{
			Name: "job",
			Handler: func(ctx context.Context) error {
				running.Add(1)
				defer running.Add(-1)
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
				if ctx.Err() != nil {
					t.Error("正常排空不应取消任务")
				}
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer stopAndWait(cronService.Stop, cronService.Wait, cronService.Cancel)
		service, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		defer stopAndWait(service.Stop, service.Wait, service.Cancel)
		shutdown := service.Stop
		service.Stop = func() error {
			if running.Load() == 0 {
				t.Error("HTTP 停收不应等待 Cron 排空")
			}
			defer finish()
			return shutdown()
		}
		manager, err := bootstrap.NewAppManager(5 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(service); err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(cronService); err != nil {
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
		if err := cronService.Run(); err != nil {
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
		var cronService apprun.Service
		err = bootstrap.Start(
			func() (apprun.Service, error) {
				return bootstrap.NewHTTPService(http.NewServeMux())
			},
			func() (apprun.Service, error) {
				var err error
				cronService, err = bootstrap.NewCronService()
				return cronService, err
			},
		)
		var listenErr *net.OpError
		if !errors.As(err, &listenErr) {
			t.Fatalf("应保留监听失败: %v", err)
		}
		if cronService.Run == nil {
			t.Fatal("未构造 Cron 服务")
		}
		defer stopAndWait(cronService.Stop, cronService.Wait, cronService.Cancel)
		result := make(chan error, 1)
		go func() { result <- cronService.Run() }()
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("启动失败未回收 Cron: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("启动失败后 Cron 仍可启动")
		}
	})
}
