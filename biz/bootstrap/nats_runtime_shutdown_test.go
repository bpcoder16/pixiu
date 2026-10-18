package bootstrap_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go"
)

func TestNATSMixedSignalShutdownOrder(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		writeCronConfig(t, "jobs:\n  - name: tick\n    every: 100ms\n")
		c := runtimeClient(t, "")
		peer, err := natsx.New(natsx.Config{
			Name: "peer",
			URLs: []string{c.Conn().ConnectedUrl()},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		enteredJS, enteredCore, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		var cronStopped, httpStopped atomic.Bool
		var ticks atomic.Int32
		js, err := natsx.NewJetStreamRuntime(ctx, c, bootstrapRegistry(func(ctx context.Context, _ natsx.Message) error {
			close(enteredJS)
			<-release
			if ctx.Err() != nil || !cronStopped.Load() || !httpStopped.Load() {
				t.Error("JS 排空顺序或 context 错误")
			}
			return peer.Publish(ctx, "after-drain", nil)
		}), natsx.JetStreamRuntimeConfig{ShutdownTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		core, err := natsx.NewCoreRuntime(c, natsx.MustNewCoreRegistry(natsx.CoreSubscriptionSpec{Name: "request", Subject: "request", Handler: func(ctx context.Context, msg *nats.Msg) error {
			close(enteredCore)
			<-release
			if ctx.Err() != nil || !cronStopped.Load() || !httpStopped.Load() {
				t.Error("Core 排空顺序或 context 错误")
			}
			return msg.Respond([]byte("ok"))
		}}), natsx.CoreRuntimeConfig{ShutdownTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		web, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		webStop := web.Shutdown
		web.Shutdown = func() error { err := webStop(); httpStopped.Store(true); return err }
		cron, err := bootstrap.NewCronScheduler(bootstrap.CronHandler{
			Name:    "tick",
			Handler: func(context.Context) error { ticks.Add(1); return nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		cronService := bootstrap.CronService(cron)
		cronStop := cronService.Shutdown
		cronService.Shutdown = func() error { err := cronStop(); cronStopped.Store(true); return err }
		coreService := bootstrap.NATSCoreService(core)
		coreStop := coreService.Shutdown
		coreService.Shutdown = func() error { releaseOnce.Do(func() { close(release) }); return coreStop() }
		requestResult := make(chan error, 1)
		driver := make(chan struct{})
		go func() {
			defer close(driver)
			if err := core.WaitReady(ctx); err != nil {
				requestResult <- err
				return
			}
			go func() {
				msg, err := peer.Conn().RequestWithContext(ctx, "request", nil)
				if err == nil && string(msg.Data) != "ok" {
					err = errors.New("bad reply")
				}
				requestResult <- err
			}()
			if _, err := c.JetStream().Publish(ctx, "bootstrap.work", nil); err != nil {
				t.Error(err)
				return
			}
			select {
			case <-enteredJS:
			case <-ctx.Done():
				t.Error(ctx.Err())
				return
			}
			select {
			case <-enteredCore:
			case <-ctx.Done():
				t.Error(ctx.Err())
				return
			}
			for ticks.Load() == 0 {
				select {
				case <-time.After(time.Millisecond):
				case <-ctx.Done():
					t.Error(ctx.Err())
					return
				}
			}
			if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
				t.Error(err)
			}
		}()
		err = bootstrap.StartWithConfig(bootstrap.StartConfig{ShutdownTimeout: 5 * time.Second},
			func() (apprun.Service, error) { return bootstrap.NATSJetStreamService(js), nil }, func() (apprun.Service, error) { return coreService, nil }, func() (apprun.Service, error) { return web, nil }, func() (apprun.Service, error) { return cronService, nil })
		if err != nil {
			t.Fatal(err)
		}
		waitRuntime(t, driver)
		waitRuntime(t, js.Done())
		waitRuntime(t, core.Done())
		if err := <-requestResult; err != nil {
			t.Fatal("排空中的回复失败", err)
		}
		if c.Conn().IsClosed() || peer.Conn().IsClosed() {
			t.Fatal("服务关闭越过 Client 生命周期")
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNATSServiceRuntimeAndApplicationBudgets(t *testing.T) {
	for _, timeout := range []time.Duration{
		40 * time.Millisecond,
		6 * time.Second,
	} {
		t.Run(timeout.String(), func(t *testing.T) {
			c := runtimeClient(t, "")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			r, err := natsx.NewJetStreamRuntime(context.Background(), c, bootstrapRegistry(func(context.Context, natsx.Message) error { close(entered); <-release; return nil }), natsx.JetStreamRuntimeConfig{ShutdownTimeout: timeout})
			if err != nil {
				t.Fatal(err)
			}
			manager, err := bootstrap.NewAppManager(5 * time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if err := manager.Register(bootstrap.NATSJetStreamService(r)); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() { result <- manager.Run(ctx) }()
			if _, err := c.JetStream().Publish(context.Background(), "bootstrap.work", nil); err != nil {
				t.Fatal(err)
			}
			waitRuntime(t, entered)
			cancel()
			err = <-result
			want := natsx.ErrRuntimeShutdownTimeout
			if timeout > time.Second {
				want = apprun.ErrShutdownTimeout
				if errors.Is(err, natsx.ErrRuntimeShutdownTimeout) {
					t.Error("报告尚未触发的 Runtime 超时")
				}
			}
			if !errors.Is(err, want) {
				t.Fatal(err)
			}
			select {
			case <-r.Done():
				t.Fatal("阻塞业务尚未退出就 Done")
			default:
			}
			once.Do(func() { close(release) })
			_ = r.Shutdown()
			waitRuntime(t, r.Done())
		})
	}
}
