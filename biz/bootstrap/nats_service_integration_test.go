//go:build natsintegration

package bootstrap

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type natsRegistrationWriteFunc func([]byte) (int, error)

func (f natsRegistrationWriteFunc) Write(p []byte) (int, error) { return f(p) }

func testNewNATSConsumerServiceConstructionErrors(t *testing.T) {
	isolatedNATS(t, func() {
		client := registrationClients(t, false, "config")[0]
		if _, err := NewNATSConsumerService("bad.name", nil, nil); err == nil || !strings.Contains(err.Error(), "invalid NATS instance name") {
			t.Fatalf("应返回非法实例名错误: %v", err)
		}
		service, err := NewNATSConsumerService("config", nil, nil)
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("应保留配置文件错误链: %v", err)
		}
		if service.Name != "" || service.Run != nil || service.Stop != nil || service.Wait != nil || service.Cancel != nil {
			t.Fatal("构造失败应返回空服务")
		}
		writeNATSRegistration(t, "config", "streamDefaults: {replicas: 1}\ncoreConsumers:\n  - name: work\n    subject: work\n    handler: missing\n")
		if _, err := NewNATSConsumerService("config", nil, nil); err == nil || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("应返回 Handler 绑定错误: %v", err)
		}
		if client.Conn().NumSubscriptions() != 0 || client.Conn().IsClosed() {
			t.Fatal("构造失败不应启动订阅或关闭共享连接")
		}
	})
}

func testNewNATSConsumerServiceSlowRegistrationLog(t *testing.T) {
	isolatedNATS(t, func() {
		client := registrationClients(t, false, "slow-log")[0]
		writeNATSRegistration(t, "slow-log", "streamDefaults: {replicas: 1}\ncoreConsumers:\n  - name: work\n    subject: work\n    handler: work\n")
		handlers := &natsx.CoreHandlerRegistry{}
		handlers.MustRegister(func(context.Context, *nats.Msg) error { return nil }, "work")
		service, err := NewNATSConsumerService("slow-log", nil, handlers)
		if err != nil {
			t.Fatal(err)
		}
		entered, release := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		logger := logit.MustNew(logit.OptWriter(logit.NewWriter(natsRegistrationWriteFunc(func(p []byte) (int, error) {
			if strings.Contains(string(p), "coreSubscribe") {
				close(entered)
				<-release
			}
			return len(p), nil
		}))))
		previous := logit.Default()
		logit.SetDefault(logger)
		defer func() {
			unblock()
			_ = stopAndWait(service.Stop, service.Wait, service.Cancel)
			logit.SetDefault(previous)
			_ = logit.Close(logger)
		}()
		manager, err := apprun.New(50 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if err := manager.Register(service); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- manager.Run(ctx) }()
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("注册日志未开始")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, apprun.ErrShutdownTimeout) {
				t.Errorf("慢日志未结束应返回关闭超时: %v", err)
			}
		case <-time.After(time.Second):
			unblock()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("释放日志后管理器仍未退出")
			}
			t.Fatal("慢注册日志阻塞了超时取消，管理器未按关闭预算返回")
		}
		waitCtx, cancelWait := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancelWait()
		if err := service.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("管理器退出不能代替注册日志完成: %v", err)
		}
		unblock()
		waitCtx, finish := context.WithTimeout(context.Background(), time.Second)
		defer finish()
		if err := service.Wait(waitCtx); err != nil {
			t.Fatal("释放日志后未完成排空", err)
		}
		if client.Conn().IsClosed() {
			t.Error("服务取消不应提前关闭共享连接")
		}
	})
}

func testNewNATSConsumerServiceDirectCancel(t *testing.T) {
	for _, mode := range []string{"before_run", "running"} {
		t.Run(mode, func(t *testing.T) {
			isolatedNATS(t, func() {
				client := registrationClients(t, false, "cancel")[0]
				entered := make(chan context.Context, 1)
				release := make(chan struct{})
				finish := sync.OnceFunc(func() { close(release) })
				defer finish()
				handlers := &natsx.CoreHandlerRegistry{}
				handlers.MustRegister(func(ctx context.Context, _ *nats.Msg) error {
					entered <- ctx
					<-release
					return ctx.Err()
				}, "work")
				writeNATSRegistration(t, "cancel", "streamDefaults: {replicas: 1}\ncoreConsumers:\n  - name: work\n    subject: work\n    handler: work\n")
				service, err := NewNATSConsumerService("cancel", nil, handlers)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
						t.Error(err)
					}
				})
				ran := make(chan error, 1)
				var handlerCtx context.Context
				if mode == "running" {
					go func() { ran <- service.Run() }()
					deadline := time.After(3 * time.Second)
					for client.Conn().NumSubscriptions() == 0 {
						select {
						case <-deadline:
							t.Fatal("消费者未启动")
						case <-time.After(time.Millisecond):
						}
					}
					if err := client.Publish(context.Background(), "work", nil); err != nil {
						t.Fatal(err)
					}
					select {
					case handlerCtx = <-entered:
					case <-time.After(time.Second):
						t.Fatal("回调未启动")
					}
				}
				service.Cancel()
				service.Cancel()
				if mode == "running" {
					if !errors.Is(handlerCtx.Err(), context.Canceled) {
						t.Errorf("Cancel 未取消业务 context: %v", handlerCtx.Err())
					}
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
					defer cancel()
					if err := service.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
						t.Errorf("回调未返回时 Wait = %v，期望超时", err)
					}
					finish()
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := service.Wait(ctx); err != nil {
					t.Errorf("直接 Cancel 后 Wait 未完成: %v", err)
				}
				if mode == "before_run" {
					go func() { ran <- service.Run() }()
				}
				select {
				case err := <-ran:
					if err != nil {
						t.Errorf("Cancel 后 Run = %v", err)
					}
				case <-time.After(time.Second):
					t.Error("Cancel 后 Run 未返回")
				}
				if client.Conn().NumSubscriptions() != 0 || client.Conn().IsClosed() {
					t.Error("Cancel 后应停止订阅并保留共享连接")
				}
				if err := client.Publish(context.Background(), "after.cancel", nil); err != nil {
					t.Errorf("Cancel 后共享连接应仍允许发布: %v", err)
				}
			})
		})
	}
}

func testNewNATSConsumerService(t *testing.T) {
	for _, mode := range []string{"normal", "timeout", "factory_failure", "partial_start"} {
		t.Run(mode, func(t *testing.T) {
			isolatedNATS(t, func() {
				var wantCloseErr error
				if mode == "timeout" {
					wantCloseErr = natsx.ErrCloseTimeout
				}
				client := registrationClientsWithCloseError(t, true, wantCloseErr, "service")[0]
				entered, release := make(chan struct{}), make(chan struct{})
				finished := make(chan error, 1)
				handler := func(ctx context.Context, _ *nats.Msg) error {
					close(entered)
					<-release
					err := ctx.Err()
					if err == nil {
						err = natsx.Named("service").Publish(ctx, "completed", nil)
					}
					finished <- err
					return err
				}
				config := "streamDefaults: {replicas: 1}\ncoreConsumers:\n  - name: work\n    subject: work\n    handler: work\n"
				if mode == "partial_start" {
					// JetStream 已启动，再由 Core 的通用参数校验触发启动失败。
					config = "streamDefaults: {replicas: 1}\nstreams:\n  - name: EVENTS\n    subjects: [events]\n    consumers:\n      - durable: worker\n        handler: task\ncoreConsumers:\n  - name: work\n    subject: ''\n    handler: work\n"
				}
				writeNATSRegistration(t, "service", config)
				jsHandlers := &natsx.JetStreamHandlerRegistry{}
				jsHandlers.MustRegister(func(context.Context, jetstream.Msg) error { return nil }, "task")
				coreHandlers := &natsx.CoreHandlerRegistry{}
				coreHandlers.MustRegister(handler, "work")
				service, err := NewNATSConsumerService("service", jsHandlers, coreHandlers)
				if err != nil {
					t.Fatal(err)
				}
				if service.Name != "nats:service" || client.Conn().NumSubscriptions() != 0 || client.Conn().IsClosed() {
					t.Fatal("构造应返回命名服务，不启动订阅或关闭共享连接")
				}
				if mode == "factory_failure" {
					constructionErr := errors.New("later factory failed")
					err := Start(func() (apprun.Service, error) { return service, nil }, func() (apprun.Service, error) { return apprun.Service{}, constructionErr })
					if !errors.Is(err, constructionErr) {
						t.Fatalf("构造失败原因丢失: %v", err)
					}
					if err := service.Run(); err != nil || client.Conn().NumSubscriptions() != 0 || client.Conn().IsClosed() {
						t.Fatalf("Stop 早于 Run 后不得启动消费或关闭共享连接: %v", err)
					}
					return
				}
				timeout := time.Second
				if mode == "timeout" {
					timeout = 50 * time.Millisecond
				}
				manager, err := apprun.New(timeout)
				if err != nil {
					t.Fatal(err)
				}
				shutdownStarted, shutdownFinished := make(chan struct{}), make(chan struct{})
				stop := service.Stop
				service.Stop = func() error {
					close(shutdownStarted)
					defer close(shutdownFinished)
					return stop()
				}
				if err := manager.Register(service); err != nil {
					t.Fatal(err)
				}
				if mode == "partial_start" {
					err := manager.Run(context.Background())
					if err == nil || !strings.Contains(err.Error(), "register Core consumers") {
						t.Fatalf("部分注册失败应通过 Run 返回并收尾: %v", err)
					}
					select {
					case <-shutdownFinished:
					default:
						t.Fatal("失败后未完成消费排空")
					}
					if client.Conn().IsClosed() {
						t.Fatal("启动失败收尾不应提前关闭共享连接")
					}
					return
				}
				// 各服务并行停收，其 Stop 阶段仍可使用共享连接。
				producerDone := make(chan struct{})
				producerErr := errors.New("producer shutdown failed")
				if err := manager.Register(apprun.Service{
					Name: "producer",
					Run: func() error {
						<-producerDone
						return nil
					},
					Stop: func() error {
						defer close(producerDone)
						return errors.Join(producerErr, client.Publish(context.Background(), "producer.stopped", nil))
					},
					Wait:   func(context.Context) error { return nil },
					Cancel: func() {},
				}); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() { done <- manager.Run(ctx) }()
				deadline := time.After(3 * time.Second)
				for client.Conn().NumSubscriptions() == 0 {
					select {
					case <-deadline:
						t.Fatal("消费者未启动")
					case <-time.After(time.Millisecond):
					}
				}
				if err := client.Publish(context.Background(), "work", nil); err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("回调未开始")
				}
				cancel()
				<-shutdownStarted
				if mode == "normal" {
					close(release)
				}
				var runErr error
				select {
				case runErr = <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("服务退出未遵守预算")
				}
				if !errors.Is(runErr, producerErr) {
					t.Fatalf("普通关闭错误丢失: %v", runErr)
				}
				var resources lifecycle.Stack
				dependencyClosed := false
				_ = resources.Register(func() error {
					dependencyClosed = true
					return nil
				})
				_ = resources.Register(natsx.CloseAll)
				closeErr := resources.Close()
				if !dependencyClosed || !client.Conn().IsClosed() {
					t.Fatal("正常排空与超时路径都必须回收资源")
				}
				if mode == "timeout" {
					if !errors.Is(runErr, apprun.ErrShutdownTimeout) || !errors.Is(closeErr, natsx.ErrCloseTimeout) {
						t.Fatalf("未保留排空和强制关闭结果: %v / %v", runErr, closeErr)
					}
					close(release)
				} else if closeErr != nil {
					t.Fatal(closeErr)
				}
				select {
				case <-shutdownFinished:
				case <-time.After(time.Second):
					t.Fatal("释放回调后排空未完成")
				}
				if err := <-finished; mode == "normal" && err != nil || mode == "timeout" && !errors.Is(err, context.Canceled) {
					t.Fatalf("消费排空期间查询或发布失败: %v", err)
				}
			})
		})
	}
}

// stopAndWait 仅用于测试清理；生产调用方显式传递统一的排空期限。
func stopAndWait(stop func() error, wait func(context.Context) error, cancel func()) error {
	err := stop()
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	waitErr := wait(ctx)
	if waitErr != nil {
		cancel()
	}
	return errors.Join(err, waitErr)
}
