package natsx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestHandlerContextOwnedByClient(t *testing.T) {
	for _, mode := range []string{"core", "queue", "pull"} {
		for _, shutdown := range []string{"drain", "consumer-drain", "timeout"} {
			t.Run(mode+"/"+shutdown, func(t *testing.T) {
				closeTimeout := 2 * time.Second
				if shutdown == "timeout" {
					closeTimeout = 100 * time.Millisecond
				}
				c, err := New(Config{
					Name:         "lifecycle",
					URLs:         []string{startServer(t)},
					CloseTimeout: closeTimeout,
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				entered := make(chan context.Context, 1)
				finished := make(chan error, 1)
				release := make(chan struct{}, 1)
				defer close(release)
				handler := func(ctx context.Context) {
					entered <- ctx
					select {
					case <-ctx.Done():
					case <-release:
					}
					finished <- ctx.Err()
				}
				if mode == "core" || mode == "queue" {
					spec := CoreConsumer{
						Name:    "worker",
						Subject: "lifecycle.work",
						Handler: func(ctx context.Context, _ *nats.Msg) error {
							handler(ctx)
							return nil
						},
					}
					if mode == "queue" {
						spec.Queue = "workers"
					}
					err = c.RegisterCoreConsumers(context.Background(), spec)
				} else {
					// 管理 RPC 的超时只影响本次操作，不成为后台消费的生命周期。
					setupCtx, cancelSetup := context.WithTimeout(context.Background(), time.Second)
					defer cancelSetup()
					_, err = c.JetStream().CreateStream(setupCtx, jetstream.StreamConfig{
						Name:     "LIFECYCLE",
						Subjects: []string{"lifecycle.work"},
						Storage:  jetstream.MemoryStorage,
					})
					if err != nil {
						t.Fatal(err)
					}
					err = c.RegisterJetStreamConsumers(setupCtx, JetStreamConsumer{
						Stream: "LIFECYCLE",
						Config: jetstream.ConsumerConfig{
							Durable:   "worker",
							AckPolicy: jetstream.AckExplicitPolicy,
							AckWait:   30 * time.Second,
						},
						Handler: func(ctx context.Context, _ jetstream.Msg) error {
							handler(ctx)
							return nil
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					cancelSetup()
				}
				if err != nil {
					t.Fatal(err)
				}
				if err := c.Publish(context.Background(), "lifecycle.work", nil); err != nil {
					t.Fatal(err)
				}
				var msgCtx context.Context
				select {
				case msgCtx = <-entered:
				case <-time.After(time.Second):
					t.Fatal("消息回调未开始")
				}
				if _, ok := msgCtx.Deadline(); ok || msgCtx.Err() != nil {
					t.Fatalf("消息 ctx 不应带外部期限或取消状态: %v", msgCtx.Err())
				}
				closed := make(chan error, 1)
				go func() {
					if shutdown == "consumer-drain" {
						c.StopConsumers()
						_ = c.WaitConsumers(context.Background())
						closed <- nil
						return
					}
					closed <- c.Close()
				}()
				if shutdown != "timeout" {
					deadline := time.After(time.Second)
					for !c.closing.Load() {
						select {
						case <-deadline:
							t.Fatal("Close 未开始")
						case <-time.After(time.Millisecond):
						}
					}
					if msgCtx.Err() != nil {
						t.Error("刚开始排空就取消了消息 ctx")
					}
					release <- struct{}{}
				}
				select {
				case err := <-closed:
					if shutdown != "timeout" && err != nil || shutdown == "timeout" && !errors.Is(err, ErrCloseTimeout) {
						t.Fatalf("Close 结果不符: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Close 未按期限返回")
				}
				if shutdown == "consumer-drain" {
					if c.Conn().IsClosed() {
						t.Fatal("消费排空不应关闭共享连接")
					}
					if err := c.Publish(context.Background(), "after.drain", nil); err != nil {
						t.Fatalf("排空后连接仍须允许发布: %v", err)
					}
				}
				if !errors.Is(msgCtx.Err(), context.Canceled) {
					t.Fatalf("关闭后没有取消消息 ctx: %v", msgCtx.Err())
				}
				select {
				case err := <-finished:
					if shutdown != "timeout" && err != nil || shutdown == "timeout" && !errors.Is(err, context.Canceled) {
						t.Fatalf("回调退出状态不符: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("回调未收到关闭取消通知")
				}
			})
		}
	}
}

func TestOperationsRetainCallerContext(t *testing.T) {
	c, err := New(Config{
		Name: "operation-context",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	const subject = "operation.work"
	if err := c.Publish(ctx, subject, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Publish 未使用调用方 ctx: %v", err)
	}
	if _, err := c.Request(ctx, subject, nil, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("Request 未使用调用方 ctx: %v", err)
	}
	if _, err := c.PublishJetStream(ctx, subject, nil, "canceled-event"); !errors.Is(err, context.Canceled) {
		t.Errorf("PublishJetStream 未使用调用方 ctx: %v", err)
	}
	// 有订阅但不回复，确保 Request 等待的是调用方截止时间。
	if err := c.RegisterCoreConsumers(context.Background(), CoreConsumer{
		Name:    "worker",
		Subject: subject,
		Handler: func(context.Context, *nats.Msg) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelRequest()
	if _, err := c.Request(requestCtx, subject, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Request 未遵守调用方超时: %v", err)
	}
}

func TestCancelConsumersBeforeRegistration(t *testing.T) {
	c, err := New(Config{
		Name: "cancel-before-registration",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.RegisterStreams(context.Background(), DefaultStreamConfig("EVENTS", 1, "events")); err != nil {
		t.Fatal(err)
	}
	c.CancelConsumers()
	c.CancelConsumers()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.WaitConsumers(ctx); err != nil {
		t.Errorf("直接 CancelConsumers 后未完成排空: %v", err)
	}
	core := DefaultCoreConsumer("work", "work", func(context.Context, *nats.Msg) error { return nil })
	if err := c.RegisterCoreConsumers(context.Background(), core); !errors.Is(err, ErrClosed) {
		t.Errorf("CancelConsumers 后 Core 注册 = %v，期望 ErrClosed", err)
	}
	pull := DefaultJetStreamConsumer("EVENTS", "worker", func(context.Context, jetstream.Msg) error { return nil })
	if err := c.RegisterJetStreamConsumers(context.Background(), pull); !errors.Is(err, ErrClosed) {
		t.Errorf("CancelConsumers 后 JetStream 注册 = %v，期望 ErrClosed", err)
	}
	if c.Conn().IsClosed() {
		t.Error("CancelConsumers 不应关闭共享连接")
	}
	if err := c.Publish(context.Background(), "after.cancel", nil); err != nil {
		t.Errorf("CancelConsumers 后共享连接应仍允许发布: %v", err)
	}
}

func TestExplicitCancelAllowsCloseBeforeHandlerReturns(t *testing.T) {
	c, err := New(Config{
		Name: "cancel-close",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer func() {
		close(release)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := c.WaitConsumers(ctx); err != nil {
			t.Error(err)
		}
		_ = c.Close()
	}()
	entered := make(chan context.Context, 1)
	if err := c.RegisterCoreConsumers(context.Background(), CoreConsumer{
		Name:    "worker",
		Subject: "work",
		Handler: func(ctx context.Context, _ *nats.Msg) error {
			entered <- ctx
			<-release
			return ctx.Err()
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(context.Background(), "work", nil); err != nil {
		t.Fatal(err)
	}
	var handlerCtx context.Context
	select {
	case handlerCtx = <-entered:
	case <-time.After(time.Second):
		t.Fatal("回调未启动")
	}
	c.StopConsumers()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := c.WaitConsumers(ctx); !errors.Is(err, context.DeadlineExceeded) || handlerCtx.Err() != nil {
		t.Fatalf("Wait 只限制等待，不应提前取消业务: %v / %v", err, handlerCtx.Err())
	}
	c.CancelConsumers()
	if !errors.Is(handlerCtx.Err(), context.Canceled) {
		t.Fatal("Cancel 未取消消费 context")
	}
	closed := make(chan error, 2)
	for range 2 {
		go func() { closed <- c.Close() }()
	}
	for range 2 {
		select {
		case err := <-closed:
			if !errors.Is(err, ErrCloseTimeout) || !c.Conn().IsClosed() {
				t.Fatalf("取消后未强制回收共享连接: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("取消后 Close 仍在等待未返回的回调")
		}
	}
}
