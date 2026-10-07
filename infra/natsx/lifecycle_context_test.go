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
		for _, shutdown := range []string{"drain", "timeout"} {
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
					coreHandler := func(ctx context.Context, _ *nats.Msg) {
						handler(ctx)
					}
					if mode == "queue" {
						_, err = c.QueueSubscribe("lifecycle.work", "workers", coreHandler)
					} else {
						_, err = c.Subscribe("lifecycle.work", coreHandler)
					}
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
					consumer, err := c.JetStream().CreateConsumer(setupCtx, "LIFECYCLE", jetstream.ConsumerConfig{
						Durable:   "worker",
						AckPolicy: jetstream.AckExplicitPolicy,
					})
					if err != nil {
						t.Fatal(err)
					}
					cancelSetup()
					err = c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(ctx context.Context, _ jetstream.Msg) {
						handler(ctx)
					})
					if err != nil {
						t.Fatal(err)
					}
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
				go func() { closed <- c.Close() }()
				if shutdown == "drain" {
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
					if shutdown == "drain" && err != nil || shutdown == "timeout" && !errors.Is(err, ErrCloseTimeout) {
						t.Fatalf("Close 结果不符: %v", err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("Close 未按期限返回")
				}
				if !errors.Is(msgCtx.Err(), context.Canceled) {
					t.Fatalf("关闭后没有取消消息 ctx: %v", msgCtx.Err())
				}
				select {
				case err := <-finished:
					if shutdown == "drain" && err != nil || shutdown == "timeout" && !errors.Is(err, context.Canceled) {
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
	if _, err := c.Subscribe(subject, func(context.Context, *nats.Msg) {}); err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	requestCtx, cancelRequest := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelRequest()
	if _, err := c.Request(requestCtx, subject, nil, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Request 未遵守调用方超时: %v", err)
	}
}
