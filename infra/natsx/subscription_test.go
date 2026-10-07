package natsx

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
)

func TestCoreSubscriptionDeliveryModes(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name: "subscriptions",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	var received [4]atomic.Int64
	for i := range received {
		handler := func(ctx context.Context, msg *nats.Msg) {
			if id, ok := logit.LogIDFromContext(ctx); !ok || id != "subscription-trace" {
				t.Errorf("订阅回调未还原追踪 ID: %q", id)
			}
			if string(msg.Data) != "payload" {
				t.Errorf("消息正文错误: %q", msg.Data)
			}
			received[i].Add(1)
		}
		if i < 2 {
			_, err = c.Subscribe("subscription.work", handler)
		} else {
			_, err = c.QueueSubscribe("subscription.work", "workers", handler)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	ctx := logit.WithContext(context.Background())
	logit.AddMeta(ctx, logit.Str(logit.LogId, "subscription-trace"))
	const messages = 20
	for range messages {
		if err := c.Publish(ctx, "subscription.work", []byte("payload")); err != nil {
			t.Fatal(err)
		}
	}
	// 关闭需排空两种订阅，返回后才能检查全部已投递消息。
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		if got := received[i].Load(); got != messages {
			t.Errorf("普通订阅 %d 收到 %d 条，期望 %d 条", i, got, messages)
		}
	}
	if got := received[2].Load() + received[3].Load(); got != messages {
		t.Errorf("同一队列组共收到 %d 条，期望 %d 条", got, messages)
	}
	handler := func(context.Context, *nats.Msg) {}
	if _, err := c.Subscribe("subscription.new", handler); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后普通订阅应被拒绝: %v", err)
	}
	if _, err := c.QueueSubscribe("subscription.new", "workers", handler); !errors.Is(err, ErrClosed) {
		t.Errorf("关闭后队列订阅应被拒绝: %v", err)
	}
}

func TestQueueSubscribeRejectsEmptyQueue(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name: "invalid-queue",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, queue := range []string{"", " \t\n"} {
		if sub, err := c.QueueSubscribe("subscription.work", queue, func(context.Context, *nats.Msg) {}); err == nil || sub != nil {
			t.Fatalf("空队列名 %q 不应退化为普通订阅: sub=%v err=%v", queue, sub, err)
		}
	}
	if got := c.Conn().NumSubscriptions(); got != 0 {
		t.Fatalf("无效队列名不应创建订阅: %d", got)
	}
}
