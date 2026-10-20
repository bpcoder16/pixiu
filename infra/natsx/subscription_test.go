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
	var consumers []CoreConsumer
	for i := range received {
		spec := CoreConsumer{
			Subject: "subscription.work",
		}
		spec.Handler = func(ctx context.Context, msg *nats.Msg) error {
			if id, ok := logit.LogIDFromContext(ctx); !ok || id != "subscription-trace" {
				t.Errorf("订阅回调未还原追踪 ID: %q", id)
			}
			if string(msg.Data) != "payload" {
				t.Errorf("消息正文错误: %q", msg.Data)
			}
			received[i].Add(1)
			return nil
		}
		if i >= 2 {
			spec.Queue = "workers"
		}
		consumers = append(consumers, spec)
	}
	if err := c.RegisterCoreConsumers(context.Background(), consumers...); err != nil {
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
	for _, queue := range []string{"", "workers"} {
		if err := c.RegisterCoreConsumers(context.Background(), CoreConsumer{
			Subject: "subscription.new",
			Queue:   queue,
			Handler: func(context.Context, *nats.Msg) error { return nil },
		}); !errors.Is(err, ErrClosed) {
			t.Errorf("关闭后订阅应被拒绝，queue=%q: %v", queue, err)
		}
	}
}

func TestCoreRegistrationRejectsInvalidDeclarations(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name: "invalid-queue",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	valid := CoreConsumer{
		Subject: "subscription.work",
		Handler: func(context.Context, *nats.Msg) error { return nil },
	}
	for _, spec := range []CoreConsumer{
		{
			Subject: "subscription.work",
			Queue:   " \t\n",
			Handler: valid.Handler,
		},
		{
			Subject: "",
			Handler: valid.Handler,
		},
		{
			Subject: " \t\n",
			Handler: valid.Handler,
		},
		{
			Subject: "subscription.work",
		},
	} {
		if err := c.RegisterCoreConsumers(context.Background(), valid, spec); err == nil {
			t.Fatalf("无效声明应在创建本批订阅前拒绝: %+v", spec)
		}
		if got := c.Conn().NumSubscriptions(); got != 0 {
			t.Fatalf("无效声明不应创建订阅: %d", got)
		}
	}
}
