package natsx

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		handler := func(ctx context.Context, msg *nats.Msg) error {
			if id, ok := logit.LogIDFromContext(ctx); !ok || id != "subscription-trace" {
				t.Errorf("订阅回调未还原追踪 ID: %q", id)
			}
			if string(msg.Data) != "payload" {
				t.Errorf("消息正文错误: %q", msg.Data)
			}
			received[i].Add(1)
			return nil
		}
		var spec CoreConsumer
		if i >= 2 {
			spec = DefaultCoreQueueConsumer("subscription.work", "workers", 1, handler)
		} else {
			spec = DefaultCoreConsumer("subscription.work", handler)
		}
		if spec.Subscribers != 1 || spec.PendingMessages != 0 || spec.PendingBytes != 0 {
			t.Fatalf("Core 默认值错误: %+v", spec)
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
		DefaultCoreQueueConsumer("subscription.work", "workers", -1, valid.Handler),
		{
			Subject:     "subscription.work",
			Subscribers: -1,
			Handler:     valid.Handler,
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

func TestCoreRegistrationSubscriberCounts(t *testing.T) {
	for _, tc := range []struct {
		name        string
		queue       string
		subscribers int
		want        int
	}{
		{
			name:  "queue_default",
			queue: "workers",
			want:  1,
		},
		{
			name:        "queue_one",
			queue:       "workers",
			subscribers: 1,
			want:        1,
		},
		{
			name:        "queue_multiple",
			queue:       "workers",
			subscribers: 4,
			want:        4,
		},
		{
			name: "broadcast_default",
			want: 1,
		},
		{
			name:        "broadcast_ignores_subscribers",
			subscribers: 4,
			want:        1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			captureLogs(t)
			c := registrationClient(t)
			consumer := DefaultCoreQueueConsumer("subscription.count", tc.queue, tc.subscribers, func(context.Context, *nats.Msg) error { return nil })
			consumer.PendingMessages = 256
			consumer.PendingBytes = 1 << 20
			if err := c.RegisterCoreConsumers(context.Background(), consumer); err != nil {
				t.Fatal(err)
			}
			if got := c.Conn().NumSubscriptions(); got != tc.want {
				t.Fatalf("订阅数 = %d，期望 %d", got, tc.want)
			}
			if len(c.coreSubscriptions) != tc.want {
				t.Fatalf("未托管全部订阅: %d", len(c.coreSubscriptions))
			}
			for _, sub := range c.coreSubscriptions {
				messages, bytes, err := sub.PendingLimits()
				if err != nil || messages != 256 || bytes != 1<<20 {
					t.Fatalf("未逐个应用 pending 限额: messages=%d bytes=%d err=%v", messages, bytes, err)
				}
			}
		})
	}
}

func TestCoreQueueSubscribersConcurrentDeliveryAndDrain(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const subscribers = 4
	const messages = 128
	entered := make(chan struct{}, messages)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var received [messages]atomic.Int32
	var active atomic.Int32
	consumer := DefaultCoreQueueConsumer("subscription.concurrent", "workers", 1, func(_ context.Context, msg *nats.Msg) error {
		if got := active.Add(1); got > subscribers {
			t.Errorf("并发处理数超过订阅数: %d", got)
		}
		defer active.Add(-1)
		entered <- struct{}{}
		<-release
		if len(msg.Data) != 1 || int(msg.Data[0]) >= messages {
			t.Errorf("消息内容错误: %v", msg.Data)
			return nil
		}
		received[msg.Data[0]].Add(1)
		return nil
	})
	consumer.Subscribers = subscribers
	if err := c.RegisterCoreConsumers(ctx, consumer); err != nil {
		t.Fatal(err)
	}
	for i := range messages {
		if err := c.Publish(ctx, "subscription.concurrent", []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 回调保持阻塞，收到四次进入通知才能证明四个订阅确实能并发处理。
	for range subscribers {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("同组订阅未达到配置的并发度")
		}
	}
	draining := make([]<-chan nats.SubStatus, 0, subscribers)
	for _, sub := range c.coreSubscriptions {
		draining = append(draining, sub.StatusChanged(nats.SubscriptionDraining))
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	for _, status := range draining {
		select {
		case <-status:
		case err := <-closed:
			t.Fatalf("回调未完成就结束关闭: %v", err)
		case <-ctx.Done():
			t.Fatal("未排空全部同组订阅")
		}
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("同组订阅关闭未完成")
	}
	for i := range received {
		if got := received[i].Load(); got != 1 {
			t.Errorf("消息 %d 处理了 %d 次，期望 1 次", i, got)
		}
	}
}
