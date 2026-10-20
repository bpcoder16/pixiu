package natsx

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func registrationClient(t *testing.T) *Client {
	t.Helper()
	c, err := New(Config{
		Name: "registration",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestDefaultStreamConfigRegistrationAndPublish(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	subjects := []string{"defaults.tasks"}
	cfg := DefaultStreamConfig("DEFAULTS", 1, subjects...)
	subjects[0] = "other.tasks"
	if err := c.RegisterStreams(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	stream, err := c.JetStream().Stream(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	got := stream.CachedInfo().Config
	if got.Retention != jetstream.LimitsPolicy || got.Storage != jetstream.FileStorage ||
		got.Replicas != 1 || got.MaxBytes != 5<<30 || got.MaxMsgSize != 512<<10 ||
		got.Discard != jetstream.DiscardNew || got.MaxAge != 90*24*time.Hour ||
		got.Duplicates != 2*time.Minute || got.NoAck {
		t.Fatalf("服务端有效配置不符合推荐值: %+v", got)
	}
	ack, err := c.PublishJetStream(ctx, "defaults.tasks", []byte("task"), "same-task")
	if err != nil {
		t.Fatal(err)
	}
	future, err := c.PublishJetStreamAsync(ctx, "defaults.tasks", []byte("task"), "same-task")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case duplicate := <-future.Ok():
		if !duplicate.Duplicate || duplicate.Stream != ack.Stream || duplicate.Sequence != ack.Sequence {
			t.Fatalf("默认去重配置未生效: first=%+v duplicate=%+v", ack, duplicate)
		}
	case err := <-future.Err():
		t.Fatalf("默认配置下异步发布失败: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestDefaultJetStreamConsumerRegistrationAndOverrides(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "DEFAULT_CONSUMERS",
		Subjects: []string{"default-consumers.*"},
		Storage:  jetstream.MemoryStorage,
		// 区别于服务端继承值，验证默认构造函数显式设置未确认消息上限。
		ConsumerLimits: jetstream.StreamConsumerLimits{
			MaxAckPending: 2000,
		},
	}); err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 3)
	handler := func(_ context.Context, msg jetstream.Msg) error {
		received <- msg.Subject()
		return nil
	}
	subjects := []string{"default-consumers.defaults", "default-consumers.extra"}
	defaults := DefaultJetStreamConsumer("DEFAULT_CONSUMERS", "defaults", handler, subjects...)
	// 构造后修改原切片，不应影响注册和实际消息路由。
	subjects[0] = "default-consumers.changed"
	custom := DefaultJetStreamConsumer("DEFAULT_CONSUMERS", "custom", handler, "default-consumers.custom")
	custom.Config.BackOff = []time.Duration{time.Second}
	custom.Config.MaxDeliver = 7
	custom.Config.MaxAckPending = 64
	custom.Consume.Workers = 2
	if err := c.RegisterJetStreamConsumers(ctx, defaults, custom); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		spec          JetStreamConsumer
		subjects      []string
		ackWait       time.Duration
		maxDeliver    int
		maxAckPending int
		backOffLen    int
	}{
		{
			spec:          defaults,
			subjects:      []string{"default-consumers.defaults", "default-consumers.extra"},
			ackWait:       5 * time.Second,
			maxDeliver:    5,
			maxAckPending: 1000,
		},
		{
			spec:          custom,
			subjects:      []string{"default-consumers.custom"},
			ackWait:       time.Second,
			maxDeliver:    7,
			maxAckPending: 64,
			backOffLen:    1,
		},
	} {
		consumer, err := c.JetStream().Consumer(ctx, test.spec.Stream, test.spec.Config.Durable)
		if err != nil {
			t.Fatal(err)
		}
		got := consumer.CachedInfo().Config
		if got.Durable != test.spec.Config.Durable || got.FilterSubject != "" || !slices.Equal(got.FilterSubjects, test.subjects) ||
			got.AckPolicy != jetstream.AckExplicitPolicy || got.AckWait != test.ackWait ||
			got.MaxDeliver != test.maxDeliver || got.MaxAckPending != test.maxAckPending || len(got.BackOff) != test.backOffLen ||
			got.MaxRequestBatch != 0 || got.MaxRequestExpires != 0 {
			t.Fatalf("默认或覆盖的 Consumer 配置未生效: %+v", got)
		}
		for _, subject := range test.subjects {
			if _, err := c.PublishJetStream(ctx, subject, nil, subject); err != nil {
				t.Fatal(err)
			}
		}
	}
	seen := make(map[string]bool)
	for range 3 {
		select {
		case subject := <-received:
			if seen[subject] {
				t.Fatalf("默认消费声明重复处理消息: %s", subject)
			}
			seen[subject] = true
		case <-ctx.Done():
			t.Fatalf("默认消费声明未调用指定 handler: %v", ctx.Err())
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultStreamConfigRejectsOversizedMessage(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultStreamConfig("MESSAGE_SIZE", 1, "message-size.events")
	if err := c.RegisterStreams(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	// 正文已占满 512 KiB，组件追加的 Header 会使消息总大小超限。
	payload := []byte(strings.Repeat("x", 512<<10))
	_, err := c.PublishJetStream(ctx, "message-size.events", payload, "oversized-event")
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.ErrorCode(server.JSStreamMessageExceedsMaximumErr) {
		t.Fatalf("超限消息未被服务端拒绝: %v", err)
	}
}

func TestDefaultStreamConfigDiscardOldOverrideWhenFull(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultStreamConfig("ROLLING", 1, "rolling.events")
	cfg.Discard = jetstream.DiscardOld
	// 两条消息的正文已占满限额，加上 Header 等开销后会触发容量淘汰。
	cfg.MaxBytes = 2048
	if err := c.RegisterStreams(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	payload := []byte(strings.Repeat("x", 1024))
	first, err := c.PublishJetStream(ctx, "rolling.events", payload, "first-event")
	if err != nil {
		t.Fatal(err)
	}
	second, err := c.PublishJetStream(ctx, "rolling.events", payload, "second-event")
	if err != nil {
		t.Fatalf("容量满后未接受新消息: %v", err)
	}
	stream, err := c.JetStream().Stream(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := stream.GetMsg(ctx, first.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("容量满后未淘汰最老消息: %v", err)
	}
	msg, err := stream.GetMsg(ctx, second.Sequence)
	if err != nil || string(msg.Data) != string(payload) {
		t.Fatalf("容量满后未保留新消息: msg=%+v err=%v", msg, err)
	}
}

func TestDefaultStreamConfigReplicasReachServer(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultStreamConfig("REPLICATED", 3, "replicated.events")
	// 单机服务应拒绝三副本配置，确认注册没有将调用方的副本数回退为 1。
	err := c.RegisterStreams(ctx, cfg)
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) || apiErr.ErrorCode != jetstream.ErrorCode(server.JSStreamReplicasNotSupportedErr) {
		t.Fatalf("未收到服务端多副本限制错误: %v", err)
	}
}

func TestDefaultStreamConfigOverridesAndRejectsFullStream(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultStreamConfig("LIMITED", 1, "limited.events")
	cfg.Retention = jetstream.WorkQueuePolicy
	cfg.Storage = jetstream.MemoryStorage
	cfg.MaxBytes = 2048
	cfg.MaxMsgs = 1
	if err := c.RegisterStreams(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	stream, err := c.JetStream().Stream(ctx, cfg.Name)
	if err != nil {
		t.Fatal(err)
	}
	got := stream.CachedInfo().Config
	if got.Retention != jetstream.WorkQueuePolicy || got.Storage != jetstream.MemoryStorage ||
		got.Discard != jetstream.DiscardNew || got.MaxBytes != 2048 || got.MaxMsgs != 1 {
		t.Fatalf("调用方覆盖的配置未生效: %+v", got)
	}
	ack, err := c.PublishJetStream(ctx, "limited.events", []byte("first"), "first-event")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.PublishJetStream(ctx, "limited.events", []byte("second"), "second-event")
	var apiErr *jetstream.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("容量满后未收到服务端拒绝: %v", err)
	}
	first, err := stream.GetMsg(ctx, ack.Sequence)
	if err != nil || string(first.Data) != "first" {
		t.Fatalf("容量满后未保留原消息: msg=%+v err=%v", first, err)
	}
}

func TestRegisterResourcesReuseConflictAndPreparationFailure(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := jetstream.StreamConfig{
		Name:     "TASKS",
		Subjects: []string{"tasks.>"},
		Storage:  jetstream.MemoryStorage,
	}
	if _, err := c.JetStream().CreateStream(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterStreams(ctx, cfg); err != nil {
		t.Fatal(err)
	}
	conflict := cfg
	conflict.Subjects = []string{"other.>"}
	other, err := New(Config{
		Name: "other-registration",
		URLs: c.nc.Servers(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	if err := other.RegisterStreams(ctx, conflict); !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		t.Fatalf("独立客户端未返回 Stream 配置冲突: %v", err)
	}
	stream, err := c.JetStream().Stream(ctx, cfg.Name)
	if err != nil || stream.CachedInfo().Config.Subjects[0] != "tasks.>" {
		t.Fatalf("Stream 被修改: %v", err)
	}
	var called atomic.Int32
	first := JetStreamConsumer{
		Stream: "TASKS",
		Config: jetstream.ConsumerConfig{
			Durable:   "first",
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait:   30 * time.Second,
		},
		Handler: func(context.Context, jetstream.Msg) error {
			called.Add(1)
			return nil
		},
	}
	if _, err := c.PublishJetStream(ctx, "tasks.work", nil, "before-start"); err != nil {
		t.Fatal(err)
	}
	invalid := first
	invalid.Config.Durable = "invalid"
	invalid.Handler = nil
	if err := c.RegisterJetStreamConsumers(ctx, first, invalid); err == nil {
		t.Fatal("应先校验整批 handler")
	}
	if _, err := c.JetStream().Consumer(ctx, "TASKS", "first"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("本地校验失败仍创建了资源: %v", err)
	}
	missing := first
	missing.Stream = "MISSING"
	if err := other.RegisterJetStreamConsumers(ctx, first, missing); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("未保留远端错误链: %v", err)
	}
	if len(other.jetStreamConsumers) != 0 || called.Load() != 0 {
		t.Fatal("资源准备失败启动了本批 handler")
	}
	// 部分创建的远端资源保留；中止本次启动，新连接可按同配置复用。
	urls := c.nc.Servers()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	c, err = New(Config{
		Name: "restarted",
		URLs: urls,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.RegisterJetStreamConsumers(ctx, first); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for called.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := c.Close(); err != nil || called.Load() != 1 {
		t.Fatalf("恢复启动未消费: called=%d err=%v", called.Load(), err)
	}
}

func TestRegistrationRejectsPullLimitsBeforeStartingBatch(t *testing.T) {
	for _, source := range []string{"declaration", "effective"} {
		for _, tc := range []struct {
			name       string
			maxExpires time.Duration
			maxBatch   int
			wantError  string
		}{
			{
				name:       "expires",
				maxExpires: 10 * time.Second,
				wantError:  "MaxRequestExpires",
			},
			{
				name:      "batch with default prefetch",
				maxBatch:  2,
				wantError: "MaxMessages (4) exceeds consumer MaxRequestBatch (2)",
			},
		} {
			t.Run(source+"/"+tc.name, func(t *testing.T) {
				captureLogs(t)
				c := registrationClient(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
					Name:     "PULL_LIMITS",
					Subjects: []string{"pull-limits.jobs"},
					Storage:  jetstream.MemoryStorage,
				}); err != nil {
					t.Fatal(err)
				}
				if _, err := c.PublishJetStream(ctx, "pull-limits.jobs", nil, "before-registration"); err != nil {
					t.Fatal(err)
				}
				var called atomic.Int32
				first := JetStreamConsumer{
					Stream: "PULL_LIMITS",
					Config: jetstream.ConsumerConfig{
						Durable:   "first",
						AckPolicy: jetstream.AckExplicitPolicy,
						AckWait:   30 * time.Second,
					},
					Consume: ConsumeConfig{Workers: 2},
					Handler: func(context.Context, jetstream.Msg) error {
						called.Add(1)
						return nil
					},
				}
				second := first
				second.Config.Durable = "second"
				if source == "declaration" {
					second.Config.MaxRequestExpires = tc.maxExpires
					second.Config.MaxRequestBatch = tc.maxBatch
				} else {
					c.js = consumerPullLimitsJetStream{
						JetStream:  c.js,
						maxExpires: tc.maxExpires,
						maxBatch:   tc.maxBatch,
					}
				}
				subscriptions := c.nc.NumSubscriptions()
				err := c.RegisterJetStreamConsumers(ctx, first, second)
				if err == nil || !strings.Contains(err.Error(), tc.wantError) {
					t.Fatalf("不兼容的拉取限制未被拒绝: %v", err)
				}
				if len(c.jetStreamConsumers) != 0 || c.nc.NumSubscriptions() != subscriptions || called.Load() != 0 {
					t.Fatal("拉取限制校验失败仍启动了本批消费")
				}
				_, err = c.JetStream().Consumer(ctx, "PULL_LIMITS", "first")
				if source == "declaration" {
					if !errors.Is(err, jetstream.ErrConsumerNotFound) {
						t.Fatalf("声明校验失败仍创建了前面的 Consumer: %v", err)
					}
				} else if err != nil {
					t.Fatalf("有效配置校验失败应保留已准备的 Consumer: %v", err)
				}
			})
		}
	}
}

type consumerPullLimitsJetStream struct {
	jetstream.JetStream
	maxExpires time.Duration
	maxBatch   int
}

func (js consumerPullLimitsJetStream) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := js.JetStream.CreateConsumer(ctx, stream, cfg)
	if err != nil || cfg.Durable != "second" {
		return consumer, err
	}
	// 通过真实 RPC 返回与声明不同的有效限制，覆盖整批准备阶段的复核。
	cfg = consumer.CachedInfo().Config
	cfg.MaxRequestExpires = js.maxExpires
	cfg.MaxRequestBatch = js.maxBatch
	return js.JetStream.UpdateConsumer(ctx, stream, cfg)
}

func TestRegisterStreamsOncePerClient(t *testing.T) {
	captureLogs(t)
	cfg := jetstream.StreamConfig{
		Name:     "ONCE",
		Subjects: []string{"once.>"},
		Storage:  jetstream.MemoryStorage,
	}
	invalidRemote := cfg
	invalidRemote.Subjects = []string{"once.>.invalid"}
	for _, test := range []struct {
		name    string
		configs []jetstream.StreamConfig
		wantErr bool
	}{
		{
			name:    "success",
			configs: []jetstream.StreamConfig{cfg},
		},
		{
			name: "empty",
		},
		{
			name:    "duplicate",
			configs: []jetstream.StreamConfig{cfg, cfg},
			wantErr: true,
		},
		{
			name:    "remote failure",
			configs: []jetstream.StreamConfig{invalidRemote},
			wantErr: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := registrationClient(t)
			ctx := context.Background()
			if err := c.RegisterStreams(ctx, test.configs...); (err != nil) != test.wantErr {
				t.Fatalf("首次注册结果: %v", err)
			}
			if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
				Name:     "LATE",
				Subjects: []string{"late.>"},
				Storage:  jetstream.MemoryStorage,
			}); err == nil || !strings.Contains(err.Error(), "RegisterStreams already called") {
				t.Fatalf("未拒绝同一客户端的第二次注册: %v", err)
			}
			if _, err := c.JetStream().Stream(ctx, "LATE"); !errors.Is(err, jetstream.ErrStreamNotFound) {
				t.Fatalf("重复调用仍创建了 Stream: %v", err)
			}
		})
	}
}

func TestRegisterJetStreamConsumersOncePerClient(t *testing.T) {
	captureLogs(t)
	spec := JetStreamConsumer{
		Stream: "ONCE",
		Config: jetstream.ConsumerConfig{
			Durable:   "worker",
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait:   time.Minute,
		},
		Handler: func(context.Context, jetstream.Msg) error { return nil },
	}
	missing := spec
	missing.Stream = "MISSING"
	for _, test := range []struct {
		name      string
		consumers []JetStreamConsumer
		wantErr   bool
	}{
		{
			name:      "success",
			consumers: []JetStreamConsumer{spec},
		},
		{
			name: "empty",
		},
		{
			name:      "duplicate",
			consumers: []JetStreamConsumer{spec, spec},
			wantErr:   true,
		},
		{
			name:      "remote failure",
			consumers: []JetStreamConsumer{spec, missing},
			wantErr:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := registrationClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
				Name:     "ONCE",
				Subjects: []string{"once.>"},
				Storage:  jetstream.MemoryStorage,
			}); err != nil {
				t.Fatal(err)
			}
			if err := c.RegisterJetStreamConsumers(nil); err == nil {
				t.Fatal("未拒绝 nil context")
			}
			if err := c.RegisterJetStreamConsumers(ctx, test.consumers...); (err != nil) != test.wantErr {
				t.Fatalf("首次注册结果: %v", err)
			}
			late := spec
			late.Config.Durable = "late"
			if err := c.RegisterJetStreamConsumers(ctx, late); err == nil || !strings.Contains(err.Error(), "RegisterJetStreamConsumers already called") {
				t.Fatalf("未拒绝同一客户端的第二次注册: %v", err)
			}
			if _, err := c.JetStream().Consumer(ctx, "ONCE", "late"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("重复调用仍创建了 Consumer: %v", err)
			}
		})
	}
}

func TestRegisterCoreConsumersOncePerClient(t *testing.T) {
	captureLogs(t)
	spec := CoreConsumer{
		Name:    "worker",
		Subject: "once.core",
		Handler: func(context.Context, *nats.Msg) error { return nil },
	}
	invalid := spec
	invalid.Name = "invalid"
	invalid.Handler = nil
	badSubject := spec
	badSubject.Name = "bad-subject"
	badSubject.Subject = "bad subject"
	for _, test := range []struct {
		name      string
		consumers []CoreConsumer
		wantErr   bool
		expired   bool
	}{
		{
			name:      "success",
			consumers: []CoreConsumer{spec},
		},
		{
			name: "empty",
		},
		{
			name:      "invalid declaration",
			consumers: []CoreConsumer{spec, invalid},
			wantErr:   true,
		},
		{
			name:      "duplicate name",
			consumers: []CoreConsumer{spec, spec},
			wantErr:   true,
		},
		{
			name:      "subscription failure",
			consumers: []CoreConsumer{spec, badSubject},
			wantErr:   true,
		},
		{
			name:      "flush failure",
			consumers: []CoreConsumer{spec},
			wantErr:   true,
			expired:   true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := registrationClient(t)
			if err := c.RegisterCoreConsumers(nil); err == nil {
				t.Fatal("未拒绝 nil context")
			}
			ctx := context.Background()
			if test.expired {
				expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer cancel()
				ctx = expired
			}
			if err := c.RegisterCoreConsumers(ctx, test.consumers...); (err != nil) != test.wantErr {
				t.Fatalf("首次注册结果: %v", err)
			}
			subscriptions := c.Conn().NumSubscriptions()
			if err := c.RegisterCoreConsumers(context.Background(), spec); err == nil || !strings.Contains(err.Error(), "RegisterCoreConsumers already called") {
				t.Fatalf("未拒绝同一客户端的第二次注册: %v", err)
			}
			if c.Conn().NumSubscriptions() != subscriptions {
				t.Fatal("重复调用仍建立了订阅")
			}
		})
	}
}

func TestCoreRegistrationRejectsDuplicateNames(t *testing.T) {
	captureLogs(t)
	first := CoreConsumer{
		Name:    "worker",
		Subject: "core.events",
		Handler: func(context.Context, *nats.Msg) error { return nil },
	}
	for _, test := range []struct {
		name    string
		subject string
		queue   string
	}{
		{
			name:    "same declaration",
			subject: first.Subject,
		},
		{
			name:    "different subject",
			subject: "core.other",
		},
		{
			name:    "different queue",
			subject: first.Subject,
			queue:   "workers",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := registrationClient(t)
			second := first
			second.Subject = test.subject
			second.Queue = test.queue
			if err := c.RegisterCoreConsumers(context.Background(), first, second); err == nil || !strings.Contains(err.Error(), `duplicate Core consumer "worker"`) {
				t.Fatalf("未按 Name 拒绝重复声明: %v", err)
			}
			if c.Conn().NumSubscriptions() != 0 {
				t.Fatal("重复名称未在建立本批订阅前拒绝")
			}
		})
	}
}

func TestCoreRegistrationNamesArePerClient(t *testing.T) {
	captureLogs(t)
	url := startServer(t)
	for range 2 {
		c, err := New(Config{
			Name: "registration",
			URLs: []string{url},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		ctx := context.Background()
		if err := c.RegisterStreams(ctx); err != nil {
			t.Fatal(err)
		}
		if err := c.RegisterJetStreamConsumers(ctx); err != nil {
			t.Fatal(err)
		}
		if err := c.RegisterCoreConsumers(ctx, CoreConsumer{
			Name:    "worker",
			Subject: "core.events",
			Handler: func(context.Context, *nats.Msg) error { return nil },
		}); err != nil {
			t.Fatalf("不同 Client 的名称或注册入口相互影响: %v", err)
		}
	}
}

func TestRegisteredJetStreamAckRetryPanicAndExhaustion(t *testing.T) {
	buf := captureLogs(t)
	c := registrationClient(t)
	setup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.RegisterStreams(setup, jetstream.StreamConfig{
		Name:     "AUTO",
		Subjects: []string{"auto.*"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	type delivery struct {
		name    string
		attempt uint64
		at      time.Time
	}
	events := make(chan delivery, 16)
	var specs []JetStreamConsumer
	for _, name := range []string{"success", "panic", "exhausted"} {
		specs = append(specs, JetStreamConsumer{
			Stream: "AUTO",
			Config: jetstream.ConsumerConfig{
				Durable:       name,
				FilterSubject: "auto." + name,
				AckPolicy:     jetstream.AckExplicitPolicy,
				AckWait:       5 * time.Second,
				MaxDeliver:    3,
			},
			RetryDelay: 30 * time.Millisecond,
			Handler: func(ctx context.Context, msg jetstream.Msg) error {
				meta, err := msg.Metadata()
				if err != nil {
					return err
				}
				if ctx.Err() != nil {
					t.Error("启动 context 取消影响了 handler")
				}
				if id, _ := logit.LogIDFromContext(ctx); id != "auto-trace" {
					t.Errorf("未还原追踪 ID: %q", id)
				}
				events <- delivery{name, meta.NumDelivered, time.Now()}
				if name == "panic" && meta.NumDelivered == 1 {
					panic("expected panic")
				}
				if name == "exhausted" || meta.NumDelivered == 1 {
					return errors.New("expected failure")
				}
				return nil
			},
		})
	}
	if err := c.RegisterJetStreamConsumers(setup, specs...); err != nil {
		t.Fatal(err)
	}
	cancel()
	ctx, stop := context.WithTimeout(logit.WithContext(context.Background()), 5*time.Second)
	defer stop()
	logit.AddMeta(ctx, logit.Str(logit.LogId, "auto-trace"))
	for _, spec := range specs {
		if _, err := c.PublishJetStream(ctx, spec.Config.FilterSubject, nil, spec.Config.Durable); err != nil {
			t.Fatal(err)
		}
	}
	last := make(map[string]delivery)
	for range 7 {
		select {
		case event := <-events:
			previous := last[event.name]
			if event.attempt != previous.attempt+1 {
				t.Fatalf("重复或遗漏投递: %+v after %+v", event, previous)
			}
			if event.attempt > 1 && event.at.Sub(previous.at) < 25*time.Millisecond {
				t.Fatal("未应用延迟 NAK")
			}
			last[event.name] = event
		case <-ctx.Done():
			t.Fatalf("未完成重投: %v", last)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// 另一个连接检查持久化确认结果，避免将 handler 返回当作 ACK 已送达。
	peer, err := New(Config{Name: "observer", URLs: c.nc.Servers()})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Close()
	for _, name := range []string{"success", "panic", "exhausted"} {
		consumer, err := peer.JetStream().Consumer(ctx, "AUTO", name)
		if err != nil {
			t.Fatal(err)
		}
		want := 0
		if name == "exhausted" {
			want = 1
		}
		waitWorkerPending(t, consumer, want)
	}
	var callbacks, failures int
	var panicStack, exhausted bool
	for _, record := range readLogs(t, buf) {
		details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["operation"] != "jetStreamCallback" {
			continue
		}
		callbacks++
		if record["level"] == "ERROR" {
			failures++
		}
		panicStack = panicStack || record["panic_stack"] != nil
		exhausted = exhausted || record["outcome"] == "exhausted"
	}
	if callbacks != 7 || failures != 5 || !panicStack || !exhausted {
		t.Fatalf("结果日志重复或缺失: callbacks=%d failures=%d panic=%v exhausted=%v", callbacks, failures, panicStack, exhausted)
	}
}

func TestRegisteredJetStreamZeroRetryDelay(t *testing.T) {
	for _, test := range []struct {
		name    string
		ackWait time.Duration
		backOff []time.Duration
		waits   []time.Duration
	}{
		{
			name:    "backoff",
			backOff: []time.Duration{75 * time.Millisecond, 150 * time.Millisecond},
			waits:   []time.Duration{75 * time.Millisecond, 150 * time.Millisecond},
		},
		{
			name:    "ackwait",
			ackWait: 75 * time.Millisecond,
			waits:   []time.Duration{75 * time.Millisecond, 75 * time.Millisecond},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			captureLogs(t)
			c := registrationClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
				Name:     "ZERO_RETRY",
				Subjects: []string{"zero.retry"},
				Storage:  jetstream.MemoryStorage,
			}); err != nil {
				t.Fatal(err)
			}
			type delivery struct {
				attempt uint64
				at      time.Time
			}
			events := make(chan delivery, 3)
			if err := c.RegisterJetStreamConsumers(ctx, JetStreamConsumer{
				Stream: "ZERO_RETRY",
				Config: jetstream.ConsumerConfig{
					Durable:    "worker",
					AckPolicy:  jetstream.AckExplicitPolicy,
					AckWait:    test.ackWait,
					BackOff:    test.backOff,
					MaxDeliver: 3,
				},
				Handler: func(_ context.Context, msg jetstream.Msg) error {
					metadata, err := msg.Metadata()
					if err != nil {
						return err
					}
					events <- delivery{metadata.NumDelivered, time.Now()}
					switch metadata.NumDelivered {
					case 1:
						panic("expected panic")
					case 2:
						return errors.New("expected failure")
					default:
						return nil
					}
				},
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.PublishJetStream(ctx, "zero.retry", nil, "zero-retry-event"); err != nil {
				t.Fatal(err)
			}
			var previous time.Time
			for attempt := uint64(1); attempt <= 3; attempt++ {
				select {
				case event := <-events:
					if event.attempt != attempt {
						t.Fatalf("重复或遗漏投递: got=%d want=%d", event.attempt, attempt)
					}
					if attempt > 1 {
						want := test.waits[attempt-2]
						if elapsed := event.at.Sub(previous); elapsed < want-15*time.Millisecond {
							t.Fatalf("未按服务端确认期限重投: elapsed=%s want>=%s", elapsed, want)
						}
					}
					previous = event.at
				case <-ctx.Done():
					t.Fatalf("零值 RetryDelay 未使用服务端重投策略: attempt=%d err=%v", attempt, ctx.Err())
				}
			}
			consumer, err := c.JetStream().Consumer(ctx, "ZERO_RETRY", "worker")
			if err != nil {
				t.Fatal(err)
			}
			waitWorkerPending(t, consumer, 0)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRegisteredCoreBroadcastQueueReplyAndFailure(t *testing.T) {
	buf := captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var received [4]atomic.Int32
	var specs []CoreConsumer
	for i := range received {
		queue := ""
		if i >= 2 {
			queue = "workers"
		}
		specs = append(specs, CoreConsumer{
			Name:            fmt.Sprintf("events-%d", i),
			Subject:         "core.events",
			Queue:           queue,
			PendingMessages: 100,
			Handler: func(context.Context, *nats.Msg) error {
				received[i].Add(1)
				return nil
			},
		})
	}
	specs = append(specs, CoreConsumer{
		Name:    "request",
		Subject: "core.request",
		Handler: func(_ context.Context, msg *nats.Msg) error {
			switch string(msg.Data) {
			case "panic":
				panic("core panic")
			case "error":
				return errors.New("core failure")
			}
			return msg.Respond([]byte("reply"))
		},
	})
	if err := c.RegisterCoreConsumers(ctx, specs...); err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{"panic", "error"} {
		if err := c.Publish(ctx, "core.request", []byte(payload)); err != nil {
			t.Fatal(err)
		}
	}
	if msg, err := c.Request(ctx, "core.request", nil); err != nil || string(msg.Data) != "reply" {
		t.Fatalf("panic 后无法继续响应: %v", err)
	}
	for range 20 {
		if err := c.Publish(ctx, "core.events", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if received[0].Load() != 20 || received[1].Load() != 20 || received[2].Load()+received[3].Load() != 20 {
		t.Fatal("广播或 Queue 分发不正确")
	}
	var failures int
	for _, record := range readLogs(t, buf) {
		details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["operation"] == "coreCallback" && record["level"] == "ERROR" {
			failures++
		}
	}
	if failures != 2 {
		t.Fatalf("Core 错误日志数量: %d", failures)
	}
}

func TestRegistrationRejectsInvalidRetryConfigBeforePreparingBatch(t *testing.T) {
	for _, test := range []struct {
		name       string
		ackWait    time.Duration
		backOff    []time.Duration
		retryDelay time.Duration
		wantError  string
	}{
		{
			name:      "missing timeout",
			wantError: "AckWait or BackOff",
		},
		{
			name:       "missing timeout with delayed NAK",
			retryDelay: time.Second,
			wantError:  "AckWait or BackOff",
		},
		{
			name:       "backoff with delayed NAK",
			ackWait:    time.Minute,
			backOff:    []time.Duration{time.Second},
			retryDelay: time.Second,
			wantError:  "BackOff with positive RetryDelay",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			captureLogs(t)
			c := registrationClient(t)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
				Name:     "ACK_CONFIG",
				Subjects: []string{"ack.config"},
				Storage:  jetstream.MemoryStorage,
			}); err != nil {
				t.Fatal(err)
			}
			first := JetStreamConsumer{
				Stream: "ACK_CONFIG",
				Config: jetstream.ConsumerConfig{
					Durable:   "first",
					AckPolicy: jetstream.AckExplicitPolicy,
					AckWait:   time.Minute,
				},
				Handler: func(context.Context, jetstream.Msg) error { return nil },
			}
			second := first
			second.Config.Durable = "second"
			second.Config.AckWait = test.ackWait
			second.Config.BackOff = test.backOff
			second.RetryDelay = test.retryDelay
			if err := c.RegisterJetStreamConsumers(ctx, first, second); err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("未拒绝无效的重投配置: %v", err)
			}
			if len(c.jetStreamConsumers) != 0 {
				t.Fatal("重投配置校验失败仍启动了本批消费")
			}
			if _, err := c.JetStream().Consumer(ctx, "ACK_CONFIG", "first"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("重投配置校验失败仍创建了前面的 Consumer: %v", err)
			}
		})
	}
}

func TestRegistrationRejectsInvalidBatchAndClosedClient(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	consumer := JetStreamConsumer{
		Stream: "TASKS",
		Config: jetstream.ConsumerConfig{
			Durable:   "worker",
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait:   30 * time.Second,
		},
		Handler: func(context.Context, jetstream.Msg) error { return nil },
	}
	for _, change := range []func(*JetStreamConsumer){
		func(s *JetStreamConsumer) { s.Handler = nil },
		func(s *JetStreamConsumer) { s.Config.Durable = "" },
		func(s *JetStreamConsumer) { s.Config.AckPolicy = jetstream.AckAllPolicy },
		func(s *JetStreamConsumer) { s.Config.Name = "different" },
		func(s *JetStreamConsumer) { s.Consume.Workers = -1 },
		func(s *JetStreamConsumer) { s.Consume.MaxMessages = -1 },
		func(s *JetStreamConsumer) { s.RetryDelay = -1 },
		func(s *JetStreamConsumer) {
			s.Config.MaxRequestBatch = 1
			s.Consume.MaxMessages = 2
		},
	} {
		spec := consumer
		change(&spec)
		validationClient := registrationClient(t)
		if err := validationClient.RegisterJetStreamConsumers(ctx, spec); err == nil || errors.Is(err, jetstream.ErrStreamNotFound) {
			t.Fatalf("无效配置未在 RPC 前拒绝: %v", err)
		}
	}
	duplicateClient := registrationClient(t)
	if err := duplicateClient.RegisterJetStreamConsumers(ctx, consumer, consumer); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("未拒绝同批重复身份: %v", err)
	}
	coreClient := registrationClient(t)
	if err := coreClient.RegisterCoreConsumers(ctx,
		CoreConsumer{
			Name:    "valid",
			Subject: "core.valid",
			Handler: func(context.Context, *nats.Msg) error { return nil },
		},
		CoreConsumer{
			Name:    "invalid",
			Subject: "core.invalid",
		},
	); err == nil || coreClient.nc.NumSubscriptions() != 0 {
		t.Fatal("无效 Core 声明仍建立了前面的订阅")
	}
	for _, register := range []func(context.Context) error{
		func(ctx context.Context) error { return c.RegisterStreams(ctx) },
		func(ctx context.Context) error { return c.RegisterJetStreamConsumers(ctx) },
		func(ctx context.Context) error { return c.RegisterCoreConsumers(ctx) },
	} {
		if err := register(nil); err == nil {
			t.Fatal("未拒绝 nil context")
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if err := register(canceled); err != nil {
			t.Fatalf("空清单不应受启动 context 取消限制: %v", err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.RegisterJetStreamConsumers(ctx, consumer); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后未拒绝注册: %v", err)
	}
	if err := c.RegisterCoreConsumers(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后未拒绝 Core 注册: %v", err)
	}
}

func TestRegistrationCanceledAfterConsumerPreparation(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "CANCELED",
		Subjects: []string{"canceled.tasks"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PublishJetStream(ctx, "canceled.tasks", []byte("task"), "canceled-setup"); err != nil {
		t.Fatal(err)
	}
	// 远端准备完成后取消启动 context，不应影响本地 worker 启动和消息处理。
	c.js = cancelAfterConsumerCreate{
		JetStream: c.js,
		cancel:    cancel,
	}
	handled := make(chan error, 1)
	err := c.RegisterJetStreamConsumers(ctx, JetStreamConsumer{
		Stream: "CANCELED",
		Config: jetstream.ConsumerConfig{
			Durable:   "worker",
			AckPolicy: jetstream.AckExplicitPolicy,
			AckWait:   30 * time.Second,
		},
		Handler: func(ctx context.Context, _ jetstream.Msg) error {
			handled <- ctx.Err()
			return nil
		},
	})
	if err != nil || len(c.jetStreamConsumers) != 1 {
		t.Fatalf("准备后取消阻止了 worker 启动: err=%v handles=%d", err, len(c.jetStreamConsumers))
	}
	select {
	case err := <-handled:
		if err != nil {
			t.Fatalf("启动 context 取消影响了消息处理: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("准备后取消未能继续消费消息")
	}
}

type cancelAfterConsumerCreate struct {
	jetstream.JetStream
	cancel context.CancelFunc
}

func (js cancelAfterConsumerCreate) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := js.JetStream.CreateConsumer(ctx, stream, cfg)
	if err == nil {
		js.cancel()
	}
	return consumer, err
}

func TestRegisteredCoreWithCanceledContextWithoutDeadline(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	setup, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	if err := c.RegisterCoreConsumers(setup, CoreConsumer{
		Name:    "no-deadline",
		Subject: "core.no-deadline",
		Handler: func(ctx context.Context, msg *nats.Msg) error {
			if ctx.Err() != nil {
				t.Error("取消启动 context 不应取消长期 handler")
			}
			return msg.Respond([]byte("ready"))
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	msg, err := c.Request(ctx, "core.no-deadline", nil)
	if err != nil || string(msg.Data) != "ready" {
		t.Fatalf("已取消且无 deadline 的 context 阻止了订阅准备: msg=%v err=%v", msg, err)
	}
}

func TestRegisteredCorePartialFailureRemainsOwned(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	handler := func(context.Context, *nats.Msg) error { return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	err := c.RegisterCoreConsumers(ctx,
		CoreConsumer{
			Name:            "valid",
			Subject:         "valid",
			Queue:           "workers",
			Subscribers:     3,
			Handler:         handler,
			PendingMessages: 10,
			PendingBytes:    1024,
		},
		CoreConsumer{
			Name:    "invalid",
			Subject: "bad subject",
			Handler: handler,
		},
	)
	if !errors.Is(err, nats.ErrBadSubject) || len(c.coreSubscriptions) != 3 {
		t.Fatalf("部分失败未保留已建立的句柄: %v", err)
	}
	subs := slices.Clone(c.coreSubscriptions)
	for _, sub := range subs {
		messages, bytes, err := sub.PendingLimits()
		if err != nil || messages != 10 || bytes != 1024 {
			t.Fatalf("pending 参数未应用: %d %d %v", messages, bytes, err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatalf("部分注册失败后未正确回收: %v", err)
	}
	for _, sub := range subs {
		if sub.IsValid() {
			t.Fatal("部分注册失败后仍有订阅未回收")
		}
	}
}

// 记录确认操作，以区分“未发起确认”和“确认因连接已关闭而失败”。
type recordingAckMessage struct {
	jetstream.Msg
	acks, naks  int
	metadataErr error
	ackErr      error
}

func (m *recordingAckMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return &jetstream.MsgMetadata{NumDelivered: 1}, m.metadataErr
}

func (m *recordingAckMessage) Ack() error {
	m.acks++
	return m.ackErr
}

func (m *recordingAckMessage) NakWithDelay(time.Duration) error {
	m.naks++
	return nil
}

func TestAutoAckCancellationAndFailures(t *testing.T) {
	for _, fails := range []bool{false, true} {
		ctx, cancel := context.WithCancel(MessageContext(context.Background(), nil))
		msg := &recordingAckMessage{}
		err := handleJetStream(ctx, msg, func(context.Context, jetstream.Msg) error {
			cancel()
			if fails {
				return errors.New("business failed")
			}
			return nil
		}, time.Second, 4)
		if !errors.Is(err, context.Canceled) || msg.acks != 0 || msg.naks != 0 {
			t.Fatalf("取消后仍发出确认: err=%v ack=%d nak=%d", err, msg.acks, msg.naks)
		}
	}
	ctx := MessageContext(context.Background(), nil)
	metadataErr := errors.New("invalid metadata")
	msg := &recordingAckMessage{metadataErr: metadataErr}
	called := false
	err := handleJetStream(ctx, msg, func(context.Context, jetstream.Msg) error {
		called = true
		return nil
	}, time.Second, 4)
	if !errors.Is(err, metadataErr) || called || msg.acks != 0 || msg.naks != 0 {
		t.Fatal("元数据失败后仍执行业务或确认")
	}
	ackErr := errors.New("ack failed")
	msg = &recordingAckMessage{ackErr: ackErr}
	err = handleJetStream(ctx, msg, func(context.Context, jetstream.Msg) error {
		return nil
	}, time.Second, 4)
	if !errors.Is(err, ackErr) || msg.acks != 1 || msg.naks != 0 {
		t.Fatal("确认错误被吞掉或触发了二次确认")
	}
}
