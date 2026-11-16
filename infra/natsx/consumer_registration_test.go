package natsx

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumerRegistrationModes(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name:      "consumer-modes",
		Namespace: "qa",
		URLs:      []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.RegisterStreams(ctx, DefaultStreamConfig("TASKS", 1, "tasks.*")); err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 4)
	handler := func(_ context.Context, msg jetstream.Msg) error {
		received <- msg.Subject() + ":" + string(msg.Data())
		return nil
	}
	specs := []JetStreamConsumer{
		DefaultJetStreamConsumer("TASKS", "reused", handler, "tasks.reused"),
		DefaultJetStreamConsumer("TASKS", "created", handler, "tasks.created"),
		DefaultJetStreamConsumer("TASKS", "new-upsert", handler, "tasks.new-upsert"),
		DefaultJetStreamConsumer("TASKS", "updated", handler, "tasks.updated"),
	}
	specs[0].Mode = ""
	specs[1].Mode = ConsumerModeCreate
	specs[2].Mode = ConsumerModeUpsert
	specs[3].Mode = ConsumerModeUpsert
	specs[3].Config.Name = "updated"
	var previous jetstream.Consumer
	for _, i := range []int{0, 3} {
		cfg := specs[i].Config
		cfg.Durable = "qa_" + cfg.Durable
		if cfg.Name != "" {
			cfg.Name = "qa_" + cfg.Name
		}
		cfg.FilterSubjects = []string{"qa." + cfg.FilterSubjects[0]}
		previous, err = c.JetStream().CreateConsumer(ctx, "QA_TASKS", cfg)
		if err != nil {
			t.Fatal(err)
		}
	}
	// 更新已有 Durable 时必须保留确认进度，不能通过删除重建实现。
	if _, err := c.PublishJetStream(ctx, "tasks.updated", []byte("acknowledged"), "acknowledged"); err != nil {
		t.Fatal(err)
	}
	msg, err := previous.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := previous.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	specs[3].Config.BackOff = []time.Duration{time.Second, 5 * time.Second}
	specs[3].Config.MaxDeliver = 30
	if err := c.RegisterJetStreamConsumers(ctx, specs...); err != nil {
		t.Fatal(err)
	}
	updated, err := c.JetStream().Consumer(ctx, "QA_TASKS", "qa_updated")
	if err != nil {
		t.Fatal(err)
	}
	info := updated.CachedInfo()
	if info.Config.MaxDeliver != 30 || info.Config.AckWait != time.Second || !slices.Equal(info.Config.BackOff, specs[3].Config.BackOff) {
		t.Fatalf("upsert 未应用远端配置: %+v", info.Config)
	}
	if !info.Created.Equal(before.Created) || info.AckFloor.Stream != before.AckFloor.Stream || info.AckFloor.Stream == 0 {
		t.Fatalf("upsert 改变了 Durable 的创建时间或确认进度: before=%+v after=%+v", before, info)
	}
	want := make(map[string]bool)
	for _, spec := range specs {
		subject := spec.Config.FilterSubjects[0]
		if spec.Stream != "TASKS" || strings.HasPrefix(spec.Config.Durable, "qa_") || strings.HasPrefix(subject, "qa.") {
			t.Fatalf("注册不应修改调用方名称或主题: %+v", spec)
		}
		want["qa."+subject+":new"] = true
		if _, err := c.PublishJetStream(ctx, subject, []byte("new"), subject); err != nil {
			t.Fatal(err)
		}
	}
	for range specs {
		select {
		case got := <-received:
			if !want[got] {
				t.Fatalf("消息重复或已确认消息被重放: %s", got)
			}
			delete(want, got)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}

func TestConsumerRegistrationModeErrors(t *testing.T) {
	captureLogs(t)
	for _, mode := range []ConsumerMode{"", ConsumerModeCreate, ConsumerModeUpsert} {
		t.Run(string(mode), func(t *testing.T) {
			c := registrationClient(t)
			ctx := t.Context()
			if err := c.RegisterStreams(ctx, DefaultStreamConfig("TASKS", 1, "tasks")); err != nil {
				t.Fatal(err)
			}
			spec := DefaultJetStreamConsumer("TASKS", "worker", func(context.Context, jetstream.Msg) error {
				t.Error("远端注册失败不得启动 handler")
				return nil
			}, "tasks")
			if _, err := c.JetStream().CreateConsumer(ctx, "TASKS", spec.Config); err != nil {
				t.Fatal(err)
			}
			spec.Mode = mode
			spec.Config.MaxDeliver = 30
			if mode == ConsumerModeUpsert {
				// DeliverPolicy 不可更新，应保留服务端错误而非删除重建。
				spec.Config.DeliverPolicy = jetstream.DeliverNewPolicy
			}
			err := c.RegisterJetStreamConsumers(ctx, spec)
			var apiErr *jetstream.APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("应保留远端错误链: %v", err)
			}
			if mode != ConsumerModeUpsert && !errors.Is(err, jetstream.ErrConsumerExists) {
				t.Fatalf("create 应保留配置冲突错误: %v", err)
			}
			consumer, err := c.JetStream().Consumer(ctx, "TASKS", "worker")
			if err != nil || consumer.CachedInfo().Config.MaxDeliver != 5 {
				t.Fatalf("失败不应覆盖旧配置: %v, %v", consumer, err)
			}
			if err := c.RegisterJetStreamConsumers(ctx); err == nil {
				t.Fatal("失败也应占用注册次数")
			}
		})
	}
}

func TestConsumerRegistrationModePreflight(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx := t.Context()
	if err := c.RegisterStreams(ctx, DefaultStreamConfig("TASKS", 1, "tasks")); err != nil {
		t.Fatal(err)
	}
	handler := func(context.Context, jetstream.Msg) error { return nil }
	first := DefaultJetStreamConsumer("TASKS", "first", handler, "tasks")
	if _, err := c.JetStream().CreateConsumer(ctx, "TASKS", first.Config); err != nil {
		t.Fatal(err)
	}
	first.Mode = ConsumerModeUpsert
	first.Config.MaxDeliver = 30
	second := DefaultJetStreamConsumer("TASKS", "second", handler, "tasks")
	second.Mode = "existing"
	if err := c.RegisterJetStreamConsumers(ctx, first, second); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("应在远端操作前拒绝非法模式: %v", err)
	}
	consumer, err := c.JetStream().Consumer(ctx, "TASKS", "first")
	if err != nil || consumer.CachedInfo().Config.MaxDeliver != 5 {
		t.Fatalf("整批预检失败不得更新前序 Consumer: %v, %v", consumer, err)
	}
	if _, err := c.JetStream().Consumer(ctx, "TASKS", "second"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
		t.Fatalf("非法模式不得创建 Consumer: %v", err)
	}
}
