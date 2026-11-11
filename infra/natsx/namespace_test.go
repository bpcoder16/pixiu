package natsx

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNamespaceValidationBeforeDial(t *testing.T) {
	for _, namespace := range []string{"test01-v1", "-", "qa.team", "qa*", "qa>", "qa/name", "qa\\name", "qa@team", " qa", "qa ", "测试", "é", "\xff", "\n"} {
		t.Run(namespace, func(t *testing.T) {
			dialer := &authCountingDialer{}
			client, err := New(Config{
				Name:      "namespace",
				Namespace: namespace,
				URLs:      []string{nats.DefaultURL},
			}, nats.SetCustomDialer(dialer))
			if client != nil {
				_ = client.Close()
				t.Fatal("非法 namespace 意外创建客户端")
			}
			if err == nil || !strings.Contains(err.Error(), "namespace") || errors.Is(err, ErrConnect) || dialer.calls != 0 {
				t.Fatalf("应在拨号前拒绝非法 namespace: err=%v, dials=%d", err, dialer.calls)
			}
		})
	}
}

func TestNamespacePublishRequestAndRegistration(t *testing.T) {
	buf := captureLogs(t)
	url := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type delivery struct {
		subject  string
		stream   string
		consumer string
	}
	received := make(chan delivery, 32)
	handler := func(_ context.Context, msg jetstream.Msg) error {
		metadata, err := msg.Metadata()
		if err != nil {
			return err
		}
		received <- delivery{
			subject:  msg.Subject(),
			stream:   metadata.Stream,
			consumer: metadata.Consumer,
		}
		return nil
	}
	coreHandler := func(_ context.Context, msg *nats.Msg) error {
		received <- delivery{subject: msg.Subject}
		if msg.Reply != "" {
			return msg.Respond(msg.Data)
		}
		return nil
	}
	streams := []jetstream.StreamConfig{DefaultStreamConfig("EVENTS", 1, "events.>")}
	consumers := []JetStreamConsumer{
		DefaultJetStreamConsumer("EVENTS", "worker", handler, "events.created"),
		DefaultJetStreamConsumer("EVENTS", "single", handler),
		DefaultJetStreamConsumer("EVENTS", "all", handler),
	}
	consumers[0].Config.Name = "worker"
	consumers[1].Config.FilterSubject = "events.single"
	core := []CoreConsumer{
		DefaultCoreConsumer("broadcast", "updates.*", coreHandler),
		DefaultCoreQueueConsumer("queue", "jobs", "workers", 2, coreHandler),
		DefaultCoreConsumer("literal", "test01_v1.orders", coreHandler),
	}
	// 跨客户端复用同一组声明，验证转换不改写调用方结构体和切片。
	for _, tc := range []struct {
		namespace  string
		normalized string
	}{
		{},
		{
			namespace:  "test01_v1",
			normalized: "TEST01_V1",
		},
		{
			namespace:  "TeSt2",
			normalized: "TEST2",
		},
		{
			namespace:  "TEST3",
			normalized: "TEST3",
		},
		{
			namespace:  "123_456",
			normalized: "123_456",
		},
		{
			namespace:  "_",
			normalized: "_",
		},
	} {
		t.Run("namespace="+tc.namespace, func(t *testing.T) {
			var streamPrefix, namePrefix, subjectPrefix string
			if tc.normalized != "" {
				streamPrefix = tc.normalized + "_"
				namePrefix = tc.namespace + "_"
				subjectPrefix = tc.namespace + "."
			}
			client, err := New(Config{
				Name:      "namespace",
				Namespace: tc.namespace,
				URLs:      []string{url},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.RegisterStreams(ctx, streams...); err != nil {
				t.Fatal(err)
			}
			if err := client.RegisterJetStreamConsumers(ctx, consumers...); err != nil {
				t.Fatal(err)
			}
			if err := client.RegisterCoreConsumers(ctx, core...); err != nil {
				t.Fatal(err)
			}
			for i, sub := range client.coreSubscriptions {
				wantQueue := ""
				if i == 1 || i == 2 {
					wantQueue = namePrefix + "workers"
				}
				if sub.Queue != wantQueue {
					t.Fatalf("Queue 前缀或广播语义错误: %q", sub.Queue)
				}
			}
			// 纯生产者没有调用任何注册入口，也必须使用初始化时的 namespace。
			publisher, err := New(Config{
				Name:      "publisher",
				Namespace: tc.namespace,
				URLs:      []string{url},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()
			ack, err := publisher.PublishJetStream(ctx, "events.created", nil, "sync")
			if err != nil || ack.Stream != streamPrefix+"EVENTS" || ack.Duplicate {
				t.Fatalf("同步发布未匹配命名空间 Stream: ack=%v, err=%v", ack, err)
			}
			future, err := publisher.PublishJetStreamAsync(ctx, "events.single", nil, "async")
			if err != nil {
				t.Fatal(err)
			}
			select {
			case ack := <-future.Ok():
				if ack.Stream != streamPrefix+"EVENTS" || ack.Duplicate {
					t.Fatalf("异步发布未匹配命名空间 Stream: %+v", ack)
				}
			case err := <-future.Err():
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if future.Msg().Subject != subjectPrefix+"events.single" || future.Msg().Header.Get(jetstream.MsgIDHeader) != "async" {
				t.Fatalf("实际主题或 messageID 错误: %+v", future.Msg())
			}
			for _, subject := range []string{"updates.changed", "test01_v1.orders"} {
				if err := publisher.Publish(ctx, subject, nil); err != nil {
					t.Fatal(err)
				}
			}
			reply, err := publisher.Request(ctx, "jobs", []byte("response"))
			if err != nil || string(reply.Data) != "response" || !strings.HasPrefix(reply.Subject, "_INBOX.") {
				t.Fatalf("Request/Respond 不应改写回复 inbox: reply=%v, err=%v", reply, err)
			}
			counts := make(map[delivery]int)
			for range 7 {
				select {
				case msg := <-received:
					counts[msg]++
				case <-ctx.Done():
					t.Fatalf("未收到预期消息: %v", counts)
				}
			}
			want := map[delivery]int{
				{
					subject:  subjectPrefix + "events.created",
					stream:   streamPrefix + "EVENTS",
					consumer: namePrefix + "worker",
				}: 1,
				{
					subject:  subjectPrefix + "events.created",
					stream:   streamPrefix + "EVENTS",
					consumer: namePrefix + "all",
				}: 1,
				{
					subject:  subjectPrefix + "events.single",
					stream:   streamPrefix + "EVENTS",
					consumer: namePrefix + "single",
				}: 1,
				{
					subject:  subjectPrefix + "events.single",
					stream:   streamPrefix + "EVENTS",
					consumer: namePrefix + "all",
				}: 1,
				{subject: subjectPrefix + "updates.changed"}:  1,
				{subject: subjectPrefix + "jobs"}:             1,
				{subject: subjectPrefix + "test01_v1.orders"}: 1,
			}
			if !reflect.DeepEqual(counts, want) {
				t.Fatalf("主题或资源前缀错误: got=%v, want=%v", counts, want)
			}
			stream, err := client.JetStream().Stream(ctx, streamPrefix+"EVENTS")
			if err != nil || !reflect.DeepEqual(stream.CachedInfo().Config.Subjects, []string{subjectPrefix + "events.>"}) {
				t.Fatalf("原生 JetStream 入口应使用实际资源名称: %v", err)
			}
			for _, spec := range consumers {
				consumer, err := stream.Consumer(ctx, namePrefix+spec.Config.Durable)
				if err != nil {
					t.Fatal(err)
				}
				for {
					info, err := consumer.Info(ctx)
					if err != nil {
						t.Fatal(err)
					}
					if info.NumAckPending == 0 && info.NumPending == 0 {
						break
					}
					select {
					case <-ctx.Done():
						t.Fatal("namespace 影响了 JetStream ACK")
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
			if err := publisher.Close(); err != nil {
				t.Fatal(err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
	if streams[0].Name != "EVENTS" || streams[0].Subjects[0] != "events.>" ||
		consumers[0].Stream != "EVENTS" || consumers[0].Config.Name != "worker" || consumers[0].Config.Durable != "worker" ||
		consumers[0].Config.FilterSubjects[0] != "events.created" || consumers[1].Config.FilterSubject != "events.single" ||
		core[0].Name != "broadcast" || core[0].Subject != "updates.*" || core[1].Queue != "workers" {
		t.Fatal("namespace 转换修改了调用方配置")
	}
	var found bool
	for _, record := range readLogs(t, buf) {
		details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["subject"] == "test01_v1.events.created" {
			found = true
		}
	}
	if !found {
		t.Fatal("发送日志缺少带 namespace 的实际主题")
	}
}

func TestNamespaceDoesNotMaskInvalidInput(t *testing.T) {
	captureLogs(t)
	url := startServer(t)
	for _, value := range []string{"", " leading", "trailing ", " "} {
		t.Run(value, func(t *testing.T) {
			client, err := New(Config{
				Name:      "invalid",
				Namespace: "test1",
				URLs:      []string{url},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := client.RegisterStreams(ctx, DefaultStreamConfig(value, 1, "events")); err == nil {
				t.Fatal("前缀掩盖了非法 Stream 名称")
			}
			if err := client.RegisterJetStreamConsumers(ctx, DefaultJetStreamConsumer("EVENTS", value, func(context.Context, jetstream.Msg) error { return nil })); err == nil || !strings.Contains(err.Error(), "nonempty") {
				t.Fatalf("前缀掩盖了非法 Durable: %v", err)
			}
			if err := client.RegisterCoreConsumers(ctx, DefaultCoreConsumer("core", value, func(context.Context, *nats.Msg) error { return nil })); err == nil {
				t.Fatal("前缀掩盖了非法订阅主题")
			}
			if value == "" || value == " " {
				if err := client.Publish(ctx, value, nil); err == nil {
					t.Fatal("前缀掩盖了空发布主题")
				}
				if _, err := client.Request(ctx, value, nil); err == nil || !strings.Contains(err.Error(), "empty subject") {
					t.Fatalf("空 Request 主题未在发送前拒绝: %v", err)
				}
				if _, err := client.PublishJetStream(ctx, value, nil, "id"); err == nil || !strings.Contains(err.Error(), "empty subject") {
					t.Fatalf("空 JetStream 主题未在发送前拒绝: %v", err)
				}
				if _, err := client.PublishJetStreamAsync(ctx, value, nil, "id"); err == nil || !strings.Contains(err.Error(), "empty subject") {
					t.Fatalf("空异步主题未在发送前拒绝: %v", err)
				}
			}
		})
	}
}

func TestNamespaceLegacyStreamAndDurable(t *testing.T) {
	serverURL := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connect := func(t *testing.T, namespace string) *Client {
		t.Helper()
		c, err := New(Config{
			Name:      "upgrade",
			Namespace: namespace,
			URLs:      []string{serverURL},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	// 预置大写 Stream 前缀和原始大小写的主题、Durable，验证只转换 Stream 名称。
	legacy := connect(t, "")
	stream, err := legacy.JetStream().CreateStream(ctx, DefaultStreamConfig("QA_BLUE_orders", 1, "qa.events"))
	if err != nil {
		t.Fatal(err)
	}
	durable, err := stream.CreateConsumer(ctx, DefaultJetStreamConsumer("QA_BLUE_orders", "qa_reader", nil).Config)
	if err != nil {
		t.Fatal(err)
	}
	for _, data := range []string{"acked", "pending"} {
		if _, err := legacy.JetStream().Publish(ctx, "qa.events", []byte(data)); err != nil {
			t.Fatal(err)
		}
	}
	msg, err := durable.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := durable.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before.AckFloor.Stream != 1 || before.NumPending != 1 {
		t.Fatalf("旧 Durable 未形成预期确认进度: %+v", before)
	}

	t.Run("拒绝碰撞", func(t *testing.T) {
		foreign := connect(t, "qa_blue")
		called := make(chan struct{}, 2)
		spec := DefaultJetStreamConsumer("orders", "reader", func(context.Context, jetstream.Msg) error {
			called <- struct{}{}
			return nil
		})
		err := foreign.RegisterJetStreamConsumers(ctx, spec)
		if err == nil || !strings.Contains(err.Error(), "QA_BLUE_orders") || !strings.Contains(err.Error(), "qa.events") || !strings.Contains(err.Error(), `namespace "qa_blue"`) {
			t.Fatalf("应报告 namespace、冲突 Stream 和主题: %v", err)
		}
		if _, err := stream.Consumer(ctx, "qa_blue_reader"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("冲突注册创建了 Durable: %v", err)
		}
		if len(called) != 0 {
			t.Fatal("冲突注册启动了 handler")
		}
	})

	t.Run("接续旧进度", func(t *testing.T) {
		c := connect(t, "qa")
		if err := c.RegisterStreams(ctx, DefaultStreamConfig("BLUE_orders", 1, "events")); err != nil {
			t.Fatal(err)
		}
		received := make(chan string, 2)
		spec := DefaultJetStreamConsumer("BLUE_orders", "reader", func(_ context.Context, msg jetstream.Msg) error {
			received <- string(msg.Data())
			return nil
		})
		if err := c.RegisterJetStreamConsumers(ctx, spec); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-received:
			if got != "pending" {
				t.Fatalf("应从积压消息接续，收到 %q", got)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		for {
			info, err := durable.Info(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !info.Created.Equal(before.Created) {
				t.Fatal("旧 Durable 被重建")
			}
			if info.AckFloor.Stream == 2 && info.AckFloor.Consumer == 2 && info.NumPending == 0 && info.NumAckPending == 0 {
				break
			}
			select {
			case <-ctx.Done():
				t.Fatalf("旧 Durable 的 ACK 进度未接续: %+v", info)
			case <-time.After(10 * time.Millisecond):
			}
		}
	})
}

func TestNamespaceStreamRenamePreservesLegacy(t *testing.T) {
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	legacy, err := c.JetStream().CreateStream(ctx, DefaultStreamConfig("qa_EVENTS", 1, "qa.events"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.JetStream().Publish(ctx, "qa.events", []byte("pending")); err != nil {
		t.Fatal(err)
	}
	client, err := New(Config{
		Name:      "rename",
		Namespace: "qa",
		URLs:      []string{c.Conn().ConnectedUrl()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// 主题保留原大小写，旧 Stream 仍占用主题时只能显式迁移，不能自动删除旧资源。
	if err := client.RegisterStreams(ctx, DefaultStreamConfig("EVENTS", 1, "events")); err == nil || !strings.Contains(err.Error(), "QA_EVENTS") {
		t.Fatalf("同主题的新 Stream 应报告冲突: %v", err)
	}
	if _, err := c.JetStream().Stream(ctx, "QA_EVENTS"); !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatalf("冲突后不应创建新 Stream: %v", err)
	}
	msg, err := legacy.GetMsg(ctx, 1)
	if err != nil || msg.Subject != "qa.events" || string(msg.Data) != "pending" {
		t.Fatalf("旧 Stream 及积压消息应保留: msg=%+v, err=%v", msg, err)
	}
}

func TestNamespaceMixedStreamRequiresFilter(t *testing.T) {
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cfg := DefaultStreamConfig("QA_mixed", 1, "qa.events", "other.events")
	stream, err := c.JetStream().CreateStream(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, filter := range []bool{false, true} {
		client, err := New(Config{
			Name:      "mixed",
			Namespace: "qa",
			URLs:      []string{c.Conn().ConnectedUrl()},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		received := make(chan string, 2)
		spec := DefaultJetStreamConsumer("mixed", "reader", func(_ context.Context, msg jetstream.Msg) error {
			received <- msg.Subject()
			return nil
		})
		if filter {
			spec.Config.FilterSubject = "events"
		}
		err = client.RegisterJetStreamConsumers(ctx, spec)
		if !filter {
			if err == nil || !strings.Contains(err.Error(), "other.events") {
				t.Fatalf("混合主题应要求显式过滤: %v", err)
			}
			if _, err := stream.Consumer(ctx, "qa_reader"); !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("整流注册失败后不应存在 Consumer: %v", err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, subject := range []string{"other.events", "qa.events"} {
			if _, err := c.JetStream().Publish(ctx, subject, nil); err != nil {
				t.Fatal(err)
			}
		}
		select {
		case subject := <-received:
			if subject != "qa.events" {
				t.Fatalf("显式过滤后收到其他 namespace 主题 %q", subject)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
}
