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
	for _, namespace := range []string{"qa.team", "qa*", "qa>", "qa/name", "qa\\name", " qa", "qa ", "测试", "\n"} {
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
		DefaultCoreConsumer("literal", "Test_1-a.orders", coreHandler),
	}
	// 跨客户端复用同一组声明，验证转换不改写调用方结构体和切片。
	for _, namespace := range []string{"", "Test_1-a", "test2"} {
		t.Run("namespace="+namespace, func(t *testing.T) {
			var namePrefix, subjectPrefix string
			if namespace != "" {
				namePrefix = namespace + "_"
				subjectPrefix = namespace + "."
			}
			client, err := New(Config{
				Name:      "namespace",
				Namespace: namespace,
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
				Namespace: namespace,
				URLs:      []string{url},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer publisher.Close()
			ack, err := publisher.PublishJetStream(ctx, "events.created", nil, "sync")
			if err != nil || ack.Stream != namePrefix+"EVENTS" || ack.Duplicate {
				t.Fatalf("同步发布未匹配命名空间 Stream: ack=%v, err=%v", ack, err)
			}
			future, err := publisher.PublishJetStreamAsync(ctx, "events.single", nil, "async")
			if err != nil {
				t.Fatal(err)
			}
			select {
			case ack := <-future.Ok():
				if ack.Stream != namePrefix+"EVENTS" || ack.Duplicate {
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
			for _, subject := range []string{"updates.changed", "Test_1-a.orders"} {
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
					stream:   namePrefix + "EVENTS",
					consumer: namePrefix + "worker",
				}: 1,
				{
					subject:  subjectPrefix + "events.created",
					stream:   namePrefix + "EVENTS",
					consumer: namePrefix + "all",
				}: 1,
				{
					subject:  subjectPrefix + "events.single",
					stream:   namePrefix + "EVENTS",
					consumer: namePrefix + "single",
				}: 1,
				{
					subject:  subjectPrefix + "events.single",
					stream:   namePrefix + "EVENTS",
					consumer: namePrefix + "all",
				}: 1,
				{subject: subjectPrefix + "updates.changed"}: 1,
				{subject: subjectPrefix + "jobs"}:            1,
				{subject: subjectPrefix + "Test_1-a.orders"}: 1,
			}
			if !reflect.DeepEqual(counts, want) {
				t.Fatalf("主题或资源前缀错误: got=%v, want=%v", counts, want)
			}
			stream, err := client.JetStream().Stream(ctx, namePrefix+"EVENTS")
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
		if details["subject"] == "Test_1-a.events.created" {
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
