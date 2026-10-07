package natsx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishLogIDUsesContextAndPropagatesHeader(t *testing.T) {
	url := startServer(t)
	for _, operation := range []string{"corePublish", "coreRequest", "jetStreamPublish"} {
		t.Run(operation, func(t *testing.T) {
			buf := captureLogs(t)
			if err := logit.SetMinLevel(logit.Default(), logit.InfoLevel); err != nil {
				t.Fatal(err)
			}
			c, err := New(Config{Name: operation, URLs: []string{url}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			subject := "trace." + operation
			received := make(chan string, 1)
			record := func(ctx context.Context, header nats.Header) {
				id, _ := logit.LogIDFromContext(ctx)
				if id != header.Get(TraceHeader) || header.Get("x-log-id") != "stale-lower" || header.Get("X-Other") != "keep" {
					t.Errorf("消费 ID 或 Header 错误: id=%q, header=%v", id, header)
				}
				logit.Info(ctx, "trace consumer")
				received <- id
			}
			if operation == "jetStreamPublish" {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if _, err := c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
					Name:     "TRACE",
					Subjects: []string{subject},
					Storage:  jetstream.MemoryStorage,
				}); err != nil {
					t.Fatal(err)
				}
				consumer, err := c.JetStream().CreateConsumer(ctx, "TRACE", jetstream.ConsumerConfig{
					Durable:   "worker",
					AckPolicy: jetstream.AckExplicitPolicy,
				})
				if err != nil {
					t.Fatal(err)
				}
				if _, err := c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(ctx context.Context, msg jetstream.Msg) {
					record(ctx, msg.Headers())
					if err := msg.Ack(); err != nil {
						t.Error(err)
					}
				}); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := c.Subscribe(subject, func(ctx context.Context, msg *nats.Msg) {
					record(ctx, msg.Header)
					if msg.Reply != "" {
						if err := msg.Respond(nil); err != nil {
							t.Error(err)
						}
					}
				}); err != nil {
					t.Fatal(err)
				}
			}
			existing := logit.WithContextLogID(context.Background())
			manual := logit.WithContext(context.Background())
			logit.AddMeta(manual, logit.Str(logit.LogId, "manual-id"))
			logit.AddMeta(manual, logit.Str(logit.LogId, "updated-id"))
			missing := logit.WithContext(context.Background())
			logit.AddField(missing, logit.Str("source", "keep"))
			longID := logit.WithContext(context.Background())
			logit.AddMeta(longID, logit.Str(logit.LogId, strings.Repeat("x", 129)))
			var wantIDs []string
			for i, source := range []context.Context{existing, manual, missing, longID, context.Background()} {
				ctx, cancel := context.WithTimeout(source, 3*time.Second)
				header := nats.Header{
					TraceHeader: []string{"stale"},
					"x-log-id":  []string{"stale-lower"},
					"X-Other":   []string{"keep"},
				}
				switch operation {
				case "corePublish":
					err = c.Publish(ctx, subject, nil, header)
				case "coreRequest":
					_, err = c.Request(ctx, subject, nil, header)
				case "jetStreamPublish":
					_, err = c.PublishJetStream(ctx, subject, nil, fmt.Sprintf("event-%d", i), header)
				}
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if id := header.Get(TraceHeader); id == "" || id == "stale" {
					t.Fatalf("未原地覆盖调用方追踪 ID: %q", id)
				}
				if header.Get("x-log-id") != "stale-lower" || header.Get("X-Other") != "keep" {
					t.Fatal("修改了其他 Header")
				}
				select {
				case id := <-received:
					if id != header.Get(TraceHeader) {
						t.Fatalf("调用方 Header 与消费 ID 不一致: header=%v, id=%q", header, id)
					}
					if want, ok := logit.LogIDFromContext(source); ok {
						if id != want {
							t.Fatalf("未复用入口 ID: got=%q, want=%q", id, want)
						}
					} else if len(id) != 36 {
						t.Fatalf("未生成新 ID: %q", id)
					}
					want, _ := logit.LogIDFromContext(source)
					wantIDs = append(wantIDs, want)
				case <-time.After(3 * time.Second):
					t.Fatal("未收到消息")
				}
			}
			if _, ok := logit.LogIDFromContext(missing); ok {
				t.Fatal("补生成的 ID 不应回写调用方 context")
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			var gotIDs []string
			var hasIDs []bool
			for _, record := range readLogs(t, buf) {
				details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
				if details["operation"] == operation {
					if record["level"] != "INFO" {
						t.Fatalf("成功操作应记录 Info 日志: %v", record)
					}
					id, _ := record[logit.LogId].(string)
					gotIDs = append(gotIDs, id)
					_, exists := record[logit.LogId]
					hasIDs = append(hasIDs, exists)
				}
			}
			if len(gotIDs) != len(wantIDs) {
				t.Fatalf("操作日志数量错误: got=%v, want=%v", gotIDs, wantIDs)
			}
			for i, want := range wantIDs {
				if gotIDs[i] != want {
					t.Fatalf("操作日志未沿用入口 ID: got=%v, want=%v", gotIDs, wantIDs)
				}
				if want == "" && hasIDs[i] {
					t.Fatal("ctx 缺少 ID 时不应补写日志 logID")
				}
			}
		})
	}
}
