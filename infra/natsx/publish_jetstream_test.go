package natsx

import (
	"context"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishJetStreamRequiredIDAndOptionalHeaders(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{
		Name: "js-options",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(logit.WithContextLogID(context.Background()), 3*time.Second)
	defer cancel()
	stream, err := c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
		Name:       "JS_OPTIONS",
		Subjects:   []string{"options.publish"},
		Storage:    jetstream.MemoryStorage,
		Duplicates: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx = logit.WithStart(ctx)
	id, _ := logit.LogIDFromContext(ctx)
	header := nats.Header{
		jetstream.MsgIDHeader: []string{"stale-id"},
		"X-Source":            []string{"service", "second"},
	}
	extra := nats.Header{}
	if ack, err := c.PublishJetStream(ctx, "options.publish", nil, "", header); err == nil || ack != nil {
		t.Fatalf("空 messageID 应被拒绝: ack=%v err=%v", ack, err)
	}
	if ack, err := c.PublishJetStream(ctx, "options.publish", nil, "event-1", header, extra); err == nil || ack != nil {
		t.Fatalf("多个 Header 应被拒绝: ack=%v err=%v", ack, err)
	}
	if header.Get(jetstream.MsgIDHeader) != "stale-id" || header.Get(TraceHeader) != "" || header.Get("X-Source") != "service" || len(extra) != 0 {
		t.Fatal("发送前拒绝参数时不应修改 Header")
	}
	for i, headers := range [][]nats.Header{{header}, {nil}, nil} {
		ack, err := c.PublishJetStream(ctx, "options.publish", []byte("event"), "event-1", headers...)
		if err != nil || ack == nil || ack.Stream != "JS_OPTIONS" || ack.Sequence != 1 || ack.Duplicate != (i != 0) {
			t.Fatalf("相同 ID 应去重: ack=%v err=%v", ack, err)
		}
	}
	if header.Get(jetstream.MsgIDHeader) != "stale-id" || header.Get(TraceHeader) != "" || header.Get("X-Source") != "service" {
		t.Fatalf("同步发布修改了调用方 Header: %v", header)
	}
	ack, err := c.PublishJetStream(ctx, "options.publish", nil, "event-2")
	if err != nil || ack == nil || ack.Sequence != 2 || ack.Duplicate {
		t.Fatalf("不同 ID 应新增消息: ack=%v err=%v", ack, err)
	}
	stored, err := stream.GetMsg(ctx, 1)
	if err != nil || stored == nil || string(stored.Data) != "event" || stored.Header.Get(jetstream.MsgIDHeader) != "event-1" || stored.Header.Get(TraceHeader) != id || stored.Header.Get("X-Source") != "service" {
		t.Fatalf("服务端消息或 Header 错误: msg=%v err=%v", stored, err)
	}
	if values := stored.Header.Values("X-Source"); len(values) != 2 || values[1] != "second" {
		t.Fatalf("业务 Header 多值丢失: %v", values)
	}
	info, err := stream.Info(ctx)
	if err != nil || info == nil || info.State.Msgs != 2 {
		t.Fatalf("无效参数不应发送，重复 ID 不应新增消息: info=%v err=%v", info, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	logit.InfoDuration(ctx, "publish summary")
	for _, record := range readLogs(t, buf) {
		if record["msg"] == "publish summary" {
			if record["NATS_js-options_4_duration_ms"] == nil || record["NATS_js-options_5_duration_ms"] != nil {
				t.Fatalf("发送前参数拒绝不应登记耗时: %v", record)
			}
			return
		}
	}
	t.Fatal("缺少发布耗时汇总")
}

func TestJetStreamPublishPreservesHeadersOnError(t *testing.T) {
	captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(logit.WithContextLogID(context.Background()), 3*time.Second)
	defer cancel()
	for _, async := range []bool{false, true} {
		name := "sync"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			header := nats.Header{
				TraceHeader:           []string{"original-trace"},
				jetstream.MsgIDHeader: []string{"original-id"},
				"X-Source":            []string{"service", "second"},
			}
			var err error
			if async {
				_, err = c.PublishJetStreamAsync(ctx, "invalid subject", nil, "event", header)
			} else {
				_, err = c.PublishJetStream(ctx, "invalid subject", nil, "event", header)
			}
			if err == nil {
				t.Fatal("非法主题应发布失败")
			}
			if len(header) != 3 || header.Get(TraceHeader) != "original-trace" || header.Get(jetstream.MsgIDHeader) != "original-id" || header.Get("X-Source") != "service" {
				t.Fatalf("发布失败修改了调用方 Header: %v", header)
			}
		})
	}
}
