package natsx

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func assertMessageLogMode(t *testing.T, details map[string]any, enabled bool) {
	t.Helper()
	for _, prefix := range []string{"request", "response"} {
		_, hasMessage := details[prefix+"_bytes"]
		for _, suffix := range []string{"body", "headers", "body_encoding"} {
			key := prefix + "_" + suffix
			if _, exists := details[key]; exists != (enabled && hasMessage) {
				t.Fatalf("详情开关与 %s 字段不符: enabled=%v details=%v", key, enabled, details)
			}
		}
	}
}

func TestAsyncLogDetailsArePerClient(t *testing.T) {
	buf := captureLogs(t)
	url := startServer(t)
	configs := []Config{
		{
			Name:       "detailed",
			URLs:       []string{url},
			LogDetails: true,
		},
		{
			Name: "summary",
			URLs: []string{url},
		},
	}
	clients := make([]*Client, len(configs))
	for i, cfg := range configs {
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
		t.Cleanup(func() { _ = c.Close() })
	}
	ctx, cancel := context.WithTimeout(logit.WithContextLogID(context.Background()), 5*time.Second)
	defer cancel()
	if err := clients[0].RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "LOG_MODES",
		Subjects: []string{"logs.*"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	data := []byte{0xff, 0x00, 0x41}
	for i, c := range clients {
		// 两个实例均已构造，再交替使用各自开关，验证配置不会互相覆盖。
		name := configs[i].Name
		if _, err := c.PublishJetStreamAsync(ctx, "logs."+name, data, name, nats.Header{
			"X-Private": []string{"private-value"},
		}); err != nil {
			t.Fatal(err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	id, _ := logit.LogIDFromContext(ctx)
	seen := make(map[string]bool)
	for _, record := range readLogs(t, buf) {
		details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["operation"] != "jetStreamPublishAsync" {
			continue
		}
		name, _ := record[logit.DownstreamIDKey].(string)
		enabled := name == "detailed"
		assertMessageLogMode(t, details, enabled)
		if details["request_bytes"] != float64(len(data)) || details["response_bytes"] != nil || details["ack"] != nil || record[logit.LogId] != id {
			t.Fatalf("异步提交概要或追踪错误: %v", record)
		}
		if enabled && (details["request_body"] != base64.StdEncoding.EncodeToString(data) || details["request_body_encoding"] != "base64") {
			t.Fatalf("异步详情未保留二进制正文: %v", details)
		}
		seen[name] = true
	}
	if !seen["detailed"] || !seen["summary"] {
		t.Fatalf("缺少实例日志: %v", seen)
	}
}
