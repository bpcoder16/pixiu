package natsx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOutboundDurationSummary(t *testing.T) {
	url := startServer(t)
	for _, tt := range []struct {
		name     string
		timing   bool
		filtered bool
	}{
		{name: "enabled", timing: true},
		{name: "filtered", timing: true, filtered: true},
		{name: "withoutStart"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			if tt.filtered {
				if err := logit.SetMinLevel(logit.Default(), logit.FatalLevel); err != nil {
					t.Fatal(err)
				}
			}
			c, err := New(Config{
				Name: "timed",
				URLs: []string{url},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			subject := "duration." + tt.name
			if _, err := c.Subscribe(subject+".request", func(_ context.Context, msg *nats.Msg) {
				if err := msg.Respond(nil); err != nil {
					t.Error(err)
				}
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := c.JetStream().CreateStream(context.Background(), jetstream.StreamConfig{
				Name:     "DURATION_" + tt.name,
				Subjects: []string{subject + ".js"},
				Storage:  jetstream.MemoryStorage,
			}); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			if tt.timing {
				ctx = logit.WithStart(ctx)
			}
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			if err := c.Publish(ctx, subject+".publish", nil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Request(ctx, subject+".request", nil, nil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.PublishJetStream(ctx, subject+".js", nil, "timed-event"); err != nil {
				t.Fatal(err)
			}
			if err := c.Publish(ctx, "invalid subject", nil); !errors.Is(err, nats.ErrBadSubject) {
				t.Fatalf("未触发发布失败: %v", err)
			}
			canceled, stop := context.WithCancel(ctx)
			stop()
			if err := c.Publish(canceled, subject+".publish", nil); !errors.Is(err, context.Canceled) {
				t.Fatalf("未拒绝已取消的调用: %v", err)
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if err := logit.SetMinLevel(logit.Default(), logit.InfoLevel); err != nil {
				t.Fatal(err)
			}
			logit.InfoDuration(ctx, "duration summary")
			var summary map[string]any
			var elapsed []any
			for _, record := range readLogs(t, buf) {
				if record["msg"] == "duration summary" {
					summary = record
				}
				details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
				switch details["operation"] {
				case "corePublish", "coreRequest", "jetStreamPublish":
					elapsed = append(elapsed, record[logit.DownstreamDurationMSKey])
				}
			}
			if summary == nil {
				t.Fatal("缺少耗时汇总日志")
			}
			wantCount := 4
			if tt.filtered {
				wantCount = 0
			}
			if len(elapsed) != wantCount {
				t.Fatalf("出站操作日志数量错误: %v", elapsed)
			}
			count := 0
			for key := range summary {
				if strings.HasPrefix(key, "NATS_timed_") {
					count++
				}
			}
			if !tt.timing {
				if count != 0 {
					t.Fatalf("未启用 WithStart 时不应登记耗时: %v", summary)
				}
				return
			}
			if count != 4 {
				t.Fatalf("出站耗时缺失或重复登记: %v", summary)
			}
			for i := 0; i < 4; i++ {
				key := fmt.Sprintf("NATS_timed_%d_duration_ms", i+1)
				value, ok := summary[key].(float64)
				if !ok || value < 0 || (i == 1 && value == 0) {
					t.Fatalf("操作耗时未登记: %s=%v", key, summary[key])
				}
				if !tt.filtered && elapsed[i] != value {
					t.Fatalf("操作日志与汇总耗时不一致: log=%v summary=%v", elapsed[i], value)
				}
			}
		})
	}
}
