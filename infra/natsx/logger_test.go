package natsx

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func captureNamedLogs(t *testing.T, name string) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
	previous := logit.Named(name)
	logit.SetNamed(name, logger)
	t.Cleanup(func() {
		logit.SetNamed(name, previous)
		_ = logit.Close(logger)
	})
	return buf
}

func TestLoggerRouting(t *testing.T) {
	defaultLogs := captureLogs(t)
	name := t.Name() + "/nats"
	consumerLogs := captureNamedLogs(t, name)
	publishLogs := map[string]*bytes.Buffer{}
	for _, route := range []string{"http", "cron"} {
		publishLogs[route] = captureNamedLogs(t, t.Name()+"/"+route)
	}
	client, err := New(Config{
		Name:       "routing",
		LoggerName: name,
		URLs:       []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	received := make(chan struct{}, 8)
	handle := func(ctx context.Context) error {
		logit.Info(ctx, "business")
		// 消费入口的路由须同时传入业务及其下游调用。
		if err := client.Publish(ctx, "routing.downstream", nil); err != nil {
			return err
		}
		received <- struct{}{}
		return nil
	}
	if err := client.RegisterStreams(ctx, DefaultStreamConfig("ROUTING", 1, "routing.js")); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterJetStreamConsumers(ctx, DefaultJetStreamConsumer("ROUTING", "worker", func(ctx context.Context, _ jetstream.Msg) error {
		return handle(ctx)
	})); err != nil {
		t.Fatal(err)
	}
	if err := client.RegisterCoreConsumers(ctx, DefaultCoreConsumer("worker", "routing.core", func(ctx context.Context, msg *nats.Msg) error {
		if err := handle(ctx); err != nil {
			return err
		}
		if msg.Reply != "" {
			return msg.Respond(nil)
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"http", "cron"} {
		caller := logit.WithContextLogID(logit.WithLoggerName(ctx, t.Name()+"/"+route))
		if err := client.Publish(caller, "routing.core", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := client.Request(caller, "routing.core", nil); err != nil {
			t.Fatal(err)
		}
		if _, err := client.PublishJetStream(caller, "routing.js", nil, route+"-sync"); err != nil {
			t.Fatal(err)
		}
		future, err := client.PublishJetStreamAsync(caller, "routing.js", nil, route+"-async")
		if err != nil {
			t.Fatal(err)
		}
		select {
		case <-future.Ok():
		case err := <-future.Err():
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for range 8 {
		select {
		case <-received:
		case <-ctx.Done():
			t.Fatal("消息消费未完成")
		}
	}
	// 等到全部回调、日志和连接关闭后再读取缓冲区，避免与日志写入竞争。
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if defaultLogs.Len() != 0 {
		t.Fatalf("NATS 自身日志不得落到默认 Logger: %s", defaultLogs.String())
	}
	counts := make(map[string]int)
	for _, record := range readLogs(t, consumerLogs) {
		counts[record["msg"].(string)]++
		if event, ok := record["nats_event"].(string); ok {
			counts[event]++
		}
		if details, ok := record[logit.DownstreamDetailsKey].(map[string]any); ok {
			counts[details["operation"].(string)]++
		}
	}
	for key, want := range map[string]int{
		"connected":         1,
		"closed":            1,
		"business":          8,
		"coreSubscribe":     1,
		"coreCallback":      4,
		"jetStreamCallback": 4,
		"corePublish":       8,
	} {
		if counts[key] != want {
			t.Errorf("消费日志 %s 数量为 %d，预期 %d", key, counts[key], want)
		}
	}
	for route, buf := range publishLogs {
		counts := make(map[string]int)
		for _, record := range readLogs(t, buf) {
			details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
			if !ok {
				t.Fatalf("%s 中混入非发布日志: %v", route, record)
			}
			counts[details["operation"].(string)]++
		}
		for _, operation := range []string{"corePublish", "coreRequest", "jetStreamPublish", "jetStreamPublishAsync"} {
			if counts[operation] != 1 {
				t.Errorf("%s 的 %s 日志未沿用调用方 ctx: %v", route, operation, counts)
			}
		}
	}
}

func TestLoggerNameFallbackAndConnectFailure(t *testing.T) {
	named := t.Name() + "/named"
	for _, name := range []string{"", t.Name() + "/missing", named} {
		t.Run(name, func(t *testing.T) {
			defaultLogs := captureLogs(t)
			want := defaultLogs
			if name == named {
				want = captureNamedLogs(t, name)
			}
			_, err := New(Config{
				Name:       "failed",
				LoggerName: name,
				URLs:       []string{"nats://127.0.0.1:1"},
			})
			if !errors.Is(err, ErrConnect) {
				t.Fatalf("应返回连接失败: %v", err)
			}
			records := readLogs(t, want)
			if len(records) != 1 || records[0]["nats_event"] != "connect_failed" {
				t.Fatalf("连接失败或默认回退日志路由错误: %v", records)
			}
			if want != defaultLogs && defaultLogs.Len() != 0 {
				t.Fatal("命名连接失败日志不应落到默认 Logger")
			}
		})
	}
}
