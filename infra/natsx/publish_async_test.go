package natsx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishJetStreamAsyncOwnershipTraceAndDedup(t *testing.T) {
	buf := captureLogs(t)
	c := registrationClient(t)
	ctx, cancel := context.WithTimeout(logit.WithStart(logit.WithContextLogID(context.Background())), 5*time.Second)
	defer cancel()
	if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "ASYNC",
		Subjects: []string{"async.work"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	header := nats.Header{
		TraceHeader:           []string{"original-trace"},
		jetstream.MsgIDHeader: []string{"original-id"},
		"X-Custom":            []string{"original", "second"},
	}
	data := []byte("original")
	if _, err := c.PublishJetStreamAsync(ctx, "async.work", data, "", header); err == nil {
		t.Fatal("未拒绝空 ID")
	}
	if _, err := c.PublishJetStreamAsync(ctx, "async.work", data, "event", header, nil); err == nil {
		t.Fatal("未拒绝多个 Header")
	}
	future, err := c.PublishJetStreamAsync(ctx, "async.work", data, "event", header)
	if err != nil {
		t.Fatal(err)
	}
	if header.Get(TraceHeader) != "original-trace" || header.Get(jetstream.MsgIDHeader) != "original-id" {
		t.Fatal("异步发布修改了调用方 Header")
	}
	data[0] = 'X'
	header["X-Custom"][0] = "changed"
	header.Set("X-Added", "after-return")
	id, _ := logit.LogIDFromContext(ctx)
	if msg := future.Msg(); string(msg.Data) != "original" || msg.Header.Get("X-Custom") != "original" || msg.Header.Get(TraceHeader) != id || msg.Header.Get(jetstream.MsgIDHeader) != "event" || msg.Header.Get("X-Added") != "" {
		t.Fatal("SDK 持有的消息未独立复制或缺少追踪")
	}
	if values := future.Msg().Header.Values("X-Custom"); len(values) != 2 || values[1] != "second" {
		t.Fatalf("业务 Header 多值丢失: %v", values)
	}
	select {
	case <-future.Ok():
	case err := <-future.Err():
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	duplicate, err := c.PublishJetStreamAsync(ctx, "async.work", nil, "event")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case ack := <-duplicate.Ok():
		if !ack.Duplicate {
			t.Fatal("相同 ID 未去重")
		}
	default:
		t.Fatal("Close 返回时确认仍未完成")
	}
	logit.InfoDuration(ctx, "async summary")
	var submissions int
	for _, record := range readLogs(t, buf) {
		details, _ := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["operation"] == "jetStreamPublishAsync" {
			submissions++
			if details["ack"] != nil {
				t.Fatal("提交日志不应声称取得 ACK")
			}
		}
		if record["msg"] == "async summary" && (record["NATS_registration_2_duration_ms"] == nil || record["NATS_registration_3_duration_ms"] != nil) {
			t.Fatal("异步耗时未按实际提交登记")
		}
	}
	if submissions != 2 {
		t.Fatalf("提交日志数量错误: %d", submissions)
	}
	if _, err := c.PublishJetStreamAsync(ctx, "async.work", nil, "after-close"); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后未拒绝发布: %v", err)
	}
}

func TestPublishJetStreamAsyncCloseWaitsForError(t *testing.T) {
	defaultLogs := captureLogs(t)
	buf := captureNamedLogs(t, t.Name())
	c, err := New(Config{
		Name:       "async-error",
		LoggerName: t.Name(),
		URLs:       []string{startServer(t)},
		JetStream:  JetStreamConfig{PublishAsyncTimeout: 100 * time.Millisecond},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "NOACK",
		Subjects: []string{"noack"},
		Storage:  jetstream.MemoryStorage,
		NoAck:    true,
	}); err != nil {
		t.Fatal(err)
	}
	msgCtx := logit.WithContextLogID(ctx)
	logID, _ := logit.LogIDFromContext(msgCtx)
	future, err := c.PublishJetStreamAsync(msgCtx, "noack", nil, "event")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-future.Err():
		if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
			t.Fatalf("启动 ctx 不应取消确认: %v", err)
		}
	default:
		t.Fatal("Close 未等到异步结果")
	}
	for _, record := range readLogs(t, defaultLogs) {
		if record["nats_event"] != nil {
			t.Fatalf("异步错误和连接日志不得落到默认 Logger: %v", record)
		}
	}
	for _, record := range readLogs(t, buf) {
		if record["nats_event"] == "async_publish_error" {
			if record["message_id"] != "event" || record[logit.LogId] != logID {
				t.Fatalf("异步失败日志未关联原消息: %v", record)
			}
			return
		}
	}
	t.Fatal("缺少异步错误日志")
}

func TestCloseTimeoutResolvesPendingAsyncPublish(t *testing.T) {
	c, err := New(Config{
		Name:         "async-close-timeout",
		URLs:         []string{startServer(t)},
		CloseTimeout: 100 * time.Millisecond,
		JetStream: JetStreamConfig{
			PublishAsyncTimeout: time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c.JetStream().CleanupPublisher()
		_ = c.Close()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
		Name:     "NOACK",
		Subjects: []string{"noack"},
		Storage:  jetstream.MemoryStorage,
		NoAck:    true,
	}); err != nil {
		t.Fatal(err)
	}
	future, err := c.PublishJetStreamAsync(ctx, "noack", nil, "event")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("关闭应保留整体超时错误: %v", err)
	}
	select {
	case err := <-future.Err():
		if !errors.Is(err, jetstream.ErrJetStreamPublisherClosed) {
			t.Fatalf("强制关闭后异步结果错误: %v", err)
		}
	default:
		t.Fatal("Close 返回后 future 仍悬挂，等待原有确认超时")
	}
	if pending := c.JetStream().PublishAsyncPending(); pending != 0 {
		t.Fatalf("强制关闭仍保留 %d 条待确认消息", pending)
	}
}
