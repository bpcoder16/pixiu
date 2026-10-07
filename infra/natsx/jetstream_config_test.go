package natsx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestNewRejectsNegativePublishAsyncTimeout(t *testing.T) {
	dialer := &authCountingDialer{}
	c, err := New(Config{
		Name: "async-timeout",
		URLs: []string{nats.DefaultURL},
		JetStream: JetStreamConfig{
			PublishAsyncTimeout: -time.Second,
		},
	}, nats.SetCustomDialer(dialer))
	if c != nil || err == nil || errors.Is(err, ErrConnect) || !strings.Contains(err.Error(), "PublishAsyncTimeout") {
		t.Fatalf("应返回异步发布超时配置错误: client=%v, err=%v", c, err)
	}
	if dialer.calls != 0 {
		t.Fatalf("无效配置触发了 %d 次拨号", dialer.calls)
	}
}

func TestJetStreamPublishAsyncTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		timeout time.Duration
		want    time.Duration
	}{
		{name: "default", want: 5 * time.Second},
		{name: "custom", timeout: 100 * time.Millisecond, want: 100 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(Config{
				Name: "async-timeout",
				URLs: []string{startServer(t)},
				JetStream: JetStreamConfig{
					PublishAsyncTimeout: tc.timeout,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				// 断言失败时也释放待确认消息，避免清理阶段等待整个关闭期限。
				c.JetStream().CleanupPublisher()
				_ = c.Close()
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err = c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
				Name:     "ASYNC_TIMEOUT",
				Subjects: []string{"async.timeout"},
				Storage:  jetstream.MemoryStorage,
				// 接收消息但不返回 PubAck，验证客户端等待确认的期限。
				NoAck: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			future, err := c.JetStream().PublishAsync("async.timeout", []byte("message"))
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-future.Err():
				if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
					t.Fatalf("应返回异步发布确认超时: %v", err)
				}
			case <-future.Ok():
				t.Fatal("未返回 PubAck 却报告发布成功")
			case <-time.After(tc.want + 3*time.Second):
				t.Fatal("未按配置结束异步发布确认等待")
			}
			if elapsed := time.Since(started); elapsed < tc.want {
				t.Fatalf("异步发布过早超时: %v < %v", elapsed, tc.want)
			}
			if pending := c.JetStream().PublishAsyncPending(); pending != 0 {
				t.Fatalf("超时后仍有 %d 条消息等待确认", pending)
			}
			select {
			case <-c.JetStream().PublishAsyncComplete():
			default:
				t.Fatal("超时后异步发布仍未完成")
			}
			if err = c.Close(); err != nil {
				t.Fatalf("异步发布超时后关闭失败: %v", err)
			}
			for _, record := range readLogs(t, buf) {
				if record["nats_event"] == "async_publish_error" && record["subject"] == "async.timeout" &&
					record["level"] == "ERROR" && record["err"] == jetstream.ErrAsyncPublishTimeout.Error() {
					return
				}
			}
			t.Fatalf("缺少 Pixiu 异步发布超时日志: %s", buf.String())
		})
	}
}
