package natsx

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCloseAllowsCallbackOperations(t *testing.T) {
	for _, mode := range []string{"core", "pull"} {
		t.Run(mode, func(t *testing.T) {
			url := startServer(t)
			c, err := New(Config{
				Name:         "draining",
				URLs:         []string{url},
				CloseTimeout: 3 * time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			peer, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close()
			output, err := peer.SubscribeSync("review.output")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := peer.Subscribe("review.request", func(msg *nats.Msg) {
				_ = msg.Respond([]byte("reply"))
			}); err != nil {
				t.Fatal(err)
			}
			if err := peer.Flush(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			setupJS, err := jetstream.New(peer)
			if err != nil {
				t.Fatal(err)
			}
			_, err = setupJS.CreateStream(ctx, jetstream.StreamConfig{
				Name:     "REVIEW",
				Subjects: []string{"review.input", "review.js", "review.async"},
				Storage:  jetstream.MemoryStorage,
			})
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := setupJS.CreateConsumer(ctx, "REVIEW", jetstream.ConsumerConfig{
				Durable:       "worker",
				FilterSubject: "review.input",
				AckPolicy:     jetstream.AckExplicitPolicy,
			})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "pull" {
				consumer, err = c.JetStream().Consumer(ctx, "REVIEW", "worker")
				if err != nil {
					t.Fatal(err)
				}
			}
			entered := make(chan struct{})
			release := make(chan struct{})
			defer close(release)
			result := make(chan error, 1)
			var future jetstream.PubAckFuture
			work := func(msgCtx context.Context) error {
				close(entered)
				<-release
				errPublish := c.Publish(msgCtx, "review.output", nil)
				reply, errRequest := c.Request(msgCtx, "review.request", nil, nil)
				if errRequest == nil && string(reply.Data) != "reply" {
					errRequest = errors.New("响应内容错误")
				}
				_, errJS := c.PublishJetStream(msgCtx, "review.js", nil, "review-event")
				var errAsync error
				future, errAsync = c.JetStream().PublishAsync("review.async", nil)
				return errors.Join(errPublish, errRequest, errJS, errAsync)
			}
			if mode == "core" {
				_, err = c.Subscribe("review.input", func(msgCtx context.Context, _ *nats.Msg) {
					result <- work(msgCtx)
				})
			} else {
				err = c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(msgCtx context.Context, msg jetstream.Msg) {
					result <- errors.Join(work(msgCtx), msg.Ack())
				})
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Publish(ctx, "review.input", nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("回调未开始")
			}
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			for !c.closing.Load() {
				select {
				case <-ctx.Done():
					t.Fatal("关闭未开始")
				case <-time.After(time.Millisecond):
				}
			}
			if _, err := c.Subscribe("review.new", func(context.Context, *nats.Msg) {}); !errors.Is(err, ErrClosed) {
				t.Fatalf("关闭期间仍允许新订阅: %v", err)
			}
			if err := c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(context.Context, jetstream.Msg) {}); !errors.Is(err, ErrClosed) {
				t.Fatalf("关闭期间仍允许新消费: %v", err)
			}
			release <- struct{}{}
			if err := <-result; err != nil {
				t.Errorf("排空中的回调操作失败: %v", err)
			}
			if err := <-closed; err != nil {
				t.Errorf("Close: %v", err)
			}
			if future != nil {
				select {
				case <-future.Ok():
				default:
					t.Error("Close 未等待回调提交的异步 PubAck")
				}
			}
			if _, err := output.NextMsg(time.Second); err != nil {
				t.Errorf("下游未收到 Core 消息: %v", err)
			}
			if err := c.Publish(ctx, "review.output", nil); !errors.Is(err, ErrClosed) {
				t.Errorf("关闭后仍接受发布: %v", err)
			}
		})
	}
}

func TestCloseAfterSubscriptionStopped(t *testing.T) {
	for _, mode := range []string{"unsubscribe", "drain"} {
		t.Run(mode, func(t *testing.T) {
			c, err := New(Config{
				Name:         "core-stopped",
				URLs:         []string{startServer(t)},
				CloseTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			sub, err := c.Subscribe("temporary", func(context.Context, *nats.Msg) {})
			if err != nil {
				t.Fatal(err)
			}
			received := make(chan struct{}, 1)
			active, err := c.Subscribe("active", func(context.Context, *nats.Msg) {
				received <- struct{}{}
			})
			if err != nil {
				t.Fatal(err)
			}
			userClosed := make(chan struct{})
			sub.SetClosedHandler(func(string) { close(userClosed) })
			if mode == "unsubscribe" {
				err = sub.Unsubscribe()
			} else {
				err = sub.Drain()
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-userClosed:
			case <-time.After(time.Second):
				t.Fatal("调用方关闭回调未执行")
			}
			if err := c.Publish(context.Background(), "active", nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-received:
			case <-time.After(time.Second):
				t.Fatal("提前停止订阅影响其他订阅")
			}
			if err := c.Close(); err != nil {
				t.Fatalf("已停止订阅影响整体关闭: %v", err)
			}
			if !c.Conn().IsClosed() || active.IsValid() {
				t.Fatal("Close 返回后连接或剩余订阅仍未关闭")
			}
		})
	}
}

func TestCloseTimeoutBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name         string
		managed      bool
		releaseEarly bool
		wantErr      error
		wantLogErr   error
	}{
		{
			name:         "core_completes_after_drain_timeout",
			managed:      true,
			releaseEarly: true,
		},
		{
			name:    "core_reaches_close_timeout",
			managed: true,
			wantErr: ErrCloseTimeout,
		},
		{
			name:       "native_subscription_reaches_drain_timeout",
			wantLogErr: nats.ErrDrainTimeout,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			cfg := Config{
				Name:         "close-timeouts",
				URLs:         []string{startServer(t)},
				DrainTimeout: 50 * time.Millisecond,
				CloseTimeout: 2 * time.Second,
			}
			c, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			entered := make(chan struct{})
			release := make(chan struct{}, 1)
			defer close(release)
			handler := func(*nats.Msg) {
				close(entered)
				<-release
			}
			if tc.managed {
				_, err = c.Subscribe("blocked", func(_ context.Context, msg *nats.Msg) {
					handler(msg)
				})
			} else {
				// 原生订阅由最终 nc.Drain 排空，用于验证官方 DrainTimeout 仍生效。
				_, err = c.Conn().Subscribe("blocked", handler)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := c.Publish(context.Background(), "blocked", nil); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("阻塞回调未开始")
			}
			started := time.Now()
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			if tc.managed {
				// 托管消费超过底层 DrainTimeout 后仍应继续等待。
				select {
				case err := <-closed:
					t.Fatalf("托管消费被底层 DrainTimeout 提前结束: %v", err)
				case <-time.After(3 * cfg.DrainTimeout):
				}
			}
			if tc.releaseEarly {
				release <- struct{}{}
			}
			select {
			case err := <-closed:
				if !errors.Is(err, tc.wantErr) || !c.Conn().IsClosed() {
					t.Fatalf("关闭结果: err=%v want=%v closed=%v", err, tc.wantErr, c.Conn().IsClosed())
				}
				if errors.Is(err, ErrCloseTimeout) && time.Since(started) < cfg.CloseTimeout {
					t.Fatal("整体关闭期限提前耗尽")
				}
			case <-time.After(cfg.CloseTimeout + time.Second):
				t.Fatal("Close 未在整体期限内结束")
			}
			if tc.wantLogErr != nil {
				var found bool
				for _, record := range readLogs(t, buf) {
					if record["nats_event"] == "close_error" && record["level"] == "ERROR" && record["err"] == tc.wantLogErr.Error() {
						found = true
					}
				}
				if !found {
					t.Fatalf("缺少关闭错误日志: %s", buf.String())
				}
			}
		})
	}
}

func TestCloseDoesNotLogOtherLastErrors(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{
		Name: "close-last-error",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sub, err := c.Conn().SubscribeSync("close.slow")
	if err != nil {
		t.Fatal(err)
	}
	if err := sub.SetPendingLimits(1, -1); err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := c.Conn().Publish("close.slow", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().LastError(); !errors.Is(err, nats.ErrSlowConsumer) {
		t.Fatalf("未触发非 Drain 错误: %v", err)
	}
	if err := sub.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("连接最近错误不应作为关闭结果返回: %v", err)
	}
	var count int
	for _, record := range readLogs(t, buf) {
		if record["nats_event"] == "close_error" && record["level"] == "ERROR" && record["err"] == nats.ErrSlowConsumer.Error() {
			count++
		}
	}
	if count != 0 {
		t.Fatalf("非超时错误不应记录 close_error: %s", buf.String())
	}
}

func TestCloseLogsPublishDrainTimeout(t *testing.T) {
	buf := captureLogs(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		if _, err := io.WriteString(conn, "INFO {\"server_id\":\"drain-timeout\",\"proto\":1,\"headers\":true}\r\n"); err != nil {
			return
		}
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if line == "PING\r\n" {
				if _, err := io.WriteString(conn, "PONG\r\n"); err != nil {
					return
				}
				break
			}
		}
		// 完成首次握手后不再回复 PING，让最终连接 FlushTimeout 真实超时。
		_, _ = io.Copy(io.Discard, reader)
	}()
	c, err := New(Config{
		Name:         "publish-drain-timeout",
		URLs:         []string{"nats://" + listener.Addr().String()},
		CloseTimeout: 8 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Close(); err != nil {
		t.Fatalf("发布排空超时不应作为关闭结果返回: %v", err)
	}
	if !errors.Is(c.Conn().LastError(), nats.ErrTimeout) || !c.Conn().IsClosed() {
		t.Fatalf("未触发发布排空超时: err=%v closed=%v", c.Conn().LastError(), c.Conn().IsClosed())
	}
	var count int
	for _, record := range readLogs(t, buf) {
		if record["nats_event"] == "close_error" && record["level"] == "ERROR" && record["err"] == nats.ErrTimeout.Error() {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("发布排空超时日志数量=%d，期望 1: %s", count, buf.String())
	}
}

func TestConsumeWithWorkersRuntimeErrorLogsAndCallback(t *testing.T) {
	for _, withCallback := range []bool{false, true} {
		t.Run(fmt.Sprint(withCallback), func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(Config{Name: "runtime-error", URLs: []string{startServer(t)}})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
				Name:     "ERRORS",
				Subjects: []string{"errors.*"},
				Storage:  jetstream.MemoryStorage,
			})
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := c.JetStream().CreateConsumer(ctx, "ERRORS", jetstream.ConsumerConfig{
				Durable:   "worker",
				AckPolicy: jetstream.AckExplicitPolicy,
			})
			if err != nil {
				t.Fatal(err)
			}
			called := make(chan error, 1)
			cfg := ConsumeConfig{Workers: 2}
			if withCallback {
				cfg.ErrorHandler = func(_ jetstream.ConsumeContext, err error) {
					called <- err
				}
			}
			err = c.ConsumeWithWorkers(consumer, cfg, func(context.Context, jetstream.Msg) {})
			if err != nil {
				t.Fatal(err)
			}
			handle := lastWorkerHandle(t, c)
			if err := c.Conn().Flush(); err != nil {
				t.Fatal(err)
			}
			if err := c.JetStream().DeleteConsumer(ctx, "ERRORS", "worker"); err != nil {
				t.Fatal(err)
			}
			select {
			case <-handle.Closed():
			case <-ctx.Done():
				t.Fatal("删除 Consumer 后未停止")
			}
			if withCallback {
				select {
				case err := <-called:
					if !errors.Is(err, jetstream.ErrConsumerDeleted) {
						t.Fatalf("用户回调错误不正确: %v", err)
					}
				default:
					t.Error("用户错误回调未执行")
				}
			}
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, record := range readLogs(t, buf) {
				if record["nats_event"] == "consume_error" && record["level"] == "ERROR" &&
					record["stream"] == "ERRORS" && record["consumer"] == "worker" &&
					record["err"] == jetstream.ErrConsumerDeleted.Error() {
					found = true
				}
			}
			if !found {
				t.Fatalf("缺少可定位的消费错误日志: %s", buf.String())
			}
		})
	}
}

type failingDialer struct{ err error }

func (d failingDialer) Dial(string, string) (net.Conn, error) {
	return nil, d.err
}

func TestConnectErrorPreservesOriginalError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"auth", nats.ErrAuthorization},
		{"no_servers", nats.ErrNoServers},
		{"timeout", &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}},
		{"unknown", errors.New("private-cause")},
		{"eof", io.EOF},
		{"canceled", context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			rawErr := fmt.Errorf("nats://user:private-password@127.0.0.1:4222: %w", tc.cause)
			_, err := New(Config{
				Name: "raw-error",
				URLs: []string{"nats://user:private-password@127.0.0.1:4222"},
			}, nats.SetCustomDialer(failingDialer{
				err: rawErr,
			}))
			if !errors.Is(err, ErrConnect) || !errors.Is(err, rawErr) || !errors.Is(err, tc.cause) {
				t.Fatalf("丢失原始错误链: got=%v want=%v", err, rawErr)
			}
			if !strings.Contains(err.Error(), rawErr.Error()) {
				t.Fatalf("未保留原始错误文本: %v", err)
			}
			if want, ok := tc.cause.(*net.OpError); ok {
				got, ok := errors.AsType[*net.OpError](err)
				if !ok || got != want {
					t.Fatalf("丢失原始网络错误: %v", err)
				}
			}
			var found bool
			for _, record := range readLogs(t, buf) {
				if record["nats_event"] == "connect_failed" && record["err"] == rawErr.Error() {
					found = true
				}
			}
			if !found {
				t.Fatalf("缺少原始连接错误日志: %s", buf.String())
			}
		})
	}
}

func TestConnectionOptionPreservesOriginalError(t *testing.T) {
	rawErr := errors.New("private-option-error")
	_, err := New(Config{
		Name: "invalid-option",
		URLs: []string{nats.DefaultURL},
	}, func(*nats.Options) error {
		return rawErr
	})
	if !errors.Is(err, rawErr) || !strings.Contains(err.Error(), rawErr.Error()) {
		t.Fatalf("未保留原始选项错误: %v", err)
	}
}

func TestRequestErrorLogsOriginalError(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{
		Name: "request-error",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = c.Request(ctx, "missing.reply", nil, nil)
	if !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("Request = %v, want no responders", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, record := range readLogs(t, buf) {
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if ok && details["operation"] == "coreRequest" && details["err"] == err.Error() {
			return
		}
	}
	t.Fatalf("缺少原始请求错误日志: %s", buf.String())
}
