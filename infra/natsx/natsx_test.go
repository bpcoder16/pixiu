package natsx

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startServer(t *testing.T) string {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		s.Shutdown()
		t.Fatal("NATS 未就绪")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	return s.ClientURL()
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	l := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
	old := logit.Default()
	logit.SetDefault(l)
	t.Cleanup(func() {
		logit.SetDefault(old)
		_ = logit.Close(l)
	})
	return buf
}

func readLogs(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("无效日志 %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestNewValidationAndConnectErrorLogs(t *testing.T) {
	buf := captureLogs(t)
	for _, cfg := range []Config{
		{},
		{Name: "worker"},
		{Name: "worker", URLs: []string{""}},
		{Name: "worker", URLs: []string{nats.DefaultURL}, CloseTimeout: -time.Second},
	} {
		if c, err := New(cfg); err == nil || c != nil {
			t.Fatalf("无效配置被接受: %#v, %v", cfg, err)
		}
	}
	if c, err := New(Config{Name: "worker", URLs: []string{startServer(t)}}, nats.NoCallbacksAfterClientClose()); err == nil || c != nil || !strings.Contains(err.Error(), "NoCallbacksAfterClientClose") {
		t.Fatalf("关闭通知被禁用的选项被接受: %v", err)
	}
	_, err := New(Config{Name: "worker", URLs: []string{"nats://user:secret@127.0.0.1:1"}, ConnectTimeout: 50 * time.Millisecond})
	if !errors.Is(err, ErrConnect) {
		t.Fatalf("未返回连接错误: %v", err)
	}
	var found bool
	for _, record := range readLogs(t, buf) {
		if record["nats_event"] == "connect_failed" && record["level"] == "ERROR" && record["err"] != nil {
			found = true
		}
	}
	if !found {
		t.Fatalf("缺少连接失败日志: %s", buf.String())
	}
}

func TestNewRejectsServerURLOptionsBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name    string
		url     string
		servers []string
	}{
		{
			name: "url",
			url:  "nats://private-user:private-password@127.0.0.1:4223",
		},
		{
			name:    "servers",
			servers: []string{"nats://private-user:private-password@127.0.0.1:4223"},
		},
		{
			name:    "both",
			url:     "nats://127.0.0.1:4223",
			servers: []string{"nats://127.0.0.1:4224"},
		},
		{
			name:    "same_as_config",
			url:     nats.DefaultURL,
			servers: []string{nats.DefaultURL},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			dialer := &authCountingDialer{}
			c, err := New(Config{
				Name: "url-options",
				URLs: []string{nats.DefaultURL},
			}, func(opts *nats.Options) error {
				opts.Url = tc.url
				opts.Servers = tc.servers
				return nil
			}, nats.SetCustomDialer(dialer))
			if c != nil {
				_ = c.Close()
				t.Fatal("地址选项意外建立连接")
			}
			if err == nil || errors.Is(err, ErrConnect) || dialer.calls != 0 {
				t.Fatalf("未在拨号前拒绝地址选项: err=%v, dials=%d", err, dialer.calls)
			}
			if !strings.Contains(err.Error(), "Config.URLs") {
				t.Fatalf("错误未提示地址配置入口: %v", err)
			}
			if strings.Contains(err.Error()+buf.String(), "private-") || strings.Contains(err.Error()+buf.String(), "nats://") {
				t.Fatal("地址配置错误或日志泄露凭据或完整 URL")
			}
		})
	}
}

func TestNewRejectsConnectionCallbacksBeforeDial(t *testing.T) {
	for _, tc := range []struct {
		name   string
		option nats.Option
	}{
		{
			name:   "DisconnectedErrCB",
			option: nats.DisconnectErrHandler(func(*nats.Conn, error) {}),
		},
		{
			name:   "DisconnectedCB",
			option: nats.DisconnectHandler(func(*nats.Conn) {}),
		},
		{
			name:   "ReconnectedCB",
			option: nats.ReconnectHandler(func(*nats.Conn) {}),
		},
		{
			name:   "ClosedCB",
			option: nats.ClosedHandler(func(*nats.Conn) {}),
		},
		{
			name:   "AsyncErrorCB",
			option: nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialer := &authCountingDialer{}
			c, err := New(Config{
				Name: "callback-options",
				URLs: []string{nats.DefaultURL},
			}, tc.option, nats.SetCustomDialer(dialer))
			if c != nil {
				_ = c.Close()
				t.Fatal("自定义连接回调意外建立连接")
			}
			if err == nil || errors.Is(err, ErrConnect) || dialer.calls != 0 {
				t.Fatalf("未在拨号前拒绝自定义连接回调: err=%v, dials=%d", err, dialer.calls)
			}
			if !strings.Contains(err.Error(), "managed by pixiu") {
				t.Fatalf("错误未说明连接回调归属: %v", err)
			}
		})
	}
}

func TestNewConnectionOptions(t *testing.T) {
	serverURL := startServer(t)
	for _, tc := range []struct {
		name             string
		options          []nats.Option
		maxReconnect     int
		pingInterval     time.Duration
		reconnectBufSize int
	}{
		{
			name:             "defaults",
			maxReconnect:     -1,
			pingInterval:     20 * time.Second,
			reconnectBufSize: 16 * 1024 * 1024,
		},
		{
			name: "overrides",
			options: []nats.Option{
				nats.MaxReconnects(3),
				nats.PingInterval(30 * time.Second),
				nats.ReconnectBufSize(32 * 1024 * 1024),
			},
			maxReconnect:     3,
			pingInterval:     30 * time.Second,
			reconnectBufSize: 32 * 1024 * 1024,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			c, err := New(Config{
				Name: "connection-options",
				URLs: []string{serverURL},
			}, tc.options...)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if got := c.Conn().Opts; got.MaxReconnect != tc.maxReconnect || got.PingInterval != tc.pingInterval {
				t.Errorf("连接选项未生效: MaxReconnect=%d PingInterval=%s", got.MaxReconnect, got.PingInterval)
			}
			if got := c.Conn().Opts.ReconnectBufSize; got != tc.reconnectBufSize {
				t.Errorf("重连发送缓冲选项未生效: got=%d want=%d", got, tc.reconnectBufSize)
			}
			_, err = c.Subscribe("connection.reply", func(_ context.Context, msg *nats.Msg) {
				_ = msg.Respond([]byte("reply"))
			})
			if err != nil {
				t.Fatal(err)
			}
			// 通过状态通知观察真实连接重建，不占用 Pixiu 管理的回调。
			reconnected := c.Conn().StatusChanged(nats.CONNECTED)
			if err := c.Conn().ForceReconnect(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			select {
			case <-reconnected:
			case <-ctx.Done():
				t.Fatal("未收到重连通知")
			}
			reply, err := c.Request(ctx, "connection.reply", nil, nil)
			if err != nil || string(reply.Data) != "reply" {
				t.Fatalf("重连后未恢复启动阶段的订阅: reply=%v err=%v", reply, err)
			}
			if err := c.Close(); err != nil {
				t.Fatalf("重连后关闭失败: %v", err)
			}
			seen := make(map[string]bool)
			for _, record := range readLogs(t, buf) {
				if event, ok := record["nats_event"].(string); ok {
					seen[event] = true
				}
			}
			for _, event := range []string{"disconnected", "reconnected", "closed"} {
				if !seen[event] {
					t.Errorf("缺少连接状态日志: %s", event)
				}
			}
		})
	}
}

func TestCorePublishRequestTraceAndFullLogs(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{Name: "orders", URLs: []string{startServer(t)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := logit.WithContext(context.Background())
	logit.AddMeta(ctx, logit.Str(logit.LogId, "trace-123"))
	received := make(chan *nats.Msg, 1)
	_, err = c.Subscribe("orders.created", func(_ context.Context, m *nats.Msg) {
		received <- m
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.QueueSubscribe("orders.reply", "order-workers", func(_ context.Context, m *nats.Msg) {
		if m.Header.Get(TraceHeader) != "trace-123" || string(m.Data) != "private-request" {
			t.Errorf("请求未传递追踪 ID 或消息内容: %v", m)
		}
		_ = m.RespondMsg(&nats.Msg{
			Data: []byte("private-reply"),
			Header: nats.Header{
				"X-Response": []string{"first", "second"},
			},
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	header := nats.Header{
		"X-Source":    []string{"service"},
		"x-log-id":    []string{"stale-trace"},
		"Nats-Log-Id": []string{"other-header"},
	}
	extraHeader := nats.Header{}
	if err := c.Publish(ctx, "orders.created", nil, header, extraHeader); err == nil {
		t.Fatal("不应接受多个 Header")
	}
	if header.Get(TraceHeader) != "" || len(extraHeader) != 0 {
		t.Fatal("拒绝多个 Header 时不应修改调用方数据")
	}
	if err := c.Publish(ctx, "orders.created", []byte("private-payload"), header); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if string(got.Data) != "private-payload" {
			t.Fatalf("消息内容错误: %q", got.Data)
		}
		if got.Header.Get(TraceHeader) != "trace-123" || got.Header.Get("X-Source") != "service" || got.Header.Get("Nats-Log-Id") != "other-header" || got.Header.Get("x-log-id") != "stale-trace" {
			t.Fatalf("Header 丢失: %v", got.Header)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到 Core 消息")
	}
	if header.Get(TraceHeader) != "trace-123" || header.Get("x-log-id") != "stale-trace" || header.Get("Nats-Log-Id") != "other-header" {
		t.Fatal("未原地写入追踪 ID 或修改了其他 Header")
	}
	for _, headers := range [][]nats.Header{nil, {nil}} {
		if err := c.Publish(ctx, "orders.created", nil, headers...); err != nil {
			t.Fatal(err)
		}
		select {
		case got := <-received:
			if got.Header.Get(TraceHeader) != "trace-123" || got.Header.Get("X-Source") != "" || len(got.Data) != 0 {
				t.Fatalf("省略 Header 或显式传 nil 时消息错误: %v", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("未收到无业务 Header 的消息")
		}
	}
	reqCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	reply, err := c.Request(reqCtx, "orders.reply", []byte("private-request"), nil)
	if err != nil || string(reply.Data) != "private-reply" {
		t.Fatalf("请求结果: %v, %v", reply, err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	var foundPublish, foundRequest, foundCallback bool
	for _, record := range readLogs(t, buf) {
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok {
			continue
		}
		if record["msg"] != "NATS" || record[logit.DownstreamTypeKey] != "NATS" || record[logit.DownstreamIDKey] != "orders" {
			t.Fatalf("统一下游标识错误: %v", record)
		}
		if _, ok := details["bytes"]; ok {
			t.Fatalf("不应只记录正文长度: %v", details)
		}
		if _, ok := details["reply_bytes"]; ok {
			t.Fatalf("不应记录响应长度字段: %v", details)
		}
		switch details["operation"] {
		case "corePublish", "coreCallback":
			if details["request_body"] != "private-payload" {
				continue
			}
			headers, ok := details["request_headers"].(map[string]any)
			if !ok || details["request_body_encoding"] != "utf-8" || headers["X-Source"] == nil || headers[TraceHeader] == nil || headers["x-log-id"] == nil || headers["Nats-Log-Id"] == nil {
				t.Fatalf("缺少完整消息 Header: %v", details)
			}
			if details["operation"] == "corePublish" {
				foundPublish = true
			} else {
				foundCallback = true
			}
		case "coreRequest":
			if details["request_body"] != "private-request" || details["response_body"] != "private-reply" || details["response_body_encoding"] != "utf-8" {
				t.Fatalf("缺少完整请求响应正文: %v", details)
			}
			headers, ok := details["response_headers"].(map[string]any)
			if !ok {
				t.Fatalf("缺少响应 Header: %v", details)
			}
			values, ok := headers["X-Response"].([]any)
			if !ok || len(values) != 2 || values[0] != "first" || values[1] != "second" {
				t.Fatalf("响应 Header 值被截断: %v", headers)
			}
			foundRequest = true
		}
	}
	if !foundPublish || !foundRequest || !foundCallback {
		t.Fatalf("缺少完整消息日志: publish=%v request=%v callback=%v: %s", foundPublish, foundRequest, foundCallback, buf.String())
	}
}

func TestRequestOptionalHeaders(t *testing.T) {
	captureLogs(t)
	c, err := New(Config{
		Name: "request-headers",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	received := make(chan *nats.Msg, 4)
	if _, err := c.Subscribe("optional.request", func(_ context.Context, msg *nats.Msg) {
		received <- msg
		if err := msg.Respond([]byte("reply")); err != nil {
			t.Error(err)
		}
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(logit.WithContextLogID(context.Background()), 3*time.Second)
	defer cancel()
	id, _ := logit.LogIDFromContext(ctx)
	header := nats.Header{"X-Source": []string{"service"}}
	extra := nats.Header{}
	if reply, err := c.Request(ctx, "optional.request", nil, header, extra); err == nil || reply != nil {
		t.Fatalf("多个 Header 应在发送前拒绝: reply=%v err=%v", reply, err)
	}
	if header.Get(TraceHeader) != "" || header.Get("X-Source") != "service" || len(extra) != 0 {
		t.Fatal("拒绝多个 Header 时不应修改调用方 Header")
	}
	for _, headers := range [][]nats.Header{nil, {nil}, {header}} {
		reply, err := c.Request(ctx, "optional.request", []byte("request"), headers...)
		if err != nil || reply == nil || string(reply.Data) != "reply" {
			t.Fatalf("可选 Header 请求失败: reply=%v err=%v", reply, err)
		}
		select {
		case msg := <-received:
			if string(msg.Data) != "request" || msg.Header.Get(TraceHeader) != id {
				t.Fatalf("请求正文或追踪 ID 错误: %v", msg)
			}
			wantSource := ""
			if len(headers) == 1 && headers[0] != nil {
				wantSource = "service"
			}
			if msg.Header.Get("X-Source") != wantSource {
				t.Fatalf("请求 Header 错误: %v", msg.Header)
			}
		case <-ctx.Done():
			t.Fatal("未收到请求")
		}
	}
	if header.Get(TraceHeader) != id {
		t.Fatal("提供 Header 时应原地写入追踪 ID")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		t.Fatalf("被拒绝的请求不应发送: %v", msg)
	default:
	}
}

func TestPublishFailureLogsCompleteBinaryMessage(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{
		Name: "binary-log",
		URLs: []string{startServer(t)},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	data := []byte{0xff, 0x00, 0x41}
	err = c.Publish(context.Background(), "invalid subject", data, nats.Header{
		"X-Data": []string{"first", "second"},
	})
	if !errors.Is(err, nats.ErrBadSubject) {
		t.Fatalf("未触发发布失败: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, record := range readLogs(t, buf) {
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok || details["operation"] != "corePublish" {
			continue
		}
		body, ok := details["request_body"].(string)
		if !ok || details["request_body_encoding"] != "base64" || body != base64.StdEncoding.EncodeToString(data) || details["err"] != err.Error() || record["level"] != "ERROR" {
			t.Fatalf("二进制失败消息不完整: %v", details)
		}
		headers, ok := details["request_headers"].(map[string]any)
		if !ok {
			t.Fatalf("失败消息未记录 Header: %v", details)
		}
		values, ok := headers["X-Data"].([]any)
		if !ok || len(values) != 2 || values[0] != "first" || values[1] != "second" || headers[TraceHeader] == nil {
			t.Fatalf("失败消息 Header 不完整: %v", headers)
		}
		return
	}
	t.Fatalf("缺少发布失败日志: %s", buf.String())
}

func TestMessageContextScopesAndTraceHeader(t *testing.T) {
	buf := captureLogs(t)
	type key struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), key{}, "parent"))
	defer cancel()
	parent = logit.WithContext(parent)
	logit.AddField(parent, logit.Str("parent_field", "must-not-leak"))

	first := MessageContext(parent, nats.Header{
		TraceHeader:   []string{"new"},
		"Nats-Log-Id": []string{"old"},
	})
	second := MessageContext(parent, nats.Header{"Nats-Log-Id": []string{"old"}})
	third := MessageContext(parent, nil)
	if got, _ := logit.LogIDFromContext(first); got != "new" {
		t.Fatalf("追踪 Header 未还原: %q", got)
	}
	secondID, _ := logit.LogIDFromContext(second)
	if secondID == "" || secondID == "new" || secondID == "old" {
		t.Fatalf("非追踪 Header 不应作为消息 ID: %q", secondID)
	}
	if got, _ := logit.LogIDFromContext(third); got == "" || got == "new" || got == "old" || got == secondID {
		t.Fatalf("未生成独立消息 ID: %q", got)
	}
	logit.AddField(first, logit.Str("message_field", "first"))
	logit.Info(first, "first")
	logit.Info(second, "second")
	if first.Value(key{}) != "parent" || second.Value(key{}) != "parent" {
		t.Fatal("丢失父级 context 值")
	}
	cancel()
	if first.Err() != context.Canceled || second.Err() != context.Canceled {
		t.Fatal("消息 context 没有继承取消")
	}
	records := readLogs(t, buf)
	if len(records) != 2 || records[0][logit.LogId] != "new" || records[1][logit.LogId] != secondID {
		t.Fatalf("日志追踪值错误: %v", records)
	}
	if records[1]["message_field"] != nil || records[0]["parent_field"] != nil {
		t.Fatalf("消息字段跨作用域污染: %v", records)
	}
}

func TestJetStreamPublishConsumeAndCloseWaits(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{Name: "jobs", URLs: []string{startServer(t)}, CloseTimeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js := c.JetStream()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "JOBS", Subjects: []string{"jobs.*"}, Storage: jetstream.MemoryStorage, Duplicates: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, "JOBS", jetstream.ConsumerConfig{Durable: "worker", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan string, 1)
	release := make(chan struct{})
	_, err = c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(msgCtx context.Context, msg jetstream.Msg) {
		id, _ := logit.LogIDFromContext(msgCtx)
		entered <- id
		<-release
		_ = msg.Ack()
	})
	if err != nil {
		t.Fatal(err)
	}
	pubCtx := logit.WithContext(ctx)
	logit.AddMeta(pubCtx, logit.Str(logit.LogId, "job-trace"))
	header := nats.Header{}
	ack, err := c.PublishJetStream(pubCtx, "jobs.run", []byte("private-job"), "job-1", header)
	if err != nil || ack == nil || ack.Duplicate {
		t.Fatalf("首条 PubAck: %v, %v", ack, err)
	}
	if header.Get(TraceHeader) != "job-trace" || header.Get(jetstream.MsgIDHeader) != "job-1" {
		t.Errorf("JetStream 未原地写入追踪 ID 和消息 ID: %v", header)
	}
	ack, err = c.PublishJetStream(pubCtx, "jobs.run", []byte("private-job"), "job-1")
	if err != nil || ack == nil || !ack.Duplicate {
		t.Fatalf("重复 PubAck: %v, %v", ack, err)
	}
	select {
	case id := <-entered:
		if id != "job-trace" {
			t.Fatalf("消费端追踪 ID: %q", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("未收到 JetStream 消息")
	}
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	closedAgain := make(chan error, 1)
	go func() { closedAgain <- c.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("回调未完成即返回: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case err := <-closedAgain:
		t.Fatalf("并发 Close 未等待同一轮关闭: %v", err)
	default:
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close 未结束")
	}
	if err := <-closedAgain; err != nil {
		t.Fatalf("并发 Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("重复 Close: %v", err)
	}
	if err := c.Publish(ctx, "jobs.run", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后发布结果: %v", err)
	}
	var foundPublish, foundDuplicate, foundCallback bool
	for _, record := range readLogs(t, buf) {
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok || details["request_body"] != "private-job" {
			continue
		}
		headers, ok := details["request_headers"].(map[string]any)
		if !ok || headers[jetstream.MsgIDHeader] == nil || headers[TraceHeader] == nil {
			t.Fatalf("JetStream 消息 Header 不完整: %v", details)
		}
		switch details["operation"] {
		case "jetStreamPublish":
			ack, ok := details["ack"].(map[string]any)
			if !ok || ack["stream"] != "JOBS" || ack["seq"] != float64(1) {
				t.Fatalf("丢失 PubAck 信息: %v", details)
			}
			for _, key := range []string{"stream", "sequence", "duplicate"} {
				if _, ok := details[key]; ok {
					t.Fatalf("不应重复记录平铺 PubAck 字段 %q: %v", key, details)
				}
			}
			if ack["duplicate"] == true {
				foundDuplicate = true
			}
			foundPublish = true
		case "jetStreamCallback":
			foundCallback = true
		}
	}
	if !foundPublish || !foundDuplicate || !foundCallback {
		t.Fatalf("缺少 JetStream 完整消息日志: publish=%v duplicate=%v callback=%v", foundPublish, foundDuplicate, foundCallback)
	}
}

func TestCloseTimeoutForBlockedCallback(t *testing.T) {
	c, err := New(Config{Name: "blocked", URLs: []string{startServer(t)}, CloseTimeout: 120 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	_, err = c.Subscribe("blocked.work", func(context.Context, *nats.Msg) {
		close(entered)
		<-release
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Conn().Flush(); err != nil {
		t.Fatal(err)
	}
	if err := c.Publish(context.Background(), "blocked.work", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("回调未开始")
	}
	if err := c.Close(); !errors.Is(err, ErrCloseTimeout) || !c.Conn().IsClosed() {
		t.Fatalf("阻塞回调的关闭结果: %v, closed=%v", err, c.Conn().IsClosed())
	}
	close(release)
}

func TestCloseAfterConsumerStopped(t *testing.T) {
	for _, mode := range []string{"stop", "drain"} {
		t.Run(mode, func(t *testing.T) {
			c, err := New(Config{
				Name:         "temporary",
				URLs:         []string{startServer(t)},
				CloseTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			js := c.JetStream()
			_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
				Name:     "TEMP",
				Subjects: []string{"temp.*"},
				Storage:  jetstream.MemoryStorage,
			})
			if err != nil {
				t.Fatal(err)
			}
			consumer, err := js.CreateOrUpdateConsumer(ctx, "TEMP", jetstream.ConsumerConfig{
				Durable:   "temporary",
				AckPolicy: jetstream.AckExplicitPolicy,
			})
			if err != nil {
				t.Fatal(err)
			}
			handle, err := c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(context.Context, jetstream.Msg) {})
			if err != nil {
				t.Fatal(err)
			}
			if mode == "stop" {
				handle.Stop()
			} else {
				handle.Drain()
			}
			select {
			case <-handle.Closed():
			case <-ctx.Done():
				t.Fatal("消费句柄未停止")
			}
			if err := c.Close(); err != nil {
				t.Fatalf("已停止消费句柄影响整体关闭: %v", err)
			}
			if !c.Conn().IsClosed() {
				t.Fatal("Close 返回后连接仍未关闭")
			}
		})
	}
}

func TestConsumeDoesNotAutoAck(t *testing.T) {
	c, err := New(Config{Name: "redelivery", URLs: []string{startServer(t)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	js := c.JetStream()
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{Name: "REDELIVER", Subjects: []string{"redeliver.*"}, Storage: jetstream.MemoryStorage})
	if err != nil {
		t.Fatal(err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, "REDELIVER", jetstream.ConsumerConfig{
		Durable: "redelivery", AckPolicy: jetstream.AckExplicitPolicy,
		AckWait: 150 * time.Millisecond, MaxDeliver: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan int32, 2)
	var count atomic.Int32
	handle, err := c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(_ context.Context, msg jetstream.Msg) {
		current := count.Add(1)
		if current == 2 {
			_ = msg.Ack()
		}
		delivered <- current
	})
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Stop()
	if _, err := c.PublishJetStream(ctx, "redeliver.work", nil, "redeliver-event"); err != nil {
		t.Fatal(err)
	}
	for want := int32(1); want <= 2; want++ {
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("投递次序: got=%d want=%d", got, want)
			}
		case <-ctx.Done():
			t.Fatal("未 ACK 消息没有重投")
		}
	}
}

func TestConnectionAsyncErrorHandlesNilSubscription(t *testing.T) {
	buf := captureLogs(t)
	c, err := New(Config{Name: "events", URLs: []string{startServer(t)}})
	if err != nil {
		t.Fatal(err)
	}
	rawErr := errors.New("nats://user:private-password@127.0.0.1:4222\nprivate-error-text")
	c.nc.Opts.AsyncErrorCB(c.nc, nil, rawErr)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, record := range readLogs(t, buf) {
		if record["nats_event"] == "async_error" && record["level"] == "ERROR" && record["err"] == rawErr.Error() {
			seen = true
		}
	}
	if !seen {
		t.Fatalf("缺少异步错误日志: %s", buf.String())
	}
}

func TestTrustedTraceID(t *testing.T) {
	for _, test := range []struct {
		name string
		id   string
	}{
		{
			name: "manual",
			id:   "case-trace",
		},
		{
			name: "long",
			id:   strings.Repeat("x", 129),
		},
		{
			name: "control_characters",
			id:   "value\r\n\x00\x7f",
		},
		{
			name: "empty",
			id:   "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := logit.WithContext(context.Background())
			logit.AddMeta(ctx, logit.Str(logit.LogId, test.id))
			out := &nats.Msg{Subject: "trace.trusted"}
			setMessageLogID(ctx, out)
			in := MessageContext(context.Background(), nats.Header{TraceHeader: []string{test.id}})
			inID, _ := logit.LogIDFromContext(in)
			for _, id := range []string{out.Header.Get(TraceHeader), inID} {
				if test.id == "" {
					if len(id) != 36 {
						t.Errorf("空 ID 未生成新值: %q", id)
					}
				} else if id != test.id {
					t.Errorf("已有 ID 未原样保留: got=%q, want=%q", id, test.id)
				}
			}
		})
	}
}

func TestMessageContextTraceHeaderCaseSensitive(t *testing.T) {
	for _, name := range []string{"x-log-id", "X-LOG-ID"} {
		t.Run(name, func(t *testing.T) {
			ctx := MessageContext(context.Background(), nats.Header{name: []string{"other-id"}})
			id, _ := logit.LogIDFromContext(ctx)
			if id == "other-id" || len(id) != 36 {
				t.Fatalf("Header 名未精确匹配时应生成新 ID: %q", id)
			}
		})
	}
}
