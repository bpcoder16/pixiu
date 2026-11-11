package natsoutbox

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"gorm.io/gorm"
)

// 命名注册表关闭后不能重新初始化，各用例通过独立进程隔离。
func isolatedNamedClients(t *testing.T) bool {
	t.Helper()
	const marker = "PIXIU_NATSOUTBOX_NAMED_TEST"
	if os.Getenv(marker) == t.Name() {
		return true
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("命名客户端独立进程测试失败: %v\n%s", err, output)
	}
	return false
}

func TestJetStreamHandlerRequiresNamedClients(t *testing.T) {
	if !isolatedNamedClients(t) {
		return
	}
	for _, tc := range []struct {
		name  string
		names []string
	}{
		{name: "empty"},
		{
			name:  "empty_name",
			names: []string{""},
		},
		{
			name:  "blank_name",
			names: []string{" \t"},
		},
		{
			name:  "surrounding_space",
			names: []string{" events"},
		},
		{
			name:  "invalid_utf8",
			names: []string{"\xff"},
		},
		{
			name:  "missing",
			names: []string{"missing"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				value := recover()
				if value == nil {
					t.Fatal("配置错误应在构造 Handler 时 panic")
				}
				if tc.name == "missing" && value != `natsx: client "missing" is not registered` {
					t.Fatalf("应沿用 Named 的原始 panic: %v", value)
				}
			}()
			NewJetStreamHandler(tc.names...)
		})
	}
}

func TestJetStreamAcknowledgementAndStableID(t *testing.T) {
	if !isolatedNamedClients(t) {
		return
	}
	s, err := server.NewServer(&server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(func() {
		s.Shutdown()
		s.WaitForShutdown()
	})
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS 未启动")
	}
	client, err := natsx.NewNamed(natsx.Config{
		Name:      "events",
		URLs:      []string{s.ClientURL()},
		Namespace: "test1",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = natsx.CloseAll() })
	auditClient, err := natsx.NewNamed(natsx.Config{
		Name:      "audit",
		URLs:      []string{s.ClientURL()},
		Namespace: "test2",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := client.JetStream().CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST1_EVENTS",
		Subjects: []string{"test1.events.created"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	auditStream, err := auditClient.JetStream().CreateStream(ctx, jetstream.StreamConfig{
		Name:     "TEST2_EVENTS",
		Subjects: []string{"test2.events.created"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		t.Fatal(err)
	}
	names := []string{"events", "audit"}
	h := NewJetStreamHandler(names...)
	names[0] = "missing"
	data, _ := json.Marshal(map[string]any{
		"client":  "events",
		"subject": "events.created",
		"data":    []byte{0, 1, 255},
		"header": nats.Header{
			"X-Source":    []string{"a", "b"},
			"X-Lines":     []string{"a\r\nb"},
			"Nats-Msg-Id": []string{"old"},
		},
		"extra": true, // 未知字段不影响消息发布。
	})
	d := outbox.Delivery{
		ID: 42,
		Task: outbox.Task{
			Kind:    JetStreamKind,
			Version: JetStreamVersion,
			Payload: data,
		},
	}
	for range 2 {
		if err := h(ctx, d); err != nil {
			t.Fatalf("未取得确认: %v", err)
		}
	}
	info, err := stream.Info(ctx)
	if err != nil || info.State.Msgs != 1 {
		t.Fatalf("稳定 ID 未去重: %+v %v", info, err)
	}

	msg, err := stream.GetMsg(ctx, 1)
	if err != nil || msg.Subject != "test1.events.created" || string(msg.Data) != string([]byte{0, 1, 255}) || msg.Header.Get("Nats-Msg-Id") != "42" || len(msg.Header.Values("X-Source")) != 2 {
		t.Fatalf("消息快照变化: %+v %v", msg, err)
	}
	if got := msg.Header.Get("X-Lines"); got != "a  b" {
		t.Fatalf("Header 换行应由 SDK 规范化: %q", got)
	}
	d.Payload = []byte(`{"client":"events","subject":"events.created","header":{"X:Bad":["value"]}}`)
	if err := h(ctx, d); !errors.Is(err, nats.ErrBadHeaderMsg) {
		t.Fatalf("非法 Header 名应返回 SDK 错误: %v", err)
	}
	d.Payload = []byte(`{"client":"audit","subject":"events.created","data":"YXVkaXQ="}`)
	if err := h(ctx, d); err != nil {
		t.Fatalf("未按任务中的客户端名称路由: %v", err)
	}
	auditMsg, err := auditStream.GetMsg(ctx, 1)
	if err != nil || auditMsg.Subject != "test2.events.created" || string(auditMsg.Data) != "audit" || auditMsg.Header.Get("Nats-Msg-Id") != "42" {
		t.Fatalf("消息未发布到指定命名客户端: %+v %v", auditMsg, err)
	}
	if err := NewJetStreamHandler("events")(ctx, d); err == nil {
		t.Fatal("已注册但未列入 Handler 的客户端不得发布")
	}
	for _, suffix := range []string{" {}", " invalid"} {
		d.Payload = []byte(string(data) + suffix)
		if err := h(ctx, d); err == nil {
			t.Fatalf("尾部额外数据未拒绝: %q", suffix)
		}
	}
	d.Payload = []byte(`{"client":"missing","subject":"events.created","data":""}`)
	if err := h(ctx, d); err == nil {
		t.Fatal("缺失客户端不得回退默认实例")
	}
	if err := natsx.CloseAll(); err != nil {
		t.Fatal(err)
	}
	d.Payload = data
	if err := h(ctx, d); !errors.Is(err, natsx.ErrClosed) {
		t.Fatalf("Handler 应持有构造时取得的客户端，不再查询注册表: %v", err)
	}
	defer func() {
		if value := recover(); value != "natsx: named clients closed" {
			t.Fatalf("注册表关闭后构造 Handler 应沿用 Named 的 panic: %v", value)
		}
	}()
	NewJetStreamHandler("events")
}

func TestEnqueueRejectsInvalidMessagesBeforeDatabase(t *testing.T) {
	store, err := outbox.NewStore(outbox.StoreConfig{
		Backend:  enqueueTestBackend(t),
		MasterDB: func(context.Context) *gorm.DB { panic("不得访问主库") },
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []JetStreamMessage{
		{Subject: "events.created"},
		{Client: "events", Subject: "events.*"},
		{Client: "events", Subject: "events..created"},
	} {
		// 普通空会话可区分消息预检与 Store 的事务校验，不能靠 nil Store 短路通过。
		if _, err := EnqueueJetStreamTxWithStore(store, &gorm.DB{}, msg); !errors.Is(err, outbox.ErrInvalidArgument) || err.Error() == "outbox: invalid argument: unavailable or DryRun database" {
			t.Fatalf("非法消息未拒绝: %+v", msg)
		}
	}
}

func TestEnqueueUsesDefaultStore(t *testing.T) {
	if !isolatedNamedClients(t) {
		return
	}
	msg := JetStreamMessage{
		Client:  "events",
		Subject: "events.created",
	}
	func() {
		defer func() {
			if got := recover(); got != "outbox: default store is not registered" {
				t.Fatalf("未注册默认 Store 应明确 panic: %v", got)
			}
		}()
		_, _ = EnqueueJetStreamTx(nil, msg)
	}()
	if _, err := EnqueueJetStreamTxWithStore(nil, nil, msg); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("显式入口应拒绝 nil Store: %v", err)
	}
	_, err := outbox.NewDefaultStore(outbox.StoreConfig{
		Backend:  enqueueTestBackend(t),
		MasterDB: func(context.Context) *gorm.DB { panic("事务登记不得访问主库入口") },
	})
	if err != nil {
		t.Fatal(err)
	}
	// 仅注册 Store 即可登记；已有事务错误应原样传播，不依赖 Worker 或 NATS 注册。
	want := errors.New("业务事务不可用")
	tx := &gorm.DB{
		Config: &gorm.Config{},
		Error:  want,
	}
	if id, err := EnqueueJetStreamTx(tx, msg); id != 0 || !errors.Is(err, want) {
		t.Fatalf("未使用默认 Store 的事务校验: %d %v", id, err)
	}
}

func enqueueTestBackend(t *testing.T) outbox.Backend {
	t.Helper()
	for _, backend := range []outbox.Backend{outbox.MySQL, outbox.PostgreSQL} {
		_, err := outbox.NewStore(outbox.StoreConfig{
			Backend:  backend,
			MasterDB: func(context.Context) *gorm.DB { panic("探测构建能力不应查询数据库") },
		})
		if err == nil {
			return backend
		}
	}
	t.Skip("构建标签已禁用全部数据库后端")
	return ""
}
