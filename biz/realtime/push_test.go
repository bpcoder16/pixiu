package realtime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestPushTargets(t *testing.T) {
	s, _ := startTestServer(t, baseConfig())
	msg := PushMessage{Type: "client.state", Data: &emptypb.Empty{}}
	for _, f := range []func(context.Context, string, PushMessage) (PushResult, error){s.PushToUser, s.PushToDevice} {
		if _, err := f(t.Context(), " ", msg); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("空目标应拒绝: %v", err)
		}
		if res, err := f(t.Context(), "offline", msg); err != nil || res != (PushResult{}) {
			t.Fatalf("零命中: %+v %v", res, err)
		}
	}
}

func TestPushToOnlineTargets(t *testing.T) {
	cfg := baseConfig()
	cfg.Authenticate = func(_ context.Context, r *http.Request) (ClientInfo, error) {
		return ClientInfo{
			UserID:   r.Header.Get("X-User"),
			DeviceID: r.Header.Get("X-Device"),
		}, nil
	}
	s, h := startTestServer(t, cfg)
	connect := func(user, device string) *websocket.Conn {
		t.Helper()
		d := websocket.Dialer{Subprotocols: []string{SubprotocolJSON}}
		c, _, err := d.Dial("ws"+strings.TrimPrefix(h.URL, "http"), http.Header{
			"X-User":   {user},
			"X-Device": {device},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	phone := connect("alice", "phone")
	tablet := connect("alice", "tablet")
	connect("bob", "laptop")
	awaitTargets(t, s, 3)

	msg := PushMessage{Type: "home.refreshed", Data: &emptypb.Empty{}}
	got, err := s.PushToUser(t.Context(), "alice", msg)
	if err != nil || got != (PushResult{Matched: 2, Enqueued: 2}) {
		t.Fatalf("用户推送 = %+v, %v", got, err)
	}
	responseBody(t, phone, protocol.Kind_EVENT)
	responseBody(t, tablet, protocol.Kind_EVENT)
	got, err = s.PushToDevice(t.Context(), "phone", msg)
	if err != nil || got != (PushResult{Matched: 1, Enqueued: 1}) {
		t.Fatalf("设备推送 = %+v, %v", got, err)
	}
	responseBody(t, phone, protocol.Kind_EVENT)

	_ = phone.Close()
	awaitTargets(t, s, 2)
	got, err = s.PushToDevice(t.Context(), "phone", msg)
	if err != nil || got != (PushResult{}) {
		t.Fatalf("设备断开后仍被命中: %+v, %v", got, err)
	}
	got, err = s.PushToUser(t.Context(), "alice", msg)
	if err != nil || got != (PushResult{Matched: 1, Enqueued: 1}) {
		t.Fatalf("用户剩余连接推送 = %+v, %v", got, err)
	}
	responseBody(t, tablet, protocol.Kind_EVENT)
}

type blockedPushBody struct {
	*emptypb.Empty
	once             sync.Once
	entered, release chan struct{}
}

func (m *blockedPushBody) ProtoReflect() protoreflect.Message {
	m.once.Do(func() { close(m.entered); <-m.release })
	return m.Empty.ProtoReflect()
}

func TestPushStopWaitsForAcceptedCall(t *testing.T) {
	cfg := baseConfig()
	s, h := startTestServer(t, cfg)
	c := dial(t, h, SubprotocolProtobuf)
	awaitTargets(t, s, 1)
	msg := &blockedPushBody{Empty: &emptypb.Empty{}, entered: make(chan struct{}), release: make(chan struct{})}
	unblock := sync.OnceFunc(func() { close(msg.release) })
	defer unblock()
	done := make(chan error, 1)
	go func() {
		_, err := s.Broadcast(context.Background(), PushMessage{Type: "notice", Data: msg})
		done <- err
	}()
	await(t, msg.entered)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Broadcast(t.Context(), PushMessage{}); !errors.Is(err, websocketx.ErrClosed) {
		t.Fatalf("停收后接纳新推送: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未等待已接纳推送: %v", err)
	}
	unblock()
	if err := await(t, done); err != nil {
		t.Fatal(err)
	}
	_, data := read(t, c)
	var envelope protocol.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil || envelope.Type != "notice" {
		t.Fatalf("排空期间推送丢失: %v", err)
	}
	await(t, s.Done())
}

func awaitTargets(t *testing.T, s *Server, count int) {
	t.Helper()
	end := time.Now().Add(2 * time.Second)
	for len(s.targets.snapshot(pushTarget{kind: allTargets})) != count {
		if time.Now().After(end) {
			t.Fatal("连接索引不符合预期")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestPushPartialDelivery(t *testing.T) {
	cfg := baseConfig()
	cfg.Transport.SendQueueBytes = 95
	s, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	pb := dial(t, h, SubprotocolProtobuf)
	awaitTargets(t, s, 2)
	got, err := s.Broadcast(t.Context(), PushMessage{Type: "long.type.abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", Data: &emptypb.Empty{}})
	if got.Matched != 2 || got.Enqueued != 1 || got.Failed != 1 || !errors.Is(err, websocketx.ErrQueueFull) {
		t.Fatalf("部分投递: %+v %v", got, err)
	}
	_, _ = read(t, pb)
}

func TestPushEventErrorOneof(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		out := Outgoing{Kind: protocol.Kind_EVENT, Type: "client.state", Error: &Error{Code: "UNAVAILABLE", Message: "稍后再试"}}
		frame, err := encodeEnvelope(protocolName, out, 64<<10)
		if err != nil {
			t.Fatal(err)
		}
		if protocolName == SubprotocolJSON {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(frame.Data, &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["kind"]) != "3" || fields["error"] == nil || fields["data"] != nil || fields["requestId"] != nil {
				t.Fatalf("错误事件信封: %s", frame.Data)
			}
		} else {
			var env protocol.Envelope
			if err := proto.Unmarshal(frame.Data, &env); err != nil || env.Kind != protocol.Kind_EVENT || env.RequestId != "" || env.GetError().GetCode() != "UNAVAILABLE" {
				t.Fatalf("错误 PB 事件: %v, %v", &env, err)
			}
		}
		out.Data = &emptypb.Empty{}
		if _, err := encodeEnvelope(protocolName, out, 64<<10); !errors.Is(err, ErrInvalidMessage) {
			t.Fatal("错误事件不能同时含 data")
		}
	}
}
