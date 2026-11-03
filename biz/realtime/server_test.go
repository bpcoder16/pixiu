package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func baseConfig() Config {
	return Config{
		Authenticate: func(_ context.Context, r *http.Request) (ClientInfo, error) {
			return ClientInfo{
				UserID:   "user",
				DeviceID: r.Header.Get("X-Device"),
			}, nil
		},
		OnMessage: func(context.Context, *Session, *Message) error {
			return nil
		},
	}
}

func await[T any](t testing.TB, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("等待事件超时")
		var zero T
		return zero
	}
}

func startTestServer(t testing.TB, cfg Config) (*Server, *httptest.Server) {
	t.Helper()
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	run := make(chan error, 1)
	go func() { run <- s.Run() }()
	deadline := time.Now().Add(3 * time.Second)
	for !s.Ready() {
		if time.Now().After(deadline) {
			t.Fatal("服务未就绪")
		}
		time.Sleep(time.Millisecond)
	}
	httpServer := httptest.NewServer(s)
	t.Cleanup(func() {
		s.Cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := s.Wait(ctx); errors.Is(err, context.DeadlineExceeded) {
			t.Error("服务没有真实收尾")
		}
		await(t, run)
		httpServer.Close()
	})
	return s, httpServer
}

func dial(t testing.TB, h *httptest.Server, protocols ...string) *websocket.Conn {
	t.Helper()
	d := websocket.Dialer{Subprotocols: protocols}
	c, _, err := d.Dial("ws"+strings.TrimPrefix(h.URL, "http"), http.Header{
		"X-Device": []string{"device"},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func read(t testing.TB, c *websocket.Conn) (int, []byte) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	kind, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	return kind, data
}

func requestWire(t testing.TB, protocolName string, data proto.Message) (int, []byte) {
	t.Helper()
	if protocolName == SubprotocolJSON {
		body, err := protojson.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		return websocket.TextMessage, append(append([]byte(
			"{\"kind\":1,\"requestId\":\"r-1\",\"scene\":\"home\",\"action\":\"refresh\",\"data\":"),
			body...), '}')
	}
	body, err := proto.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(&protocol.Envelope{
		Kind:      protocol.Kind_REQUEST,
		RequestId: "r-1",
		Scene:     "home",
		Action:    "refresh",
		Body:      &protocol.Envelope_Data{Data: body},
	})
	if err != nil {
		t.Fatal(err)
	}
	return websocket.BinaryMessage, wire
}

func responseBody(t testing.TB, c *websocket.Conn, wantKind protocol.Kind) []byte {
	t.Helper()
	typ, wire := read(t, c)
	if c.Subprotocol() == SubprotocolJSON {
		if typ != websocket.TextMessage {
			t.Fatal("JSON 协议收到非 Text 帧")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(wire, &fields); err != nil {
			t.Fatal(err)
		}
		var kind protocol.Kind
		if err := json.Unmarshal(fields["kind"], &kind); err != nil || kind != wantKind {
			t.Fatalf("消息类型错误: %s", wire)
		}
		if wantKind == protocol.Kind_RESPONSE && string(fields["requestId"]) != "\"r-1\"" {
			t.Fatalf("响应未关联请求: %s", wire)
		}
		if wantKind == protocol.Kind_EVENT && fields["requestId"] != nil {
			t.Fatal("事件错误携带 requestId")
		}
		if string(fields["type"]) != "\"home.refreshed\"" || fields["scene"] != nil || fields["action"] != nil {
			t.Fatalf("下行必须携带业务类型且不携带路由字段: %s", wire)
		}
		return fields["data"]
	}
	if typ != websocket.BinaryMessage {
		t.Fatal("PB 协议收到非 Binary 帧")
	}
	var envelope protocol.Envelope
	if err := proto.Unmarshal(wire, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Kind != wantKind || envelope.Type != "home.refreshed" ||
		envelope.Scene != "" || envelope.Action != "" {
		t.Fatalf("响应信封错误: %v", &envelope)
	}
	if wantKind == protocol.Kind_RESPONSE && envelope.RequestId != "r-1" ||
		wantKind == protocol.Kind_EVENT && envelope.RequestId != "" {
		t.Fatal("关联请求 ID 错误")
	}
	if _, ok := envelope.Body.(*protocol.Envelope_Data); !ok {
		t.Fatal("正文缺失")
	}
	return envelope.GetData()
}

func TestProtocolsRoundTripAndUnsolicitedSend(t *testing.T) {
	payloads := []proto.Message{
		wrapperspb.Int64(9007199254740993),
		wrapperspb.Bytes([]byte{0, 255, 1, 10}),
		&descriptorpb.FieldDescriptorProto{Type: descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()},
		structpb.NewStringValue("oneof"),
		structpb.NewNullValue(),
		&emptypb.Empty{},
	}
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, payload := range payloads {
			t.Run(protocolName+"/"+string(payload.ProtoReflect().Descriptor().FullName()), func(t *testing.T) {
				opened := make(chan *Session, 1)
				cfg := baseConfig()
				cfg.OnOpen = func(_ context.Context, s *Session) error {
					opened <- s
					return nil
				}
				cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
					value := payload.ProtoReflect().New().Interface()
					if err := m.Decode(value); err != nil {
						return err
					}
					if !proto.Equal(value, payload) {
						return errors.New("解码值不一致")
					}
					return s.Reply(ctx, m, "home.refreshed", value)
				}
				_, h := startTestServer(t, cfg)
				c := dial(t, h, protocolName)
				session := await(t, opened)
				typ, wire := requestWire(t, protocolName, payload)
				if err := c.WriteMessage(typ, wire); err != nil {
					t.Fatal(err)
				}
				for _, kind := range []protocol.Kind{protocol.Kind_RESPONSE, protocol.Kind_EVENT} {
					if kind == protocol.Kind_EVENT {
						if err := session.Send(context.Background(), Outgoing{
							Kind: protocol.Kind_EVENT,
							Type: "home.refreshed",
							Data: payload,
						}); err != nil {
							t.Fatal(err)
						}
					}
					body := responseBody(t, c, kind)
					got := payload.ProtoReflect().New().Interface()
					var err error
					if protocolName == SubprotocolJSON {
						err = protojson.Unmarshal(body, got)
					} else {
						err = proto.Unmarshal(body, got)
					}
					if err != nil || !proto.Equal(got, payload) {
						t.Fatalf("下发正文错误: %v, %v", got, err)
					}
				}
			})
		}
	}
}

func TestNegotiationAuthenticationAndEarlyStop(t *testing.T) {
	cfg := baseConfig()
	var authCalls atomic.Int32
	cfg.Authenticate = func(context.Context, *http.Request) (ClientInfo, error) {
		authCalls.Add(1)
		return ClientInfo{}, errors.New("不得公开的凭据内容")
	}
	s, err := NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	h := httptest.NewServer(s)
	defer h.Close()
	defer s.Cancel()
	d := websocket.Dialer{Subprotocols: []string{SubprotocolJSON}}
	url := "ws" + strings.TrimPrefix(h.URL, "http")
	_, response, err := d.Dial(url, nil)
	if err == nil || response.StatusCode != http.StatusServiceUnavailable || authCalls.Load() != 0 {
		t.Fatal("Run 前接纳了握手")
	}
	run := make(chan error, 1)
	go func() { run <- s.Run() }()
	for !s.Ready() {
		time.Sleep(time.Millisecond)
	}
	for _, protocols := range [][]string{nil, {"unsupported"}, {SubprotocolJSON}} {
		d.Subprotocols = protocols
		_, response, err = d.Dial(url, nil)
		want := http.StatusBadRequest
		if len(protocols) > 0 && protocols[0] == SubprotocolJSON {
			want = http.StatusUnauthorized
		}
		if err == nil || response.StatusCode != want {
			t.Fatalf("协商/认证状态错误: %v, %v", response, err)
		}
	}
	if authCalls.Load() != 1 {
		t.Fatal("非法子协议进入认证")
	}
	_ = s.Stop()
	if err := await(t, run); err != nil || s.Ready() {
		t.Fatalf("停机失败: %v", err)
	}
	early, err := NewServer(baseConfig())
	if err != nil {
		t.Fatal(err)
	}
	_ = early.Stop()
	if err := early.Run(); err != nil || early.Ready() {
		t.Fatalf("停收后的 Run 重新启动: %v", err)
	}
	if err := early.Run(); !errors.Is(err, ErrStarted) {
		t.Fatalf("重复 Run = %v", err)
	}
}

func TestSessionIsolationAndHTTPIndependence(t *testing.T) {
	type state struct {
		count atomic.Int32
	}
	opened := make(chan *Session, 2)
	cfg := baseConfig()
	cfg.NewState = func(ClientInfo) any { return &state{} }
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
		count := s.State().(*state).count.Add(1)
		return s.Reply(ctx, m, "home.refreshed", wrapperspb.Int32(count))
	}
	_, h := startTestServer(t, cfg)
	first := dial(t, h, SubprotocolProtobuf, SubprotocolJSON)
	a := await(t, opened)
	second := dial(t, h, SubprotocolJSON)
	b := await(t, opened)
	h.Close()
	if a.ID() == b.ID() || a.State() == b.State() || a.Info().UserID != "user" ||
		a.Info().DeviceID != "device" || a.Context().Err() != nil ||
		first.Subprotocol() != SubprotocolJSON {
		t.Fatal("连接隔离或协议选择错误")
	}
	for _, c := range []*websocket.Conn{first, second, first} {
		typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
		if err := c.WriteMessage(typ, wire); err != nil {
			t.Fatal(err)
		}
		responseBody(t, c, protocol.Kind_RESPONSE)
	}
	if a.State().(*state).count.Load() != 2 || b.State().(*state).count.Load() != 1 {
		t.Fatal("状态跨连接串写")
	}
}

func TestJSONEnvelopeCompatibility(t *testing.T) {
	valid := []byte(`{"kind":1,"requestId":"r-1","scene":"home","action":"refresh","data":{}}`)
	for _, tc := range []struct {
		name  string
		wire  []byte
		scene string
	}{
		{
			name:  "duplicate",
			wire:  bytes.Replace(valid, []byte(`"scene":"home"`), []byte(`"scene":"home","scene":"other"`), 1),
			scene: "other",
		},
		{
			name:  "case-insensitive",
			wire:  bytes.Replace(valid, []byte(`"scene"`), []byte(`"Scene"`), 1),
			scene: "home",
		},
		{
			name:  "unknown",
			wire:  bytes.Replace(valid, []byte(`"scene"`), []byte(`"extra":{"nested":[1]},"scene"`), 1),
			scene: "home",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type result struct {
				requestID string
				scene     string
				action    string
				decodeErr error
			}
			received := make(chan result, 1)
			cfg := baseConfig()
			cfg.OnMessage = func(_ context.Context, _ *Session, m *Message) error {
				received <- result{
					requestID: m.RequestID(),
					scene:     m.Scene(),
					action:    m.Action(),
					decodeErr: m.Decode(&emptypb.Empty{}),
				}
				return nil
			}
			_, h := startTestServer(t, cfg)
			c := dial(t, h, SubprotocolJSON)
			if err := c.WriteMessage(websocket.TextMessage, tc.wire); err != nil {
				t.Fatal(err)
			}
			got := await(t, received)
			if got.requestID != "r-1" || got.scene != tc.scene || got.action != "refresh" || got.decodeErr != nil {
				t.Fatalf("JSON 信封未沿用标准库解码语义: %+v", got)
			}
		})
	}
}

func TestInvalidFramesAndEnvelopes(t *testing.T) {
	valid := []byte("{\"kind\":1,\"requestId\":\"r-1\",\"scene\":\"home\",\"action\":\"refresh\",\"data\":{}}")
	for _, tc := range []struct {
		name string
		typ  int
		wire []byte
		code int
	}{
		{
			name: "wrong-frame",
			typ:  websocket.BinaryMessage,
			wire: valid,
			code: 1003,
		},
		{
			name: "malformed",
			typ:  websocket.TextMessage,
			wire: []byte("{"),
			code: 1007,
		},
		{
			name: "missing-data",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`,"data":{}`), nil, 1),
			code: 1007,
		},
		{
			name: "request-error",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"data":{}`), []byte(`"data":{},"error":{"code":"FAIL"}`), 1),
			code: 1007,
		},
		{
			name: "request-error-null",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"data":{}`), []byte(`"data":{},"Error":null`), 1),
			code: 1007,
		},
		{
			name: "null-request-id",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"requestId":"r-1"`), []byte(`"requestId":null`), 1),
			code: 1007,
		},
		{
			name: "wrong-field-type",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"scene":"home"`), []byte(`"scene":1`), 1),
			code: 1007,
		},
		{
			name: "invalid-utf8",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"data":{}`), []byte("\"data\":\"\xff\""), 1),
			code: 1007,
		},
		{
			name: "missing",
			typ:  websocket.TextMessage,
			wire: []byte("{}"),
			code: 1007,
		},
		{
			name: "direction",
			typ:  websocket.TextMessage,
			wire: bytes.Replace(valid, []byte(`"kind":1`), []byte(`"kind":3`), 1),
			code: 1007,
		},
		{
			name: "trailing",
			typ:  websocket.TextMessage,
			wire: append(bytes.Clone(valid), []byte("{}")...),
			code: 1007,
		},
		{
			name: "oversize",
			typ:  websocket.TextMessage,
			wire: bytes.Repeat([]byte(" "), 65537),
			code: 1009,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var called atomic.Bool
			cfg := baseConfig()
			cfg.OnMessage = func(context.Context, *Session, *Message) error {
				called.Store(true)
				return nil
			}
			_, h := startTestServer(t, cfg)
			c := dial(t, h, SubprotocolJSON)
			if err := c.WriteMessage(tc.typ, tc.wire); err != nil {
				t.Fatal(err)
			}
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _, err := c.ReadMessage()
			if !websocket.IsCloseError(err, tc.code) || called.Load() {
				t.Fatalf("非法消息处理错误: %v, called=%v", err, called.Load())
			}
		})
	}
}

func TestDrainRetainsAcceptedReplyAndWaitDoesNotCancel(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	cfg := baseConfig()
	cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
		entered <- ctx
		<-release
		return s.Reply(ctx, m, "home.refreshed", wrapperspb.String("done"))
	}
	s, h := startTestServer(t, cfg)
	c := dial(t, h, SubprotocolJSON)
	typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	_ = c.WriteMessage(typ, wire)
	handlerContext := await(t, entered)
	_ = s.Stop()
	waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := s.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) || handlerContext.Err() != nil {
		t.Fatalf("Wait 错误取消了业务: %v", err)
	}
	unblock()
	if body := responseBody(t, c, protocol.Kind_RESPONSE); string(body) != "\"done\"" {
		t.Fatalf("排空期间响应丢失: %s", body)
	}
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := s.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOutboundValidationAndTransportErrors(t *testing.T) {
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.Transport.MaxSendBytes = 128
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	_, h := startTestServer(t, cfg)
	c := dial(t, h, SubprotocolJSON)
	s := await(t, opened)
	var nilBody *emptypb.Empty
	for _, out := range []Outgoing{
		{
			Type: "home.refreshed",
			Data: &emptypb.Empty{},
		},
		{
			Kind:      protocol.Kind_REQUEST,
			RequestID: "r-1",
			Type:      "home.refreshed",
			Data:      &emptypb.Empty{},
		},
		{
			Kind: 99,
			Type: "home.refreshed",
			Data: &emptypb.Empty{},
		},
		{
			Kind: protocol.Kind_EVENT,
			Type: "home.refreshed",
			Data: nilBody,
		},
		{
			Kind:      protocol.Kind_EVENT,
			Type:      "home.refreshed",
			RequestID: "r-1",
			Data:      &emptypb.Empty{},
		},
		{
			Kind: protocol.Kind_RESPONSE,
			Type: "home.refreshed",
			Data: &emptypb.Empty{},
		},
		{
			Kind: protocol.Kind_EVENT,
			Type: "home.refreshed",
			Data: &emptypb.Empty{},
			Error: &Error{
				Code:    "FAIL",
				Message: "失败",
			},
		},
	} {
		if err := s.Send(context.Background(), out); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("非法输出被接受: %v", err)
		}
	}
	out := Outgoing{
		Kind: protocol.Kind_EVENT,
		Type: "home.refreshed",
		Data: wrapperspb.String(strings.Repeat("x", 128)),
	}
	if err := s.Send(context.Background(), out); !errors.Is(err, websocketx.ErrMessageTooLarge) {
		t.Fatalf("出站长度错误: %v", err)
	}
	out.Data = &emptypb.Empty{}
	out.ExpiresAt = time.Now().Add(-time.Second)
	if err := s.Send(context.Background(), out); !errors.Is(err, websocketx.ErrExpired) {
		t.Fatalf("未保留过期错误: %v", err)
	}
	_, _ = s.Close(1000, "done")
	out.ExpiresAt = time.Time{}
	if err := s.Send(context.Background(), out); !errors.Is(err, websocketx.ErrClosed) {
		t.Fatalf("关闭连接仍接受发送: %v", err)
	}
	_ = c.Close()
}
