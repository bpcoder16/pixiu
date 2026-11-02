package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestPublicErrorAndHandlerFailure(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		t.Run(protocolName, func(t *testing.T) {
			called := make(chan struct{}, 2)
			observed := make(chan error, 1)
			var count atomic.Int32
			cfg := baseConfig()
			cfg.Transport.Observe = func(_ context.Context, event websocketx.Event) {
				if event.Kind == websocketx.EventHandled && event.Err != nil {
					observed <- event.Err
				}
			}
			cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
				called <- struct{}{}
				if count.Add(1) == 1 {
					return errors.New("内部敏感信息")
				}
				return s.ReplyError(ctx, m, "FORBIDDEN", "没有权限")
			}
			_, h := startTestServer(t, cfg)
			c := dial(t, h, protocolName)
			typ, wire := requestWire(t, protocolName, &emptypb.Empty{})
			if err := c.WriteMessage(typ, wire); err != nil {
				t.Fatal(err)
			}
			await(t, called)
			if err := await(t, observed); !errors.Is(err, websocketx.ErrCallback) {
				t.Fatalf("内部错误未脱敏: %v", err)
			}
			if err := c.WriteMessage(typ, wire); err != nil {
				t.Fatal(err)
			}
			_, reply := read(t, c)
			if strings.Contains(string(reply), "内部敏感信息") {
				t.Fatal("内部错误被下发")
			}
			if protocolName == SubprotocolJSON {
				var envelope map[string]json.RawMessage
				if err := json.Unmarshal(reply, &envelope); err != nil {
					t.Fatal(err)
				}
				var failure map[string]string
				if err := json.Unmarshal(envelope["error"], &failure); err != nil ||
					failure["code"] != "FORBIDDEN" || failure["message"] != "没有权限" ||
					envelope["data"] != nil || string(envelope["requestId"]) != "\"r-1\"" {
					t.Fatalf("错误响应不一致: %s, %v", reply, err)
				}
			} else {
				var envelope protocol.Envelope
				if err := proto.Unmarshal(reply, &envelope); err != nil {
					t.Fatal(err)
				}
				if envelope.GetError().GetCode() != "FORBIDDEN" ||
					envelope.GetError().GetMessage() != "没有权限" || envelope.RequestId != "r-1" {
					t.Fatalf("PB 错误响应不一致: %v", &envelope)
				}
			}
		})
	}
}

// 将编码固定在请求超时之后，覆盖 Reply 入口检查与实际入队之间的窗口。
type replyAfterDeadline struct {
	*emptypb.Empty
	ctx context.Context
}

func (m replyAfterDeadline) ProtoReflect() protoreflect.Message {
	<-m.ctx.Done()
	return m.Empty.ProtoReflect()
}

func TestReplyDeadlineDuringEncoding(t *testing.T) {
	result := make(chan error, 1)
	cfg := baseConfig()
	cfg.Transport.MessageTimeout = 100 * time.Millisecond
	cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
		result <- s.Reply(context.Background(), m, replyAfterDeadline{
			Empty: &emptypb.Empty{},
			ctx:   ctx,
		})
		return nil
	}
	_, h := startTestServer(t, cfg)
	c := dial(t, h, SubprotocolJSON)
	typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	if err := c.WriteMessage(typ, wire); err != nil {
		t.Fatal(err)
	}
	if err := await(t, result); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("编码期间请求超时，应拒绝入队并保留原请求错误: %v", err)
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatalf("请求超时后应直接收到关闭帧: %v", err)
	}
}

func TestTimeoutRejectsLateReplyAndDoneWaitsForCallback(t *testing.T) {
	entered := make(chan *Session, 1)
	replyErr := make(chan error, 1)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	cfg := baseConfig()
	cfg.Transport.MessageTimeout = 40 * time.Millisecond
	cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
		entered <- s
		<-release
		replyErr <- s.Reply(context.Background(), m, &emptypb.Empty{})
		return nil
	}
	server, h := startTestServer(t, cfg)
	defer unblock()
	c := dial(t, h, SubprotocolJSON)
	typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	_ = c.WriteMessage(typ, wire)
	s := await(t, entered)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, 1011) {
		t.Fatalf("消息超时未关闭连接: %v", err)
	}
	server.Cancel()
	select {
	case <-s.Done():
		t.Fatal("回调未返回就宣告 Session 完成")
	case <-server.Done():
		t.Fatal("回调未返回就宣告 Server 完成")
	default:
	}
	unblock()
	if err := await(t, replyErr); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("迟到回复未保留请求期限: %v", err)
	}
}

func TestInitializationFailureAndRevalidationCleanup(t *testing.T) {
	for _, mode := range []string{"open-error", "open-panic", "revalidate"} {
		t.Run(mode, func(t *testing.T) {
			closed := make(chan *Session, 1)
			cfg := baseConfig()
			cfg.NewState = func(ClientInfo) any { return new(atomic.Int32) }
			cfg.OnClose = func(_ context.Context, s *Session) { closed <- s }
			cfg.OnOpen = func(context.Context, *Session) error {
				if mode == "open-error" {
					return errors.New("初始化失败")
				}
				if mode == "open-panic" {
					panic("初始化 panic")
				}
				return nil
			}
			if mode == "revalidate" {
				cfg.Transport.RevalidateInterval = time.Millisecond
				cfg.Revalidate = func(_ context.Context, info ClientInfo) error {
					if info.UserID != "user" || info.DeviceID != "device" {
						t.Error("复核身份发生改变")
					}
					return errors.New("资格失效")
				}
			}
			server, h := startTestServer(t, cfg)
			c := dial(t, h, SubprotocolJSON)
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			_, _, err := c.ReadMessage()
			code := 1011
			if mode == "revalidate" {
				code = 1008
			}
			if !websocket.IsCloseError(err, code) {
				t.Fatalf("回调失败关闭码错误: %v", err)
			}
			s := await(t, closed)
			if s.State() == nil {
				t.Fatal("收尾丢失业务状态")
			}
			await(t, s.Done())
			if _, exists := server.sessions.Load(s.raw); exists {
				t.Fatal("真实收尾后保留了业务会话")
			}
		})
	}
}

func TestReplyRejectsForeignSessionAndCompletedRequest(t *testing.T) {
	opened := make(chan *Session, 2)
	requests := make(chan *Message, 1)
	handled := make(chan struct{}, 1)
	cfg := baseConfig()
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	cfg.OnMessage = func(_ context.Context, _ *Session, m *Message) error {
		requests <- m
		return nil
	}
	cfg.Transport.Observe = func(_ context.Context, event websocketx.Event) {
		if event.Kind == websocketx.EventHandled {
			handled <- struct{}{}
		}
	}
	_, h := startTestServer(t, cfg)
	a := dial(t, h, SubprotocolJSON)
	first := await(t, opened)
	dial(t, h, SubprotocolJSON)
	second := await(t, opened)
	typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	_ = a.WriteMessage(typ, wire)
	request := await(t, requests)
	await(t, handled)
	if err := second.Reply(context.Background(), request, &emptypb.Empty{}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatalf("跨 Session 回复被接受: %v", err)
	}
	if err := first.Reply(context.Background(), request, &emptypb.Empty{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("已结束请求仍可回复: %v", err)
	}
}

func TestSharedBudgetRejectionDoesNotSilentlyDrop(t *testing.T) {
	budget, err := websocketx.NewBudget(websocketx.BudgetConfig{SendBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.Transport.Budget = budget
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	_, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	s := await(t, opened)
	if err := s.Send(context.Background(), Outgoing{
		Kind:   Event,
		Scene:  "home",
		Action: "refresh",
		Data:   &emptypb.Empty{},
	}); !errors.Is(err, websocketx.ErrBudgetExceeded) {
		t.Fatalf("共享预算错误丢失: %v", err)
	}
}

func TestReservedTransportConfiguration(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Authenticate = nil },
		func(c *Config) { c.OnMessage = nil },
		func(c *Config) { c.Transport.Subprotocols = []string{"other"} },
		func(c *Config) { c.Transport.CheckReady = func() bool { return true } },
		func(c *Config) {
			c.Transport.Authenticate = func(context.Context, *http.Request) (websocketx.Identity, error) {
				return websocketx.Identity{}, nil
			}
		},
	} {
		cfg := baseConfig()
		mutate(&cfg)
		if _, err := NewServer(cfg); err == nil {
			t.Fatal("保留配置或缺失必要回调被静默接受")
		}
	}
}

func TestPBRequestEnvelopeFields(t *testing.T) {
	group := bytes.Repeat(protowire.AppendTag(nil, 10, protowire.StartGroupType), 33)
	group = append(group, bytes.Repeat(protowire.AppendTag(nil, 10, protowire.EndGroupType), 33)...)
	body := &emptypb.Empty{}
	body.ProtoReflect().SetUnknown(group)
	typ, wire := requestWire(t, SubprotocolProtobuf, body)
	cfg := baseConfig()
	cfg.OnMessage = func(ctx context.Context, s *Session, m *Message) error {
		var value emptypb.Empty
		if err := m.Decode(&value); err != nil {
			return s.ReplyError(ctx, m, "INVALID", "非法正文")
		}
		return s.Reply(ctx, m, &value)
	}
	_, h := startTestServer(t, cfg)
	for _, tc := range []struct {
		name   string
		prefix []byte
		reject bool
	}{
		{"unknown-group", group, true},
		// error 出现在 data 之前，不能被 protobuf 的 oneof 覆盖规则掩盖。
		{"error-before-data", protowire.AppendBytes(protowire.AppendTag(nil, 6, protowire.BytesType), nil), true},
		{"unknown-field", protowire.AppendVarint(protowire.AppendTag(nil, 10, protowire.VarintType), 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, h, SubprotocolProtobuf)
			if err := c.WriteMessage(typ, append(bytes.Clone(tc.prefix), wire...)); err != nil {
				t.Fatal(err)
			}
			if tc.reject {
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseInvalidFramePayloadData) {
					t.Fatalf("非法 PB 信封应关闭 1007: %v", err)
				}
				return
			}
			if got := responseBody(t, c, "response"); !bytes.Equal(got, group) {
				t.Fatalf("普通未知信封字段或业务正文未按原生规则处理: %x", got)
			}
		})
	}
}

func TestMalformedProtobufAndDeepJSON(t *testing.T) {
	invalidPB := []*protocol.Envelope{
		{
			Kind:      protocol.Kind_REQUEST,
			RequestId: "r1",
			Scene:     "home",
			Action:    "refresh",
		},
		{
			Kind:      protocol.Kind_RESPONSE,
			RequestId: "r1",
			Scene:     "home",
			Action:    "refresh",
			Body:      &protocol.Envelope_Data{},
		},
		{
			Kind:      protocol.Kind_REQUEST,
			RequestId: "r1",
			Scene:     "home",
			Action:    "refresh",
			Body:      &protocol.Envelope_Error{Error: &protocol.Error{Code: "FAIL"}},
		},
	}
	for _, envelope := range invalidPB {
		wire, err := proto.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeEnvelope(SubprotocolProtobuf, wire); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("非法 PB 信封被接受: %v", envelope)
		}
	}
	if _, err := decodeEnvelope(SubprotocolProtobuf, []byte{0xff}); !errors.Is(err, ErrInvalidMessage) {
		t.Fatal("损坏的 PB 被接受")
	}
	prefix := "{\"kind\":\"request\",\"requestId\":\"r1\",\"scene\":\"home\",\"action\":\"refresh\",\"data\":"
	deep := prefix + strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33) + "}"
	if _, err := decodeEnvelope(SubprotocolJSON, []byte(deep)); !errors.Is(err, ErrInvalidMessage) {
		t.Fatal("超深 JSON 被接受")
	}
	if _, err := decodeEnvelope(SubprotocolJSON, []byte(prefix+"\"[\\\"{}]\"}")); err != nil {
		t.Fatalf("字符串内括号被错误统计: %v", err)
	}
}
