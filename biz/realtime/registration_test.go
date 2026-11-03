package realtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestServerRegistersHandlersAtConstruction(t *testing.T) {
	cfg := baseConfig()
	failure := errors.New("register failed")
	cfg.RegisterHandlers = func(r *Router) error { return failure }
	if server, err := NewServer(cfg); server != nil || !errors.Is(err, failure) {
		t.Fatalf("注册失败应保留错误并中止构造: %v", err)
	}
	var retained *Router
	calls := 0
	cfg.RegisterHandlers = func(r *Router) error {
		calls++
		retained = r
		return nil
	}
	missed := make(chan string, 1)
	cfg.OnNotFound = func(_ context.Context, _ *Session, m *Message) error {
		missed <- m.Action()
		return nil
	}
	_, h := startTestServer(t, cfg)
	if calls != 1 || retained.Handle("home", "refresh", cfg.OnNotFound) == nil {
		t.Fatal("服务应在构造时一次完成注册并封闭路由表")
	}
	c := dial(t, h, SubprotocolJSON)
	typ, frame := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	if err := c.WriteMessage(typ, frame); err != nil {
		t.Fatal(err)
	}
	if got := await(t, missed); got != "refresh" {
		t.Fatalf("自定义未命中处理器收到 %q", got)
	}
}

func TestReplyFailurePreservesCause(t *testing.T) {
	for _, tc := range []struct {
		name      string
		transport websocketx.Config
		want      error
	}{
		{"queue", websocketx.Config{SendQueueBytes: 1}, websocketx.ErrQueueFull},
		{"budget", websocketx.Config{}, websocketx.ErrBudgetExceeded},
		{"size", websocketx.Config{MaxSendBytes: 1}, websocketx.ErrMessageTooLarge},
		{"encode", websocketx.Config{}, ErrInvalidMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := baseConfig()
			cfg.Transport = tc.transport
			if tc.name == "budget" {
				var err error
				cfg.Transport.Budget, err = websocketx.NewBudget(websocketx.BudgetConfig{SendBytes: 1})
				if err != nil {
					t.Fatal(err)
				}
			}
			handled := make(chan error, 1)
			cfg.RegisterHandlers = func(r *Router) error {
				return r.Handle("home", "refresh", func(ctx context.Context, s *Session, m *Message) error {
					var err error
					switch tc.name {
					case "encode":
						err = s.Reply(ctx, m, "", wrapperspb.String("ok"))
					case "queue":
						err = s.ReplyError(ctx, m, "request.error", "INVALID_ARGUMENT", "非法参数")
					default:
						err = s.Reply(ctx, m, "home.refreshed", wrapperspb.String("ok"))
					}
					handled <- err
					return err
				})
			}
			_, h := startTestServer(t, cfg)
			c := dial(t, h, SubprotocolJSON)
			typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
			if err := c.WriteMessage(typ, wire); err != nil {
				t.Fatal(err)
			}
			if err := await(t, handled); !errors.Is(err, tc.want) {
				t.Fatalf("回复失败原因丢失: got %v want %v", err, tc.want)
			}
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			_, _, err := c.ReadMessage()
			code := websocket.CloseInternalServerErr
			if tc.name == "queue" || tc.name == "budget" {
				code = websocket.CloseTryAgainLater
			}
			if !websocket.IsCloseError(err, code) {
				t.Fatalf("应关闭 %d: %v", code, err)
			}
		})
	}
}

func TestReplyWithCanceledCallerKeepsConnection(t *testing.T) {
	cfg := baseConfig()
	rejected := make(chan error, 1)
	cfg.RegisterHandlers = testRoute("home", "refresh", func(ctx context.Context, s *Session, m *Message) error {
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		rejected <- s.Reply(canceled, m, "home.refreshed", &emptypb.Empty{})
		return s.Reply(ctx, m, "home.refreshed", &emptypb.Empty{})
	})
	_, h := startTestServer(t, cfg)
	c := dial(t, h, SubprotocolJSON)
	typ, wire := requestWire(t, SubprotocolJSON, &emptypb.Empty{})
	if err := c.WriteMessage(typ, wire); err != nil {
		t.Fatal(err)
	}
	if err := await(t, rejected); !errors.Is(err, context.Canceled) {
		t.Fatalf("调用方取消原因丢失: %v", err)
	}
	responseBody(t, c, protocol.Kind_RESPONSE)
}
