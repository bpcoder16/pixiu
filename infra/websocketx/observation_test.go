package websocketx

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestReadFailuresKeepCloseClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		err  error
	}{
		{
			name: "stop",
			code: websocket.CloseGoingAway,
		},
		{
			name: "cancel",
			code: websocket.CloseGoingAway,
		},
		{
			name: "peer-close",
			code: websocket.CloseNormalClosure,
		},
		{
			name: "tcp-close",
			code: websocket.CloseAbnormalClosure,
			err:  net.ErrClosed,
		},
		{
			name: "partial-tcp-close",
			code: websocket.CloseAbnormalClosure,
			err:  net.ErrClosed,
		},
		{
			name: "pong-timeout",
			code: websocket.CloseAbnormalClosure,
			err:  context.DeadlineExceeded,
		},
		{
			name: "protocol",
			code: websocket.CloseProtocolError,
			err:  ErrInvalidMessage,
		},
		{
			name: "partial-protocol",
			code: websocket.CloseProtocolError,
			err:  ErrInvalidMessage,
		},
		{
			name: "stopped-partial-protocol",
			code: websocket.CloseProtocolError,
			err:  ErrInvalidMessage,
		},
		{
			name: "stopped-limit-protocol",
			code: websocket.CloseProtocolError,
			err:  ErrInvalidMessage,
		},
		{
			name: "invalid-text",
			code: websocket.CloseInvalidFramePayloadData,
			err:  ErrInvalidMessage,
		},
		{
			name: "read-limit",
			code: websocket.CloseMessageTooBig,
			err:  ErrMessageTooLarge,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opened := make(chan *Session, 1)
			ready := make(chan struct{}, 1)
			closed := make(chan Event, 1)
			cfg := Config{
				ReadLimit: 32,
				OnOpen: func(_ context.Context, s *Session) error {
					opened <- s
					return nil
				},
				Observe: func(_ context.Context, event Event) {
					switch event.Kind {
					case EventOpened:
						ready <- struct{}{}
					case EventClosed:
						closed <- event
					}
				},
			}
			if tc.name == "pong-timeout" {
				cfg.PingInterval = 5 * time.Millisecond
				cfg.PongTimeout = 30 * time.Millisecond
			}
			e, server := newTestEndpoint(t, cfg)
			c := dialTest(t, server)
			s := await(t, opened)
			await(t, ready)
			switch tc.name {
			case "stop":
				_ = e.Stop()
			case "cancel":
				e.Cancel()
			case "peer-close":
				if err := c.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "PRIVATE_REASON"), time.Now().Add(time.Second)); err != nil {
					t.Fatal(err)
				}
			case "tcp-close", "partial-tcp-close":
				if tc.name == "partial-tcp-close" {
					writeMaskedFragment(t, c, 0x02, []byte("part"))
				}
				_ = c.Close()
			case "protocol", "partial-protocol", "stopped-partial-protocol", "stopped-limit-protocol":
				if tc.name == "stopped-partial-protocol" || tc.name == "stopped-limit-protocol" {
					_ = e.StopInput()
				}
				opcode := byte(0x81)
				if tc.name != "protocol" {
					// 非末尾数据帧确保错误来自正文读取或停收后的丢弃路径。
					data := []byte("part")
					if tc.name == "stopped-limit-protocol" {
						data = make([]byte, cfg.ReadLimit)
					}
					writeMaskedFragment(t, c, 0x02, data)
					opcode = 0x80
				}
				if _, err := c.NetConn().Write([]byte{opcode, 0x01, 'x'}); err != nil {
					t.Fatal(err)
				}
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseProtocolError) {
					t.Fatalf("协议错误关闭码: %v", err)
				}
			case "invalid-text":
				if err := c.WriteMessage(websocket.TextMessage, []byte{255}); err != nil {
					t.Fatal(err)
				}
			case "read-limit":
				if err := c.WriteMessage(websocket.BinaryMessage, make([]byte, 33)); err != nil {
					t.Fatal(err)
				}
			case "pong-timeout":
				// 不运行客户端读循环，让服务端确实等不到 Pong。
			}
			event := await(t, closed)
			await(t, s.Done())
			if tc.name == "pong-timeout" {
				// 消费遗留 Ping 时不写 Pong；收到保留码 1006 帧会成为协议错误，而非异常 EOF。
				c.SetPingHandler(func(string) error { return nil })
				_ = c.SetReadDeadline(time.Now().Add(time.Second))
				if _, _, err := c.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseAbnormalClosure) {
					t.Fatalf("本地异常状态被写入关闭帧: %v", err)
				}
			}
			if event.Code != tc.code || !errors.Is(event.Err, tc.err) {
				t.Fatalf("关闭分类错误: code=%d err=%v，期望 code=%d err=%v", event.Code, event.Err, tc.code, tc.err)
			}
			if e.Budget().Usage() != (Usage{}) {
				t.Fatalf("读取失败后预算未释放: %+v", e.Budget().Usage())
			}
		})
	}
}

func TestCallbackDeadlineClassification(t *testing.T) {
	for _, kind := range []EventKind{EventOpened, EventHandled, EventRevalidated} {
		for _, cancelEarly := range []bool{false, true} {
			mode := "timeout"
			if cancelEarly {
				mode = "cancel-before-timeout"
			}
			t.Run(string(kind)+"/"+mode, func(t *testing.T) {
				type invocation struct {
					ctx     context.Context
					session *Session
				}
				entered := make(chan invocation, 1)
				release := make(chan struct{})
				observed := make(chan Event, 1)
				callback := func(ctx context.Context, s *Session) error {
					entered <- invocation{
						ctx:     ctx,
						session: s,
					}
					<-release
					return ctx.Err()
				}
				cfg := Config{
					MessageTimeout:     60 * time.Millisecond,
					RevalidateInterval: time.Millisecond,
					RevalidateTimeout:  60 * time.Millisecond,
					Observe: func(_ context.Context, event Event) {
						if event.Kind == kind {
							observed <- event
						}
					},
				}
				switch kind {
				case EventOpened:
					cfg.OnOpen = callback
				case EventHandled:
					cfg.OnMessage = func(ctx context.Context, s *Session, _ Message) error {
						return callback(ctx, s)
					}
				case EventRevalidated:
					cfg.Revalidate = callback
				}
				e, server := newTestEndpoint(t, cfg)
				// 在 Endpoint 清理之前释放回调，即使断言失败也不留下挂起任务。
				defer func() {
					if !isClosed(release) {
						close(release)
					}
				}()
				c := dialTest(t, server)
				if kind == EventHandled {
					if err := c.WriteMessage(websocket.TextMessage, []byte("request")); err != nil {
						t.Fatal(err)
					}
				}
				call := await(t, entered)
				want := error(context.DeadlineExceeded)
				if cancelEarly {
					want = context.Canceled
					e.Cancel()
					await(t, call.ctx.Done())
					// 回调晚于原期限退出，仍应保留先发生的主动取消。
					deadline, _ := call.ctx.Deadline()
					timer := time.NewTimer(time.Until(deadline) + 10*time.Millisecond)
					defer timer.Stop()
					await(t, timer.C)
				}
				await(t, call.session.Detached())
				if !errors.Is(call.ctx.Err(), want) {
					t.Errorf("回调 context 分类错误: %v，期望 %v", call.ctx.Err(), want)
				}
				if isClosed(call.session.Done()) {
					t.Fatal("回调未返回却已完成收尾")
				}
				close(release)
				event := await(t, observed)
				if !errors.Is(event.Err, want) {
					t.Errorf("回调观察分类错误: %v，期望 %v", event.Err, want)
				}
				await(t, call.session.Done())
			})
		}
	}
}
