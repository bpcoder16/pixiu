package websocketx

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func newTestEndpoint(t testing.TB, cfg Config) (*Endpoint, *httptest.Server) {
	t.Helper()
	e, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(e)
	t.Cleanup(func() {
		e.Cancel()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := e.Wait(ctx); errors.Is(err, context.DeadlineExceeded) {
			t.Error("Endpoint 未实际退出")
		}
		server.Close()
	})
	return e, server
}

func dialTest(t testing.TB, server *httptest.Server) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
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

func readTest(t testing.TB, c *websocket.Conn) (int, []byte) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	typ, data, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	return typ, data
}

func TestConfigurationAndEarlyStop(t *testing.T) {
	for _, cfg := range []Config{
		{ReadLimit: -1},
		{ReceiveQueueSize: -1},
		{SendQueueBytes: -1},
		{MaxPendingHandshakes: -1},
		{MaxConnections: -1},
		{HandshakeTimeout: -1},
		{
			PingInterval: time.Second,
			PongTimeout:  time.Second,
		},
		{AllowedOrigins: []string{"https://example.com/path"}},
		{RequireSubprotocol: true},
	} {
		if _, err := New(cfg); err == nil {
			t.Errorf("非法配置被接受: %+v", cfg)
		}
	}
	e, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Wait(nil); !errors.Is(err, ErrNilContext) {
		t.Fatalf("Wait(nil) = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := e.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	e.Cancel()
	w := httptest.NewRecorder()
	e.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("关闭后状态 = %d", w.Code)
	}
}

func TestPendingHandshakeLimitDefaultAndOverride(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured int
		want       int
	}{
		{
			name: "default",
			want: 256,
		},
		{
			name:       "lower",
			configured: 128,
			want:       128,
		},
		{
			name:       "higher",
			configured: 512,
			want:       512,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newTestEndpoint(t, Config{MaxPendingHandshakes: tc.configured})
			if got := e.config.MaxPendingHandshakes; got != tc.want {
				t.Fatalf("握手上限 = %d，期望 %d", got, tc.want)
			}
		})
	}
}

func TestConnectionLimitDefaultAndOverride(t *testing.T) {
	for _, tc := range []struct {
		name       string
		configured int
		want       int
	}{
		{
			name: "default",
			want: 10_000,
		},
		{
			name:       "lower",
			configured: 1,
			want:       1,
		},
		{
			name:       "higher",
			configured: 20_000,
			want:       20_000,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, _ := newTestEndpoint(t, Config{MaxConnections: tc.configured})
			if got := e.config.MaxConnections; got != tc.want {
				t.Fatalf("连接上限 = %d，期望 %d", got, tc.want)
			}
		})
	}
}

func TestRawMessagesAndIndependentSession(t *testing.T) {
	opened := make(chan *Session, 1)
	e, server := newTestEndpoint(t, Config{
		Authenticate: func(ctx context.Context, r *http.Request) (Identity, error) {
			return Identity{Subject: "alice"}, nil
		},
		OnOpen: func(ctx context.Context, s *Session) error {
			opened <- s
			return nil
		},
		OnMessage: func(ctx context.Context, s *Session, m Message) error {
			return s.Send(ctx, m)
		},
	})
	c := dialTest(t, server)
	s := await(t, opened)
	server.Close()
	if s.Context().Err() != nil || s.Identity().Subject != "alice" || e.Len() != 1 {
		t.Fatal("HTTP 返回或关闭影响了独立会话")
	}
	for _, input := range []Message{
		{
			Type: Text,
			Data: []byte("  第一行\n第二行  "),
		},
		{
			Type: Binary,
			Data: []byte{0, 255, 1, 10},
		},
		{
			Type: Text,
			Data: []byte{},
		},
	} {
		if err := c.WriteMessage(int(input.Type), input.Data); err != nil {
			t.Fatal(err)
		}
		typ, data := readTest(t, c)
		if typ != int(input.Type) || !bytes.Equal(data, input.Data) {
			t.Fatalf("消息发生改变: %d %q", typ, data)
		}
	}
	if got, ok := e.Get(s.ID()); !ok || got != s {
		t.Fatal("主表查询失败")
	}
	e.Range(func(got *Session) bool {
		if _, err := got.Close(websocket.CloseNormalClosure, "done"); err != nil {
			t.Error(err)
		}
		return true
	})
	await(t, s.Done())
	if e.Len() != 0 {
		t.Fatal("已摘除连接仍可路由")
	}
}

func TestStopInputDrainsAcceptedMessagesAndPreservesOutput(t *testing.T) {
	entered := make(chan *Session, 1)
	release := make(chan struct{})
	received := make(chan struct{}, 4)
	var calls atomic.Int32
	e, server := newTestEndpoint(t, Config{
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventReceived {
				received <- struct{}{}
			}
		},
		OnMessage: func(ctx context.Context, s *Session, m Message) error {
			if calls.Add(1) == 1 {
				entered <- s
				<-release
			}
			return s.Send(ctx, m)
		},
	})
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("one"))
	s := await(t, entered)
	await(t, received)
	_ = c.WriteMessage(websocket.TextMessage, []byte("two"))
	await(t, received)
	_ = e.StopInput()
	if err := s.Send(context.Background(), Message{
		Type: Text,
		Data: []byte("before-drain"),
	}); err != nil {
		t.Fatal(err)
	}
	_, data := readTest(t, c)
	if string(data) != "before-drain" {
		t.Fatal(string(data))
	}
	_ = c.WriteMessage(websocket.TextMessage, []byte("ignored"))
	_ = e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := e.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) || s.Context().Err() != nil {
		t.Fatalf("Wait 不应取消业务: %v", err)
	}
	close(release)
	for _, want := range []string{"one", "two"} {
		_, data = readTest(t, c)
		if string(data) != want {
			t.Fatalf("顺序错误: %s", data)
		}
	}
	if err := e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || e.Budget().Usage() != (Usage{}) {
		t.Fatalf("上行接纳或预算错误: %d %+v", calls.Load(), e.Budget().Usage())
	}
}

func TestCancelDetachesButTracksHungCallback(t *testing.T) {
	entered := make(chan *Session, 1)
	detached := make(chan struct{}, 1)
	release := make(chan struct{})
	var closed atomic.Int32
	e, server := newTestEndpoint(t, Config{
		MaxConnections: 1,
		OnMessage: func(ctx context.Context, s *Session, m Message) error {
			entered <- s
			<-release
			return nil
		},
		OnDetach: func(s *Session) { detached <- struct{}{} },
		OnClose:  func(ctx context.Context, s *Session) { closed.Add(1) },
	})
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("held"))
	s := await(t, entered)
	e.Cancel()
	await(t, detached)
	if e.Len() != 0 || s.Context().Err() == nil || e.Budget().Usage().ReceiveBytes != 4 {
		t.Fatalf("取消后状态错误: %+v", e.Budget().Usage())
	}
	select {
	case <-s.Done():
		t.Fatal("挂起回调被伪报结束")
	default:
	}
	if err := s.Send(context.Background(), Message{Type: Text}); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	close(release)
	await(t, e.Done())
	if closed.Load() != 1 || e.Budget().Usage() != (Usage{}) {
		t.Fatalf("未正确回收: %d %+v", closed.Load(), e.Budget().Usage())
	}
}
