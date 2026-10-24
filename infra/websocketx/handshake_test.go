package websocketx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/gorilla/websocket"
)

func TestHijackedHandshakeSharesDeadlineAndCancelCloses(t *testing.T) {
	deadline := make(chan time.Time, 1)
	var opens atomic.Int32
	e, server := newTestEndpoint(t, Config{
		HandshakeTimeout: 500 * time.Millisecond,
		Authenticate: func(ctx context.Context, _ *http.Request) (Identity, error) {
			until, _ := ctx.Deadline()
			deadline <- until
			time.Sleep(30 * time.Millisecond)
			return Identity{}, nil
		},
		OnOpen: func(context.Context, *Session) error {
			opens.Add(1)
			return nil
		},
	})
	connections := make(chan *gateConn, 1)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.ServeHTTP(gateWriter{
			ResponseWriter: w,
			connections:    connections,
			arm:            true,
		}, r)
	})
	dialed := make(chan error, 1)
	go func() {
		c, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if c != nil {
			c.Close()
		}
		if response != nil {
			response.Body.Close()
		}
		dialed <- err
	}()
	until := await(t, deadline)
	gate := await(t, connections)
	await(t, gate.entered)
	gate.mu.Lock()
	writeUntil := gate.deadline
	gate.mu.Unlock()
	if delta := writeUntil.Sub(until); delta > 10*time.Millisecond || delta < -10*time.Millisecond {
		t.Fatalf("升级重置了握手期限: %v", delta)
	}
	e.Cancel()
	if err := await(t, dialed); err == nil {
		t.Fatal("取消后升级仍成功")
	}
	await(t, e.Done())
	if opens.Load() != 0 {
		t.Fatal("取消后运行 OnOpen")
	}
}

func TestCancelDoesNotWaitForObserve(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	e, server := newTestEndpoint(t, Config{
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventReceived {
				entered <- struct{}{}
				<-release
			}
		},
	})
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("x"))
	await(t, entered)
	started := time.Now()
	e.Cancel()
	if time.Since(started) > 500*time.Millisecond {
		t.Error("Cancel 等待了观察器")
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := c.ReadMessage(); err == nil {
		t.Error("取消后网络仍可读取")
	}
	select {
	case <-e.Done():
		t.Error("观察器未返回却已完成")
	default:
	}
	close(release)
	await(t, e.Done())
}

func TestClosingSessionRetainsConnectionQuota(t *testing.T) {
	opened := make(chan *Session, 2)
	closing := make(chan struct{}, 2)
	release := make(chan struct{})
	e, server := newTestEndpoint(t, Config{
		MaxConnections: 1,
		OnOpen: func(_ context.Context, s *Session) error {
			opened <- s
			return nil
		},
		OnClose: func(context.Context, *Session) {
			closing <- struct{}{}
			<-release
		},
	})
	c := dialTest(t, server)
	s := await(t, opened)
	c.Close()
	await(t, closing)
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err == nil || response == nil || response.StatusCode != 503 {
		t.Errorf("未收尾连接未占额度: %v %+v", err, response)
	}
	if response != nil {
		response.Body.Close()
	}
	close(release)
	await(t, s.Done())
	dialTest(t, server)
	s2 := await(t, opened)
	if s2.ID() == s.ID() || e.Len() != 1 {
		t.Fatal("连接 ID 被复用或索引错误")
	}
}

func TestCallbackPanicIsolatedAndLogsRedacted(t *testing.T) {
	var output bytes.Buffer
	logger := logit.MustNew(logit.OptWriter(logit.NewWriter(&output)))
	t.Cleanup(func() { _ = logit.Close(logger) })
	observed := make(chan Event, 16)
	e, server := newTestEndpoint(t, Config{
		Logger: logger,
		Authenticate: func(context.Context, *http.Request) (Identity, error) {
			return Identity{Subject: "PRIVATE_SUBJECT"}, nil
		},
		OnMessage: func(context.Context, *Session, Message) error {
			panic("PRIVATE_PANIC")
		},
		OnDetach: func(*Session) { panic("PRIVATE_DETACH") },
		OnClose:  func(context.Context, *Session) { panic("PRIVATE_CLOSE") },
		Observe:  func(_ context.Context, event Event) { observed <- event },
	})
	c, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"?ticket=PRIVATE_TICKET", http.Header{
		"Authorization": []string{"PRIVATE_AUTH"},
		"Cookie":        []string{"PRIVATE_COOKIE"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = c.WriteMessage(websocket.TextMessage, []byte("PRIVATE_BODY"))
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err = c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatal(err)
	}
	_ = e.Stop()
	_ = e.Wait(context.Background())
	if strings.Contains(output.String(), "PRIVATE_") {
		t.Fatalf("元信息日志泄漏: %s", output.String())
	}
	found := false
	closedFailure := false
	for len(observed) > 0 {
		event := <-observed
		found = found || errors.Is(event.Err, ErrPanic)
		closedFailure = closedFailure || event.Kind == EventClosed && errors.Is(event.Err, ErrPanic)
	}
	if !found || !closedFailure {
		t.Fatal("业务或收尾 panic 未被观察")
	}
}

func TestWrappedCallbackErrorsAreRedacted(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, ErrPanic} {
		t.Run(cause.Error(), func(t *testing.T) {
			var output bytes.Buffer
			logger := logit.MustNew(logit.OptWriter(logit.NewWriter(&output)))
			t.Cleanup(func() { _ = logit.Close(logger) })
			handled := make(chan Event, 1)
			e, server := newTestEndpoint(t, Config{
				Logger: logger,
				OnMessage: func(context.Context, *Session, Message) error {
					return fmt.Errorf("PRIVATE_CREDENTIAL: %w", cause)
				},
				Observe: func(_ context.Context, event Event) {
					if event.Kind == EventHandled {
						handled <- event
					}
				},
			})
			c := dialTest(t, server)
			if err := c.WriteMessage(websocket.TextMessage, []byte("request")); err != nil {
				t.Fatal(err)
			}
			event := await(t, handled)
			_ = e.Stop()
			_ = e.Wait(context.Background())
			if !errors.Is(event.Err, cause) || strings.Contains(event.Err.Error(), "PRIVATE_") || strings.Contains(output.String(), "PRIVATE_") {
				t.Fatalf("错误分类丢失或原始文本泄漏: event=%v log=%s", event.Err, output.String())
			}
		})
	}
}

type panicHandshakeConn struct{ net.Conn }

func (c panicHandshakeConn) Write([]byte) (int, error) { panic("handshake write panic") }

type panicHandshakeWriter struct{ http.ResponseWriter }

func (w panicHandshakeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.(http.Hijacker).Hijack()
	return panicHandshakeConn{conn}, rw, err
}

func TestPanicAfterHijackReclaimsConnection(t *testing.T) {
	observed := make(chan Event, 1)
	e, server := newTestEndpoint(t, Config{Observe: func(_ context.Context, event Event) { observed <- event }})
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.ServeHTTP(panicHandshakeWriter{w}, r)
	})
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if response != nil {
		response.Body.Close()
	}
	if err == nil {
		t.Fatal("panic 升级成功")
	}
	if event := await(t, observed); event.Kind != EventHandshake || !errors.Is(event.Err, ErrPanic) {
		t.Fatal(event)
	}
	_ = e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil || e.Len() != 0 {
		t.Fatalf("panic 升级未回收: %v", err)
	}
}

type panicErrorResponseWriter struct{ http.ResponseWriter }

func (w panicErrorResponseWriter) WriteHeader(int) { panic("error response write panic") }

func TestPanicWhileReportingHandshakePanicStillCleansUp(t *testing.T) {
	var authCtx context.Context
	observed := make(chan Event, 1)
	e, _ := newTestEndpoint(t, Config{
		Authenticate: func(ctx context.Context, _ *http.Request) (Identity, error) {
			authCtx = ctx
			panic("authentication panic")
		},
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventHandshake {
				observed <- event
			}
		},
	})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Header.Set("Connection", "Upgrade")
	r.Header.Set("Upgrade", "websocket")
	r.Header.Set("Sec-WebSocket-Version", "13")
	r.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	var panicValue any
	func() {
		defer func() { panicValue = recover() }()
		e.ServeHTTP(panicErrorResponseWriter{httptest.NewRecorder()}, r)
	}()
	if panicValue != "error response write panic" {
		t.Fatalf("未保留响应写入 panic: %v", panicValue)
	}
	if authCtx == nil || !errors.Is(authCtx.Err(), context.Canceled) {
		t.Error("握手 context 未取消")
	}
	e.mu.RLock()
	pending, connections := len(e.handshakes), e.connections
	e.mu.RUnlock()
	if pending != 0 || connections != 0 {
		t.Errorf("握手登记或连接额度未释放: pending=%d connections=%d", pending, connections)
	}
	_ = e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil {
		t.Fatalf("响应写入 panic 后无法收尾: %v", err)
	}
	if event := await(t, observed); event.Code != http.StatusInternalServerError || !errors.Is(event.Err, ErrPanic) {
		t.Fatalf("握手 panic 观察结果错误: %+v", event)
	}
}

func TestUpgradeSuccessBeforeReadinessLossReportsSwitchingProtocols(t *testing.T) {
	var ready atomic.Bool
	ready.Store(true)
	var opens atomic.Int32
	observed := make(chan Event, 1)
	e, server := newTestEndpoint(t, Config{
		CheckReady: ready.Load,
		OnOpen: func(context.Context, *Session) error {
			opens.Add(1)
			return nil
		},
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventHandshake {
				observed <- event
			}
		},
	})
	connections := make(chan *gateConn, 1)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.ServeHTTP(gateWriter{
			ResponseWriter: w,
			connections:    connections,
			arm:            true,
		}, r)
	})
	type dialResult struct {
		conn     *websocket.Conn
		response *http.Response
		err      error
	}
	dialed := make(chan dialResult, 1)
	go func() {
		conn, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		dialed <- dialResult{
			conn:     conn,
			response: response,
			err:      err,
		}
	}()
	gate := await(t, connections)
	await(t, gate.entered)
	// 在 101 写出前关闭就绪门，确保升级成功后无法交接 Session。
	ready.Store(false)
	close(gate.release)
	result := await(t, dialed)
	if result.conn != nil {
		defer result.conn.Close()
	}
	if result.response != nil {
		defer result.response.Body.Close()
	}
	if result.err != nil || result.response == nil || result.response.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("预期客户端已收到 101: response=%+v err=%v", result.response, result.err)
	}
	event := await(t, observed)
	if event.Code != http.StatusSwitchingProtocols || !errors.Is(event.Err, ErrClosed) {
		t.Errorf("已升级但未交接的握手观察结果错误: %+v", event)
	}
	await(t, gate.closed)
	_ = e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil || e.Len() != 0 || opens.Load() != 0 {
		t.Fatalf("升级后拒绝交接未正常回收: err=%v active=%d opens=%d", err, e.Len(), opens.Load())
	}
}

func TestOriginAndNegotiatedSubprotocol(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cfg    Config
		origin func(string) string
		want   int
	}{
		{"same", Config{}, func(url string) string { return url }, 101},
		{"scheme", Config{}, func(url string) string { return strings.Replace(url, "http:", "https:", 1) }, 403},
		{"missing", Config{RejectMissingOrigin: true}, func(string) string { return "" }, 403},
		{"any", Config{AllowAnyOrigin: true}, func(string) string { return "https://other.example" }, 101},
		{"not-ready", Config{CheckReady: func() bool { return false }}, func(string) string { return "" }, 503},
		{"auth-panic", Config{Authenticate: func(context.Context, *http.Request) (Identity, error) { panic("private") }}, func(string) string { return "" }, 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opened := make(chan *Session, 1)
			tc.cfg.Subprotocols = []string{"raw.v1"}
			tc.cfg.RequireSubprotocol = true
			tc.cfg.OnOpen = func(_ context.Context, s *Session) error {
				opened <- s
				return nil
			}
			_, server := newTestEndpoint(t, tc.cfg)
			headers := make(http.Header)
			if origin := tc.origin(server.URL); origin != "" {
				headers.Set("Origin", origin)
			}
			dialer := websocket.Dialer{Subprotocols: []string{"raw.v1"}}
			c, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), headers)
			if c != nil {
				defer c.Close()
			}
			if response == nil || response.StatusCode != tc.want {
				t.Fatalf("握手结果: %v %+v", err, response)
			}
			response.Body.Close()
			if tc.want == 101 {
				s := await(t, opened)
				if s.Subprotocol() != "raw.v1" || s.RemoteAddr() == "" {
					t.Fatal("协商或远端地址缺失")
				}
				_, _ = s.Close(websocket.CloseNormalClosure, "done")
				await(t, s.Detached())
				if s.Context().Err() == nil {
					t.Fatal("Detached 发布时 context 尚未取消")
				}
			}
		})
	}
}

func TestConcurrentAdmissionAndStop(t *testing.T) {
	for range 40 {
		var received atomic.Int32
		var handled atomic.Int32
		accepted := make(chan struct{}, 1)
		e, server := newTestEndpoint(t, Config{
			ReceiveQueueSize: 64,
			SendQueueSize:    64,
			OnMessage: func(ctx context.Context, s *Session, m Message) error {
				handled.Add(1)
				return s.Send(ctx, m)
			},
			Observe: func(_ context.Context, event Event) {
				if event.Kind == EventReceived && received.Add(1) == 16 {
					accepted <- struct{}{}
				}
			},
		})
		c := dialTest(t, server)
		for range 16 {
			if err := c.WriteMessage(websocket.TextMessage, []byte("x")); err != nil {
				t.Fatal(err)
			}
		}
		await(t, accepted)
		_ = e.Stop()
		for range 16 {
			readTest(t, c)
		}
		if err := e.Wait(context.Background()); err != nil || handled.Load() != received.Load() {
			t.Fatalf("接纳与停止竞争丢失: %v %d/%d", err, handled.Load(), received.Load())
		}
		c.Close()
		server.Close()
	}
}
