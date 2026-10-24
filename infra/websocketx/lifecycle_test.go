package websocketx

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHandshakeValidationBeforeAuthentication(t *testing.T) {
	var calls atomic.Int32
	origins := []string{"https://app.example"}
	protocols := []string{"raw.v1"}
	_, server := newTestEndpoint(t, Config{
		AllowedOrigins:     origins,
		Subprotocols:       protocols,
		RequireSubprotocol: true,
		Authenticate: func(context.Context, *http.Request) (Identity, error) {
			calls.Add(1)
			return Identity{}, errors.New("secret token")
		},
	})
	origins[0] = "https://evil.example"
	protocols[0] = "changed"
	for _, tc := range []struct {
		origin   string
		protocol string
		status   int
	}{
		{"https://evil.example", "raw.v1", 403},
		{"https://app.example", "wrong", 400},
		{"https://app.example", "raw.v1", 401},
	} {
		dialer := websocket.Dialer{Subprotocols: []string{tc.protocol}}
		c, response, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), http.Header{"Origin": []string{tc.origin}})
		if c != nil {
			c.Close()
		}
		if err == nil || response == nil || response.StatusCode != tc.status {
			t.Fatalf("握手拒绝错误: %v %+v", err, response)
		}
		response.Body.Close()
	}
	if calls.Load() != 1 {
		t.Fatalf("认证在前置校验前执行: %d", calls.Load())
	}
}

func TestAuthenticationCancellationKeepsRealOwnership(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var opened atomic.Int32
	e, server := newTestEndpoint(t, Config{
		MaxPendingHandshakes: 1,
		MaxConnections:       1,
		Authenticate: func(ctx context.Context, _ *http.Request) (Identity, error) {
			entered <- ctx
			<-release
			return Identity{}, nil
		},
		OnOpen: func(context.Context, *Session) error {
			opened.Add(1)
			return nil
		},
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
	ctx := await(t, entered)
	_, response, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err == nil || response.StatusCode != 503 {
		t.Fatalf("握手额度未保留: %v", err)
	}
	response.Body.Close()
	e.Cancel()
	await(t, ctx.Done())
	select {
	case <-e.Done():
		t.Fatal("认证回调未返回却已完成")
	default:
	}
	close(release)
	if err := await(t, dialed); err == nil {
		t.Fatal("取消后的认证结果仍升级")
	}
	await(t, e.Done())
	if opened.Load() != 0 {
		t.Fatal("迟到认证创建了会话")
	}
}

func TestHandlerTimeoutKeepsBudgetAndStopsNextMessage(t *testing.T) {
	entered := make(chan *Session, 1)
	release := make(chan struct{})
	var calls atomic.Int32
	e, server := newTestEndpoint(t, Config{
		MessageTimeout: 30 * time.Millisecond,
		OnMessage: func(_ context.Context, s *Session, _ Message) error {
			calls.Add(1)
			entered <- s
			<-release
			return nil
		},
	})
	defer close(release)
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("held"))
	s := await(t, entered)
	_ = c.WriteMessage(websocket.TextMessage, []byte("queued"))
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) {
		t.Fatal(err)
	}
	if s.Context().Err() == nil || e.Budget().Usage().ReceiveBytes < 4 || calls.Load() != 1 {
		t.Fatal("超时回调被提前释放或并发启动下一条")
	}
	select {
	case <-s.Done():
		t.Fatal("超时未返回回调被伪报结束")
	default:
	}
}

func TestSlowHandlerAndRevalidateDoNotBlockHeartbeat(t *testing.T) {
	entered := make(chan struct{}, 1)
	revalidateEntered := make(chan context.Context, 1)
	release := make(chan struct{})
	e, server := newTestEndpoint(t, Config{
		PingInterval:       10 * time.Millisecond,
		PongTimeout:        100 * time.Millisecond,
		RevalidateInterval: 5 * time.Millisecond,
		RevalidateTimeout:  time.Second,
		Revalidate: func(ctx context.Context, _ *Session) error {
			revalidateEntered <- ctx
			<-release
			return nil
		},
		OnMessage: func(ctx context.Context, s *Session, _ Message) error {
			entered <- struct{}{}
			<-release
			return s.Send(ctx, Message{
				Type: Text,
				Data: []byte("done"),
			})
		},
	})
	c := dialTest(t, server)
	pings := make(chan struct{}, 32)
	c.SetPingHandler(func(data string) error {
		pings <- struct{}{}
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	result := make(chan error, 1)
	go func() {
		_, data, err := c.ReadMessage()
		if err == nil && string(data) != "done" {
			err = errors.New("回复错误")
		}
		result <- err
	}()
	_ = c.WriteMessage(websocket.TextMessage, []byte("work"))
	await(t, entered)
	checkCtx := await(t, revalidateEntered)
	_ = e.StopInput()
	for range 12 {
		await(t, pings)
	}
	if checkCtx.Err() != nil {
		t.Fatal("StopInput 取消了已接纳复核")
	}
	close(release)
	if err := await(t, result); err != nil {
		t.Fatal(err)
	}
	_ = e.Stop()
	if err := e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestDetachBeforeInitializationReturnsAndCloseOnce(t *testing.T) {
	opened := make(chan *Session, 1)
	release := make(chan struct{})
	var detached atomic.Int32
	var closed atomic.Int32
	var e *Endpoint
	e, server := newTestEndpoint(t, Config{
		OnOpen: func(_ context.Context, s *Session) error {
			opened <- s
			<-release
			return nil
		},
		OnDetach: func(s *Session) {
			detached.Add(1)
			if _, ok := e.Get(s.ID()); ok {
				t.Error("OnDetach 时仍可路由")
			}
		},
		OnClose: func(context.Context, *Session) { closed.Add(1) },
	})
	dialTest(t, server)
	s := await(t, opened)
	for _, code := range []int{
		websocket.CloseNoStatusReceived, // 1005：未收到关闭状态，仅供本地表示，不能发送。
		websocket.CloseAbnormalClosure,  // 1006：未完成关闭握手的异常断开，不能发送。
		websocket.CloseTLSHandshake,     // 1015：TLS 握手失败，不能发送。
		2000,                            // 保留范围，不在允许的标准码或应用码范围内。
		5000,                            // 超出应用关闭码范围。
	} {
		if _, err := s.Close(code, "invalid"); err == nil {
			t.Fatalf("接受非法关闭码 %d", code)
		}
	}
	var first atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			ok, err := s.Close(websocket.CloseNormalClosure, "done")
			if err != nil {
				t.Error(err)
			}
			if ok {
				first.Add(1)
			}
		})
	}
	wg.Wait()
	if first.Load() != 1 || detached.Load() != 1 || closed.Load() != 0 {
		t.Fatalf("摘除/收尾时序错误: %d %d %d", first.Load(), detached.Load(), closed.Load())
	}
	close(release)
	await(t, s.Done())
	if closed.Load() != 1 || e.Len() != 0 {
		t.Fatal("迟到初始化复活或重复收尾")
	}
}

func TestIndependentWaitAndHistoricalFailures(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	e, server := newTestEndpoint(t, Config{
		OnMessage: func(context.Context, *Session, Message) error {
			entered <- struct{}{}
			<-release
			return errors.New("business failure")
		},
	})
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("work"))
	await(t, entered)
	// 历史错误在正常运行时结束，不能污染后续空闲关闭。
	close(release)
	deadline := time.Now().Add(time.Second)
	for e.Budget().Usage().ReceiveMessages != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.Budget().Usage().ReceiveMessages != 0 {
		t.Fatal("消息未释放")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := e.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_ = e.Stop()
	if err := e.Wait(context.Background()); err != nil {
		t.Fatalf("历史错误污染关闭: %v", err)
	}
	if err := e.Wait(ctx); err != nil {
		t.Fatalf("完成后结果不稳定: %v", err)
	}
}

func TestOpenTimeoutDoesNotDispatchMessages(t *testing.T) {
	entered := make(chan *Session, 1)
	release := make(chan struct{})
	var handled atomic.Int32
	e, server := newTestEndpoint(t, Config{
		MessageTimeout: 25 * time.Millisecond,
		OnOpen: func(_ context.Context, s *Session) error {
			entered <- s
			<-release
			return nil
		},
		OnMessage: func(context.Context, *Session, Message) error {
			handled.Add(1)
			return nil
		},
	})
	c := dialTest(t, server)
	s := await(t, entered)
	_ = c.WriteMessage(websocket.TextMessage, []byte("queued"))
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseInternalServerErr) || handled.Load() != 0 {
		t.Error("初始化超时仍派发了消息", err)
	}
	if s.Context().Err() == nil || e.Len() != 0 {
		t.Error("初始化超时未摘除")
	}
	close(release)
	await(t, s.Done())
}

func TestQueuedMessageExpirationIsReportedDuringStop(t *testing.T) {
	blocked := make(chan struct{}, 1)
	release := make(chan struct{})
	received := make(chan struct{}, 2)
	var handled atomic.Int32
	e, server := newTestEndpoint(t, Config{
		MessageTimeout: 40 * time.Millisecond,
		OnMessage: func(context.Context, *Session, Message) error {
			handled.Add(1)
			return nil
		},
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventReceived {
				received <- struct{}{}
			}
			if event.Kind == EventHandled {
				blocked <- struct{}{}
				<-release
			}
		},
	})
	c := dialTest(t, server)
	_ = c.WriteMessage(websocket.TextMessage, []byte("one"))
	await(t, received)
	await(t, blocked)
	_ = c.WriteMessage(websocket.TextMessage, []byte("two"))
	await(t, received)
	_ = e.Stop()
	time.Sleep(55 * time.Millisecond)
	close(release)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	_, _, err := c.ReadMessage()
	if !websocket.IsCloseError(err, websocket.CloseTryAgainLater) {
		t.Fatal(err)
	}
	var result *DrainError
	if err := e.Wait(context.Background()); !errors.As(err, &result) || result.ReceiveExpired != 1 || handled.Load() != 1 {
		t.Fatalf("排队期限或收尾统计错误: %v, handled=%d", err, handled.Load())
	}
}

func TestPartialMessageUsesBudgetAndIsNotAcceptedAtStop(t *testing.T) {
	var handled atomic.Int32
	e, server := newTestEndpoint(t, Config{OnMessage: func(context.Context, *Session, Message) error {
		handled.Add(1)
		return nil
	}})
	c := dialTest(t, server)
	w, err := c.NextWriter(websocket.BinaryMessage)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write(make([]byte, 8192)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for e.Budget().Usage().ReceiveBytes == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.Budget().Usage().ReceiveMessages != 1 || e.Budget().Usage().ReceiveBytes == 0 {
		t.Fatal("读取中的分片未计入预算")
	}
	_ = e.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	if handled.Load() != 0 || e.Budget().Usage() != (Usage{}) {
		t.Fatalf("半条消息被接纳或未释放: %d %+v", handled.Load(), e.Budget().Usage())
	}
	_ = w.Close()
}
