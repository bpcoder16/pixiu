package websocketx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// gateConn 只在测试中暂停一次实际数据写入，保留真实 TCP 和握手。
type gateConn struct {
	net.Conn
	armed    atomic.Bool
	entered  chan struct{}
	release  chan struct{}
	closed   chan struct{}
	once     sync.Once
	mu       sync.Mutex
	deadline time.Time
}

func (c *gateConn) Write(p []byte) (int, error) {
	if c.armed.CompareAndSwap(true, false) {
		close(c.entered)
		c.mu.Lock()
		deadline := c.deadline
		c.mu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-c.release:
		case <-c.closed:
			return 0, net.ErrClosed
		case <-timer.C:
			return 0, context.DeadlineExceeded
		}
	}
	return c.Conn.Write(p)
}

func (c *gateConn) SetWriteDeadline(deadline time.Time) error {
	c.mu.Lock()
	c.deadline = deadline
	c.mu.Unlock()
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *gateConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type gateWriter struct {
	http.ResponseWriter
	connections chan *gateConn
	arm         bool
}

func (w gateWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.(http.Hijacker).Hijack()
	if err != nil {
		return nil, nil, err
	}
	gate := &gateConn{
		Conn:    conn,
		entered: make(chan struct{}),
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
	gate.armed.Store(w.arm)
	w.connections <- gate
	return gate, rw, nil
}

func gatedEndpoint(t *testing.T, cfg Config) (*Endpoint, *httptest.Server, <-chan *gateConn, <-chan *Session) {
	t.Helper()
	opened := make(chan *Session, 8)
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	e, server := newTestEndpoint(t, cfg)
	connections := make(chan *gateConn, 8)
	server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.ServeHTTP(gateWriter{
			ResponseWriter: w,
			connections:    connections,
		}, r)
	})
	return e, server, connections, opened
}

func TestSendOwnershipFIFOAndBudgetIncludesWriting(t *testing.T) {
	budget, err := NewBudget(BudgetConfig{SendBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	e, server, connections, opened := gatedEndpoint(t, Config{
		Budget:        budget,
		SendQueueSize: 2,
	})
	c := dialTest(t, server)
	s := await(t, opened)
	gate := await(t, connections)
	gate.armed.Store(true)
	input := []byte("one")
	if err := s.Send(context.Background(), Message{
		Type: Text,
		Data: input,
	}); err != nil {
		t.Fatal(err)
	}
	await(t, gate.entered)
	input[0] = 'X'
	preparedInput := []byte("two")
	prepared, err := e.Prepare(Message{
		Type: Text,
		Data: preparedInput,
	})
	if err != nil {
		t.Fatal(err)
	}
	preparedInput[0] = 'X'
	ctx, cancel := context.WithCancel(context.Background())
	if err := s.SendPrepared(ctx, prepared); err != nil {
		t.Fatal(err)
	}
	cancel() // 入队后取消调用方 context 不撤回任务。
	if err := s.Send(context.Background(), Message{
		Type: Text,
		Data: []byte("long"),
	}); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("本连接满应优先于共享满: %v", err)
	}
	if got := budget.Usage(); got.SendMessages != 2 || got.SendBytes != 6 {
		t.Fatalf("写入中项未计入: %+v", got)
	}
	other := dialTest(t, server)
	s2 := await(t, opened)
	await(t, connections)
	if err := s2.SendPrepared(context.Background(), prepared); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("预制消息必须按每连接计算积压: %v", err)
	}
	if err := s2.Send(context.Background(), Message{
		Type: Text,
		Data: []byte("ok"),
	}); err != nil {
		t.Fatal(err)
	}
	_, data := readTest(t, other)
	if string(data) != "ok" {
		t.Fatal(string(data))
	}
	close(gate.release)
	for _, want := range []string{"one", "two"} {
		_, data = readTest(t, c)
		if string(data) != want {
			t.Fatalf("所有权/FIFO 错误: %s", data)
		}
	}
	_ = e.Stop()
	if err := e.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if budget.Usage() != (Usage{}) {
		t.Fatal(budget.Usage())
	}
}

func TestPreparedValidationAndConcurrentReuse(t *testing.T) {
	e, server, connections, opened := gatedEndpoint(t, Config{MaxSendBytes: 4})
	for _, m := range []Message{
		{
			Type: Text,
			Data: []byte{255},
		},
		{Type: MessageType(3)},
		{
			Type: Binary,
			Data: []byte("12345"),
		},
		{
			Type:      Text,
			ExpiresAt: time.Now().Add(-time.Second),
		},
	} {
		if _, err := e.Prepare(m); err == nil {
			t.Fatalf("接受非法消息: %+v", m)
		}
	}
	peer, _ := New(Config{})
	t.Cleanup(func() {
		peer.Cancel()
		_ = peer.Wait(context.Background())
	})
	wrong, _ := peer.Prepare(Message{
		Type: Text,
		Data: []byte("x"),
	})
	prepared, _ := e.Prepare(Message{
		Type: Text,
		Data: []byte("same"),
	})
	var wg sync.WaitGroup
	clients := make([]*websocket.Conn, 8)
	for i := range clients {
		clients[i] = dialTest(t, server)
		s := await(t, opened)
		await(t, connections)
		if err := s.SendPrepared(context.Background(), wrong); !errors.Is(err, ErrWrongEndpoint) {
			t.Fatal(err)
		}
		if err := s.SendPrepared(context.Background(), nil); !errors.Is(err, ErrInvalidMessage) {
			t.Fatal(err)
		}
		wg.Go(func() {
			if err := s.SendPrepared(context.Background(), prepared); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	for _, c := range clients {
		_, data := readTest(t, c)
		if string(data) != "same" {
			t.Fatal(string(data))
		}
	}
}

func TestReceiveBoundsAndCompression(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{
			false: "plain",
			true:  "compressed",
		}[compressed], func(t *testing.T) {
			_, server := newTestEndpoint(t, Config{
				ReadLimit:         32,
				EnableCompression: compressed,
			})
			dialer := websocket.Dialer{EnableCompression: compressed}
			c, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if err := c.WriteMessage(websocket.BinaryMessage, bytes.Repeat([]byte("x"), 1024)); err != nil {
				t.Fatal(err)
			}
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _, err = c.ReadMessage()
			if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
				t.Fatalf("超限状态 = %v", err)
			}
		})
	}
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{
			false: "local",
			true:  "shared",
		}[shared], func(t *testing.T) {
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			cfg := Config{
				ReceiveQueueBytes: 4,
				OnMessage: func(context.Context, *Session, Message) error {
					entered <- struct{}{}
					<-release
					return nil
				},
			}
			if shared {
				cfg.ReceiveQueueBytes = 100
				cfg.Budget, _ = NewBudget(BudgetConfig{ReceiveBytes: 4})
			}
			e, server := newTestEndpoint(t, cfg)
			defer close(release)
			c := dialTest(t, server)
			_ = c.WriteMessage(websocket.TextMessage, []byte("1234"))
			await(t, entered)
			if e.Budget().Usage().ReceiveBytes != 4 {
				t.Fatal(e.Budget().Usage())
			}
			_ = c.WriteMessage(websocket.TextMessage, []byte("x"))
			_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
			_, _, err := c.ReadMessage()
			want := websocket.ClosePolicyViolation
			if shared {
				want = websocket.CloseTryAgainLater
			}
			if !websocket.IsCloseError(err, want) {
				t.Fatalf("接收错误归因: %v", err)
			}
		})
	}
}

func TestShutdownReportsExpirationAndCancellation(t *testing.T) {
	for _, cancelMode := range []bool{false, true} {
		t.Run(map[bool]string{
			false: "expiry",
			true:  "cancel",
		}[cancelMode], func(t *testing.T) {
			e, server, connections, opened := gatedEndpoint(t, Config{})
			c := dialTest(t, server)
			s := await(t, opened)
			gate := await(t, connections)
			gate.armed.Store(true)
			_ = s.Send(context.Background(), Message{
				Type: Text,
				Data: []byte("writing"),
			})
			await(t, gate.entered)
			_ = e.StopInput()
			_ = s.Send(context.Background(), Message{
				Type:      Text,
				Data:      []byte("queued"),
				ExpiresAt: time.Now().Add(20 * time.Millisecond),
			})
			_ = e.Stop()
			if cancelMode {
				e.Cancel()
			} else {
				time.Sleep(30 * time.Millisecond)
				close(gate.release)
				readTest(t, c)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			err := e.Wait(ctx)
			var failures *DrainError
			if !errors.As(err, &failures) || !errors.Is(err, ErrDrainFailed) {
				t.Fatalf("已知丢失必须进入关闭结果: %v", err)
			}
			if !cancelMode && failures.SendExpired != 1 {
				t.Fatal(failures)
			}
			if e.Budget().Usage() != (Usage{}) {
				t.Fatal(e.Budget().Usage())
			}
		})
	}
}
