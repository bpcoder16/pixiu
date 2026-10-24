package websocketx

import (
	"bytes"
	"compress/flate"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// writeMaskedFragment 只构造本测试使用的短客户端分片，压缩流由标准库生成。
func writeMaskedFragment(t *testing.T, c *websocket.Conn, opcode byte, data []byte) {
	t.Helper()
	if len(data) >= 126 {
		t.Fatal("测试分片过长")
	}
	frame := []byte{opcode, 0x80 | byte(len(data)), 1, 2, 3, 4}
	for i, value := range data {
		frame = append(frame, value^byte(i%4+1))
	}
	if _, err := c.NetConn().Write(frame); err != nil {
		t.Fatal(err)
	}
}

func TestStopInputKeepsCompressedMessageLimit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		tail     int
		oversize bool
	}{
		{
			name: "below",
			tail: 4095,
		},
		{
			name: "exact",
			tail: 4096,
		},
		{
			name:     "over",
			tail:     4097,
			oversize: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opened := make(chan *Session, 1)
			e, server := newTestEndpoint(t, Config{
				ReadLimit:         8192,
				EnableCompression: true,
				DisableHeartbeat:  true,
				OnOpen: func(_ context.Context, s *Session) error {
					opened <- s
					return nil
				},
			})
			dialer := websocket.Dialer{EnableCompression: true}
			c, _, err := dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			s := await(t, opened)
			var compressed bytes.Buffer
			w, err := flate.NewWriter(&compressed, flate.BestSpeed)
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			_, _ = w.Write(bytes.Repeat([]byte("a"), 4096))
			_ = w.Flush()
			split := compressed.Len()
			writeMaskedFragment(t, c, 0x42, compressed.Bytes()) // RSV1 + Binary，尚未 FIN。
			deadline := time.Now().Add(time.Second)
			for s.Usage().ReceiveBytes != 8192 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			if s.Usage().ReceiveBytes != 8192 {
				t.Fatal("没有进入第二块读取")
			}
			_ = e.StopInput()
			_, _ = w.Write(bytes.Repeat([]byte("b"), tc.tail))
			_ = w.Flush()
			data := compressed.Bytes()
			writeMaskedFragment(t, c, 0x80, data[split:len(data)-4]) // FIN + Continuation，去掉压缩尾标记。
			// 服务端须先丢弃完整消息才能处理其后的 Ping；合法边界仍须保留连接。
			pong := errors.New("test: pong received")
			c.SetPongHandler(func(string) error { return pong })
			_ = c.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			_ = c.SetReadDeadline(time.Now().Add(time.Second))
			_, _, err = c.ReadMessage()
			if tc.oversize {
				if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
					t.Fatalf("StopInput 重置了解压限额: %v", err)
				}
			} else if !errors.Is(err, pong) {
				t.Fatalf("限额内的消息不应关闭连接: %v", err)
			}
			_ = e.Stop()
			if err := e.Wait(context.Background()); err != nil {
				t.Fatal(err)
			}
			await(t, s.Done())
			if e.Budget().Usage() != (Usage{}) {
				t.Fatal(e.Budget().Usage())
			}
		})
	}
}

type stopOnProbeReader struct {
	reader io.Reader
	stop   func()
	reads  int
	eof    bool
}

func (r *stopOnProbeReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads == 2 {
		r.stop()
	}
	n, err := r.reader.Read(p)
	if r.reads == 2 && r.eof {
		err = io.EOF
	}
	return n, err
}

func TestStopInputDuringBudgetProbe(t *testing.T) {
	// 精确停在零预算探测读取中，避免依赖 TCP 分包时机。
	for _, tc := range []struct {
		name   string
		shared bool
		eof    bool
	}{
		{name: "local"},
		{
			name:   "shared",
			shared: true,
		},
		{
			name: "complete",
			eof:  true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Config{ReceiveQueueBytes: 1}
			if tc.shared {
				cfg.ReceiveQueueBytes = 0
				cfg.Budget, _ = NewBudget(BudgetConfig{ReceiveBytes: 1})
			}
			e, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				e.Cancel()
				_ = e.Wait(context.Background())
			}()
			s := newSession(e, nil, "reading", Identity{})
			reader := &stopOnProbeReader{
				reader: strings.NewReader("ab"),
				stop:   func() { _ = e.StopInput() },
				eof:    tc.eof,
			}
			remaining := e.config.ReadLimit
			_, err = s.readData(reader, make([]byte, 4), &remaining)
			want := ErrClosed
			if tc.eof {
				want = io.EOF
			}
			if !errors.Is(err, want) {
				t.Fatalf("停收后未接纳的输入不应再归因为队列满: %v", err)
			}
			if s.Usage() != (Usage{}) || e.Budget().Usage() != (Usage{}) {
				t.Fatalf("部分读取未释放: %+v %+v", s.Usage(), e.Budget().Usage())
			}
		})
	}
}

func TestExpiredSendQueueDoesNotStarveHeartbeat(t *testing.T) {
	dropped := make(chan struct{}, 2)
	releaseDrop := make(chan struct{})
	e, server, connections, opened := gatedEndpoint(t, Config{
		PingInterval: 20 * time.Millisecond,
		PongTimeout:  5 * time.Second,
		Observe: func(_ context.Context, event Event) {
			if event.Kind == EventSendDropped && errors.Is(event.Err, ErrExpired) {
				dropped <- struct{}{}
				<-releaseDrop
			}
		},
	})
	defer close(releaseDrop)
	c := dialTest(t, server)
	s := await(t, opened)
	gate := await(t, connections)
	pings := make(chan struct{}, 16)
	c.SetPingHandler(func(data string) error {
		pings <- struct{}{}
		return c.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()
	defer func() {
		c.Close()
		await(t, readerDone)
	}()
	gate.armed.Store(true)
	if err := s.Send(context.Background(), Message{Type: Text}); err != nil {
		t.Fatal(err)
	}
	await(t, gate.entered)
	expires := time.Now().Add(25 * time.Millisecond)
	for range 2 {
		if err := s.Send(context.Background(), Message{
			Type:      Text,
			ExpiresAt: expires,
		}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Until(expires) + 10*time.Millisecond)
	close(gate.release)
	await(t, dropped)
	await(t, pings) // 第一条正常写入后的心跳，随后确认过期分支也会检查心跳。
	time.Sleep(30 * time.Millisecond)
	releaseDrop <- struct{}{}
	await(t, dropped)
	select {
	case <-pings:
	case <-time.After(500 * time.Millisecond):
		t.Error("连续过期消息跳过了应发送的心跳")
	}
	e.Cancel()
}

func TestPreparedCompressionVariantsAndSharedBudget(t *testing.T) {
	const count = 8
	input := bytes.Repeat([]byte("same"), 1024)
	want := bytes.Clone(input)
	budget, _ := NewBudget(BudgetConfig{SendBytes: int64(count * len(input))})
	e, server, connections, opened := gatedEndpoint(t, Config{
		Budget:            budget,
		EnableCompression: true,
		CompressionLevel:  6,
	})
	prepared, err := e.Prepare(Message{
		Type: Binary,
		Data: input,
	})
	if err != nil {
		t.Fatal(err)
	}
	clear(input)
	clients := make([]*websocket.Conn, count)
	sessions := make([]*Session, count)
	gates := make([]*gateConn, count)
	for i := range clients {
		dialer := websocket.Dialer{EnableCompression: i%2 == 0}
		clients[i], _, err = dialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { clients[i].Close() })
		sessions[i] = await(t, opened)
		gates[i] = await(t, connections)
		gates[i].armed.Store(true)
	}
	start := make(chan struct{})
	var sends sync.WaitGroup
	for _, s := range sessions {
		sends.Go(func() {
			<-start
			if err := s.SendPrepared(context.Background(), prepared); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	sends.Wait()
	for _, gate := range gates {
		await(t, gate.entered)
	}
	if usage := budget.Usage(); usage.SendMessages != count || usage.SendBytes != int64(count*len(want)) {
		t.Fatalf("共享预制帧未按各连接计算完整逻辑积压: %+v", usage)
	}
	peer, peerServer, peerConnections, peerOpened := gatedEndpoint(t, Config{Budget: budget})
	peerClient := dialTest(t, peerServer)
	peerSession := await(t, peerOpened)
	await(t, peerConnections)
	if err := peerSession.SendPrepared(context.Background(), prepared); !errors.Is(err, ErrWrongEndpoint) {
		t.Fatal(err)
	}
	message := Message{
		Type: Text,
		Data: []byte("still alive"),
	}
	if err := peerSession.Send(context.Background(), message); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("另一个 Endpoint 未受共享预算约束: %v", err)
	}
	_ = e.Stop()
	for i, gate := range gates {
		close(gate.release)
		typ, data := readTest(t, clients[i])
		if typ != websocket.BinaryMessage || !bytes.Equal(data, want) {
			t.Fatalf("压缩变体或复用正文错误: client=%d type=%d bytes=%d", i, typ, len(data))
		}
	}
	if err := e.Wait(context.Background()); err != nil || budget.Usage() != (Usage{}) {
		t.Fatalf("首个 Endpoint 未正确排空: %v %+v", err, budget.Usage())
	}
	if err := peerSession.Send(context.Background(), message); err != nil {
		t.Fatal(err)
	}
	_, data := readTest(t, peerClient)
	if string(data) != string(message.Data) || isClosed(peer.Done()) {
		t.Fatal("局部关闭影响了共享预算的另一入口")
	}
}

func TestConcurrentSendCloseStopAndCancelAccounting(t *testing.T) {
	for range 10 {
		var accepted, completed, detached, closed atomic.Int64
		e, server, connections, opened := gatedEndpoint(t, Config{
			OnDetach: func(*Session) { detached.Add(1) },
			OnClose:  func(context.Context, *Session) { closed.Add(1) },
			Observe: func(_ context.Context, event Event) {
				if event.Kind == EventSent || event.Kind == EventSendDropped {
					completed.Add(1)
				}
			},
		})
		c := dialTest(t, server)
		s := await(t, opened)
		gate := await(t, connections)
		gate.armed.Store(true)
		message := Message{
			Type: Binary,
			Data: []byte("payload"),
		}
		prepared, err := e.Prepare(message)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.SendPrepared(context.Background(), prepared); err != nil {
			t.Fatal(err)
		}
		accepted.Add(1)
		await(t, gate.entered)
		start := make(chan struct{})
		var workers sync.WaitGroup
		for i := range 4 {
			workers.Go(func() {
				<-start
				for range 64 {
					var err error
					if i%2 == 0 {
						err = s.SendPrepared(context.Background(), prepared)
					} else {
						err = s.Send(context.Background(), message)
					}
					if err == nil {
						accepted.Add(1)
					} else if !errors.Is(err, ErrClosed) && !errors.Is(err, ErrQueueFull) {
						t.Error(err)
					}
				}
			})
		}
		for range 4 {
			workers.Go(func() {
				<-start
				_ = e.StopInput()
				_ = e.Stop()
				e.Cancel()
				_, _ = s.Close(websocket.CloseNormalClosure, "done")
			})
		}
		close(start)
		workers.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err = e.Wait(ctx)
		cancel()
		if !errors.Is(err, ErrDrainFailed) || accepted.Load() != completed.Load() {
			t.Fatalf("取消结果或队列记账不完整: %v accepted=%d completed=%d", err, accepted.Load(), completed.Load())
		}
		if detached.Load() != 1 || closed.Load() != 1 || s.Usage() != (Usage{}) || e.Budget().Usage() != (Usage{}) {
			t.Fatalf("重复清理或预算未释放: detach=%d close=%d session=%+v budget=%+v", detached.Load(), closed.Load(), s.Usage(), e.Budget().Usage())
		}
		if err := s.SendPrepared(context.Background(), prepared); !errors.Is(err, ErrClosed) {
			t.Fatalf("完成后仍可接纳: %v", err)
		}
		c.Close()
		server.Close()
	}
}
