package natsx

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamResumesConsumptionAndAckAfterServerRestart(t *testing.T) {
	for _, mode := range []string{"idle", "inflight"} {
		t.Run(mode, func(t *testing.T) {
			captureLogs(t)
			opts := &server.Options{
				Host:      "127.0.0.1",
				Port:      -1,
				JetStream: true,
				StoreDir:  t.TempDir(),
				NoLog:     true,
				NoSigs:    true,
			}
			s := startServerWithOptions(t, opts)
			port := s.Addr().(*net.TCPAddr).Port
			c, err := New(Config{
				Name: "restart",
				URLs: []string{s.ClientURL()},
			}, nats.ReconnectWait(20*time.Millisecond), nats.ReconnectJitter(0, 0))
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := c.RegisterStreams(ctx, jetstream.StreamConfig{
				Name:     "RESTART",
				Subjects: []string{"restart"},
				Storage:  jetstream.FileStorage,
			}); err != nil {
				t.Fatal(err)
			}
			type delivery struct {
				body string
				ctx  context.Context
			}
			received := make(chan delivery, 8)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			spec := DefaultJetStreamConsumer("RESTART", "worker", func(ctx context.Context, msg jetstream.Msg) error {
				body := string(msg.Data())
				select {
				case received <- delivery{body: body, ctx: ctx}:
				case <-ctx.Done():
					return ctx.Err()
				}
				if body == "inflight" {
					select {
					case <-release:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			if err := c.RegisterJetStreamConsumers(ctx, spec); err != nil {
				t.Fatal(err)
			}
			consumer, err := c.JetStream().Consumer(ctx, "RESTART", "worker")
			if err != nil {
				t.Fatal(err)
			}
			publish := func(body string) uint64 {
				t.Helper()
				ack, err := c.PublishJetStream(ctx, "restart", []byte(body), body)
				if err != nil {
					t.Fatal(err)
				}
				return ack.Sequence
			}
			waitMessage := func(want string) context.Context {
				t.Helper()
				select {
				case got := <-received:
					if got.body != want || got.ctx.Err() != nil {
						t.Fatalf("消息或生命周期错误: got=%q want=%q err=%v", got.body, want, got.ctx.Err())
					}
					return got.ctx
				case <-ctx.Done():
					t.Fatalf("未收到消息 %q", want)
					return nil
				}
			}
			waitAck := func(sequence uint64) {
				t.Helper()
				for {
					info, err := consumer.Info(ctx)
					if err != nil {
						t.Fatal(err)
					}
					// 检查服务端确认进度，不能把 handler 已执行当成 ACK 已送达。
					if info.AckFloor.Stream >= sequence && info.NumAckPending == 0 && info.NumPending == 0 {
						return
					}
					select {
					case <-ctx.Done():
						t.Fatalf("服务端未收到 ACK: %+v", info)
					case <-time.After(5 * time.Millisecond):
					}
				}
			}
			first := publish("before")
			waitMessage("before")
			waitAck(first)
			var inflight context.Context
			var inflightSequence uint64
			if mode == "inflight" {
				inflightSequence = publish("inflight")
				inflight = waitMessage("inflight")
			}
			states := c.Conn().StatusChanged(nats.RECONNECTING, nats.CONNECTED)
			defer c.Conn().RemoveStatusListener(states)
			waitState := func(want nats.Status) {
				t.Helper()
				select {
				case got := <-states:
					if got != want {
						t.Fatalf("连接状态错误: got=%v want=%v", got, want)
					}
				case <-ctx.Done():
					t.Fatalf("未收到连接状态 %v", want)
				}
			}
			s.Shutdown()
			s.WaitForShutdown()
			waitState(nats.RECONNECTING)
			if inflight != nil && inflight.Err() != nil {
				t.Fatal("临时断连取消了在途 handler")
			}
			// 在同一端口和存储目录重启真实 Server，复用已有 Durable，不重新注册消费。
			restartOpts := *opts
			restartOpts.Port = port
			startServerWithOptions(t, &restartOpts)
			waitState(nats.CONNECTED)
			unblock()
			if inflight != nil {
				waitAck(inflightSequence)
			}
			last := publish("after")
			waitMessage("after")
			waitAck(last)
			if err := c.Close(); err != nil {
				t.Fatalf("重连后关闭失败: %v", err)
			}
		})
	}
}

func TestCloseAfterCoreConnectionPermanentlyClosed(t *testing.T) {
	for _, mode := range []string{"idle", "inflight", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			s := startServerWithOptions(t, &server.Options{
				Host:   "127.0.0.1",
				Port:   -1,
				NoLog:  true,
				NoSigs: true,
			})
			closeTimeout := time.Second
			if mode == "timeout" {
				closeTimeout = 100 * time.Millisecond
			}
			c, err := New(Config{
				Name:         "permanent-close",
				URLs:         []string{s.ClientURL()},
				CloseTimeout: closeTimeout,
			}, nats.NoReconnect())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			entered := make(chan context.Context, 1)
			release := make(chan struct{}, 1)
			defer close(release)
			finished := make(chan struct{})
			if err := c.RegisterCoreConsumers(context.Background(), CoreConsumer{
				Name:    "worker",
				Subject: "work",
				Handler: func(ctx context.Context, _ *nats.Msg) error {
					entered <- ctx
					// 模拟取消后仍需要完成收尾的业务。
					<-release
					close(finished)
					return nil
				},
			}); err != nil {
				t.Fatal(err)
			}
			var handlerCtx context.Context
			if mode != "idle" {
				if err := c.Publish(context.Background(), "work", nil); err != nil {
					t.Fatal(err)
				}
				select {
				case handlerCtx = <-entered:
				case <-time.After(time.Second):
					t.Fatal("回调未开始")
				}
			}
			// 从服务端断开，不能用调用方主动关闭原生连接代替该故障路径。
			s.Shutdown()
			s.WaitForShutdown()
			select {
			case <-c.closedEvent:
			case <-time.After(time.Second):
				t.Fatal("连接未永久关闭")
			}
			if handlerCtx != nil && !errors.Is(handlerCtx.Err(), context.Canceled) {
				t.Fatal("永久关闭未取消在途消息 context")
			}
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			if mode == "inflight" {
				select {
				case err := <-closed:
					t.Fatalf("在途回调未结束就返回: %v", err)
				case <-time.After(50 * time.Millisecond):
				}
				release <- struct{}{}
			}
			select {
			case err := <-closed:
				if mode == "timeout" {
					if !errors.Is(err, ErrCloseTimeout) {
						t.Fatalf("在途回调未结束时应返回关闭超时: %v", err)
					}
					release <- struct{}{}
				} else if err != nil {
					t.Fatalf("连接已关闭且回调已结束，不应误报超时: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("Close 未结束")
			}
			if mode != "idle" {
				select {
				case <-finished:
				case <-time.After(time.Second):
					t.Fatal("回调未收尾")
				}
			}
		})
	}
}
