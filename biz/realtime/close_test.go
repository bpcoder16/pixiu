package realtime

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
)

func TestCancelIdleSessionWithoutOnClose(t *testing.T) {
	opened := make(chan string, 1)
	cfg := baseConfig()
	cfg.Transport.Observe = func(_ context.Context, event websocketx.Event) {
		if event.Kind == websocketx.EventOpened {
			opened <- event.ConnectionID
		}
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	raw, ok := server.endpoint.Get(await(t, opened))
	if !ok {
		t.Fatal("找不到已完成初始化的连接")
	}
	server.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := server.Wait(ctx); err != nil {
		t.Fatalf("空闲连接没有业务清理回调，排空应成功: %v", err)
	}
	if _, exists := server.sessions.Load(raw); exists {
		t.Fatal("断开后仍保留业务会话")
	}
}

func TestDetachDuringNewStateWithoutOnClose(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	cfg := baseConfig()
	cfg.NewState = func(ClientInfo) any {
		close(entered)
		<-release
		return nil
	}
	server, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	await(t, entered)
	var raw *websocketx.Session
	server.endpoint.Range(func(session *websocketx.Session) bool {
		raw = session
		return false
	})
	if raw == nil {
		t.Fatal("找不到正在初始化的连接")
	}
	// Close 同步完成摘除，再放行 NewState，保证摘除先于会话登记。
	if closed, err := raw.Close(websocket.CloseNormalClosure, "done"); err != nil || !closed {
		t.Fatalf("断开连接失败: closed=%v err=%v", closed, err)
	}
	unblock()
	await(t, raw.Done())
	if _, exists := server.sessions.Load(raw); exists {
		t.Fatal("初始化期间已摘除的连接被重新登记")
	}
}

// 正常断连与停收都允许业务使用清理 ctx；Done 必须等待清理真实返回。
func TestOnCloseIndependentContext(t *testing.T) {
	for _, mode := range []string{"peer-close", "server-stop"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan context.Context, 1)
			opened := make(chan *Session, 1)
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			cfg := baseConfig()
			cfg.OnOpen = func(_ context.Context, s *Session) error {
				opened <- s
				return nil
			}
			cfg.OnClose = func(ctx context.Context, _ *Session) {
				entered <- ctx
				<-release
			}
			server, h := startTestServer(t, cfg)
			defer unblock()
			client := dial(t, h, SubprotocolJSON)
			session := await(t, opened)
			if mode == "peer-close" {
				err := client.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(1000, "done"), time.Now().Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
			} else if err := server.Stop(); err != nil {
				t.Fatal(err)
			}
			cleanup := await(t, entered)
			if cleanup.Err() != nil || session.Context().Err() == nil {
				t.Fatalf("清理 ctx 与连接取消未分离: cleanup=%v session=%v", cleanup.Err(), session.Context().Err())
			}
			deadline, ok := cleanup.Deadline()
			if !ok || !deadline.After(time.Now()) || time.Until(deadline) > 5*time.Second {
				t.Fatalf("默认清理期限应为 5 秒: %v, %v", deadline, ok)
			}
			_ = server.Stop()
			waitCtx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
			defer cancel()
			if err := server.Wait(waitCtx); !errors.Is(err, context.DeadlineExceeded) || cleanup.Err() != nil {
				t.Fatalf("Wait 超时不应取消清理: wait=%v cleanup=%v", err, cleanup.Err())
			}
			select {
			case <-session.Done():
				t.Fatal("OnClose 未返回却提前完成 Session")
			default:
			}
			unblock()
			ctx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			if err := server.Wait(ctx); err != nil {
				t.Fatal(err)
			}
			await(t, session.Done())
			if !errors.Is(cleanup.Err(), context.Canceled) {
				t.Fatalf("回调返回后未释放清理 ctx: %v", cleanup.Err())
			}
		})
	}
}

func TestOnCloseDeadlineAndServerCancel(t *testing.T) {
	for _, mode := range []string{"deadline", "cancel-during-close", "cancel-before-close"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan context.Context, 1)
			opened := make(chan *Session, 1)
			canceled := make(chan error, 1)
			openCtx, releaseOpen := context.WithCancel(context.Background())
			defer releaseOpen()
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			cfg := baseConfig()
			if mode == "deadline" {
				cfg.Transport.OnCloseTimeout = 30 * time.Millisecond
			}
			cfg.OnOpen = func(_ context.Context, s *Session) error {
				opened <- s
				if mode == "cancel-before-close" {
					<-openCtx.Done()
				}
				return nil
			}
			cfg.OnClose = func(ctx context.Context, _ *Session) {
				entered <- ctx
				<-ctx.Done()
				canceled <- ctx.Err()
				// 模拟忽略取消的后续工作，验证期限不等于强制终止回调。
				<-release
			}
			server, h := startTestServer(t, cfg)
			defer unblock()
			dial(t, h, SubprotocolJSON)
			session := await(t, opened)
			if mode == "cancel-before-close" {
				server.Cancel()
				// 保证 Cancel 已返回后才允许进入 OnClose，验证取消能传给迟到清理。
				releaseOpen()
			} else {
				_ = server.Stop()
			}
			cleanup := await(t, entered)
			if mode == "cancel-before-close" && !errors.Is(cleanup.Err(), context.Canceled) {
				t.Fatalf("Cancel 后启动的清理未继承取消: %v", cleanup.Err())
			}
			if mode == "cancel-during-close" {
				if err := cleanup.Err(); err != nil {
					t.Fatalf("Cancel 前清理已失效: %v", err)
				}
				server.Cancel()
			}
			want := context.Canceled
			if mode == "deadline" {
				want = context.DeadlineExceeded
			}
			if err := await(t, canceled); !errors.Is(err, want) {
				t.Fatalf("清理取消原因 = %v，期望 %v", err, want)
			}
			select {
			case <-session.Done():
				t.Fatal("清理尚未返回就完成 Session")
			case <-server.Done():
				t.Fatal("清理尚未返回就完成 Server")
			default:
			}
			unblock()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := server.Wait(ctx); !errors.Is(err, websocketx.ErrDrainFailed) {
				t.Fatalf("停收期间清理超时/取消未计入关闭结果: %v", err)
			}
			await(t, session.Done())
		})
	}
}

func TestOnCloseReturnsPastDeadline(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	opened := make(chan struct{})
	closed := make(chan error, 1)
	cfg := baseConfig()
	cfg.Transport.OnCloseTimeout = time.Millisecond
	cfg.Transport.Observe = func(_ context.Context, event websocketx.Event) {
		if event.Kind == websocketx.EventClosed {
			closed <- event.Err
		}
	}
	cfg.OnOpen = func(context.Context, *Session) error {
		close(opened)
		return nil
	}
	cfg.OnClose = func(ctx context.Context, _ *Session) {
		// 单核运行至截止时间，覆盖回调先于定时器执行完毕的情形。
		deadline, _ := ctx.Deadline()
		for time.Now().Before(deadline) {
		}
	}
	s, h := startTestServer(t, cfg)
	dial(t, h, SubprotocolJSON)
	await(t, opened)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := await(t, closed); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("关闭观察未记录清理超时: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	var drain *websocketx.DrainError
	if err := s.Wait(ctx); !errors.As(err, &drain) || drain.CallbackErrors != 1 {
		t.Fatalf("排空应记录一次超时的清理回调: %v", err)
	}
}

func TestNegativeOnCloseTimeout(t *testing.T) {
	cfg := baseConfig()
	cfg.Transport.OnCloseTimeout = -time.Second
	if _, err := NewServer(cfg); err == nil {
		t.Fatal("不应接受负的清理期限")
	}
}
