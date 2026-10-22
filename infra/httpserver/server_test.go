package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConfigTimeoutPolicy(t *testing.T) {
	for _, tt := range []struct {
		name  string
		cfg   Config
		read  time.Duration
		write time.Duration
	}{
		{
			name:  "零值使用普通 API 默认期限",
			read:  10 * time.Second,
			write: 15 * time.Second,
		},
		{
			name: "负值禁用读写超时",
			cfg: Config{
				ReadTimeout:  -1,
				WriteTimeout: -time.Second,
			},
			read:  -1,
			write: -time.Second,
		},
		{
			name: "正值保留调用方期限",
			cfg: Config{
				ReadTimeout:  3 * time.Second,
				WriteTimeout: 7 * time.Second,
			},
			read:  3 * time.Second,
			write: 7 * time.Second,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(tt.cfg, http.NotFoundHandler())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = stopAndWait(s.Stop, s.Wait, s.Cancel) })
			// 检查交给标准库的有效配置,避免仅验证 Config 副本而遗漏实际生效值。
			if s.http.ReadTimeout != tt.read || s.http.WriteTimeout != tt.write {
				t.Fatalf("读写期限 = %v/%v, 期望 %v/%v", s.http.ReadTimeout, s.http.WriteTimeout, tt.read, tt.write)
			}
			if s.http.ReadHeaderTimeout != 5*time.Second || s.http.IdleTimeout != time.Minute {
				t.Fatal("读写配置改变了请求头或空闲期限")
			}
		})
	}
}

func TestMaxHeaderBytesPolicy(t *testing.T) {
	for _, tt := range []struct {
		name   string
		limit  int
		size   int
		status int
	}{
		{
			name:   "默认允许普通请求头",
			size:   32 << 10,
			status: http.StatusNoContent,
		},
		{
			name: "默认拒绝过大请求头",
			// 避开标准库为解析预留的缓冲余量,验证实际请求拒绝行为。
			size:   80 << 10,
			status: http.StatusRequestHeaderFieldsTooLarge,
		},
		{
			name:   "支持显式增大上限",
			limit:  128 << 10,
			size:   80 << 10,
			status: http.StatusNoContent,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, addr, served := startTestServer(t, Config{MaxHeaderBytes: tt.limit}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			req, err := http.NewRequest(http.MethodGet, "http://"+addr, nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("X-Payload", strings.Repeat("a", tt.size))
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != tt.status {
				t.Fatalf("请求状态 = %d, 期望 %d", resp.StatusCode, tt.status)
			}
			if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
				t.Fatal(err)
			}
			if err := awaitResult(t, served); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestShutdownWaitsForRequest(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	s, addr, served := startTestServer(t, Config{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
		<-release
		_, _ = io.WriteString(w, "done")
	}))
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	requestDone := make(chan error, 1)
	go func() {
		resp, err := http.Get("http://" + addr)
		if err == nil {
			var body []byte
			body, err = io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if err == nil && string(body) != "done" {
				err = errors.New("响应未完整写入")
			}
		}
		requestDone <- err
	}()
	requestCtx := awaitResult(t, entered)
	shutdownDone := make(chan error, 2)
	for range 2 {
		go func() { shutdownDone <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("请求完成前关闭返回: %v", err)
	case <-requestCtx.Done():
		t.Fatal("排空期间取消了请求")
	case <-s.ctx.Done():
		t.Fatal("排空期间取消了内部 context")
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	if err := awaitResult(t, requestDone); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := awaitResult(t, shutdownDone); err != nil {
			t.Fatal(err)
		}
	}
	if err := awaitResult(t, served); err != nil {
		t.Fatal(err)
	}
	if s.ctx.Err() != context.Canceled {
		t.Fatal("正常关闭未取消内部 context")
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal("重复关闭结果", err)
	}
}

func TestValidationAndListenFailure(t *testing.T) {
	if _, err := New(Config{}, nil); err == nil {
		t.Fatal("接受 nil handler")
	}
	for _, cfg := range []Config{
		{ReadHeaderTimeout: -1},
		{IdleTimeout: -1},
		{MaxHeaderBytes: -1},
	} {
		if _, err := New(cfg, http.NotFoundHandler()); err == nil {
			t.Fatal("接受不允许为负值的限制")
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	s, err := New(Config{Addr: ln.Addr().String()}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopAndWait(s.Stop, s.Wait, s.Cancel) })
	if err := s.Run(); err == nil {
		t.Fatal("忽略监听失败")
	}
	if s.started {
		t.Fatal("监听失败后错误地标记为已启动")
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	if s.ctx.Err() != context.Canceled {
		t.Fatal("监听失败后的关闭未取消内部 context")
	}
}

func TestShutdownBeforeStart(t *testing.T) {
	s, err := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	if s.ctx.Err() != context.Canceled {
		t.Fatal("未启动实例的关闭未取消内部 context")
	}
	if err := s.Run(); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Run: %v", err)
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal("重复关闭结果", err)
	}
}

func TestStartOnce(t *testing.T) {
	s, _, served := startTestServer(t, Config{}, http.NotFoundHandler())
	if err := s.Run(); !errors.Is(err, ErrStarted) {
		t.Fatalf("重复 Run: %v", err)
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, served); err != nil {
		t.Fatal(err)
	}
}

func TestRunStateErrorBeforeListen(t *testing.T) {
	t.Run("固定端口重复启动", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		if err := ln.Close(); err != nil {
			t.Fatal(err)
		}
		s, _, served := startTestServer(t, Config{Addr: addr}, http.NotFoundHandler())
		if err := s.Run(); !errors.Is(err, ErrStarted) {
			t.Fatalf("固定端口重复 Run: %v, 期望 ErrStarted", err)
		}
		if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
			t.Fatal(err)
		}
		if err := awaitResult(t, served); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("关闭后地址被占用", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		s, err := New(Config{Addr: ln.Addr().String()}, http.NotFoundHandler())
		if err != nil {
			t.Fatal(err)
		}
		if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
			t.Fatal(err)
		}
		if err := s.Run(); !errors.Is(err, ErrClosed) {
			t.Fatalf("关闭后 Run: %v, 期望 ErrClosed", err)
		}
	})
}

func TestRunConcurrentShutdown(t *testing.T) {
	for range 30 {
		s, err := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- s.Run() }()
		if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
			t.Fatal(err)
		}
		if err := awaitResult(t, done); err != nil && !errors.Is(err, ErrClosed) {
			t.Fatal("启动/关闭竞争结果", err)
		}
		if s.ctx.Err() != context.Canceled {
			t.Fatal("启动/关闭竞争未取消内部 context")
		}
	}
}

func startTestServer(t *testing.T, cfg Config, handler http.Handler) (*Server, string, <-chan error) {
	t.Helper()
	if cfg.Addr == "" {
		cfg.Addr = "127.0.0.1:0"
	}
	s, err := New(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopAndWait(s.Stop, s.Wait, s.Cancel) })
	served := make(chan error, 1)
	// 通过标准库的监听初始化回调取得动态端口,不要求生产代码提供地址查询接口。
	listening := make(chan string, 1)
	s.http.BaseContext = func(ln net.Listener) context.Context {
		listening <- ln.Addr().String()
		return s.ctx
	}
	go func() { served <- s.Run() }()
	return s, awaitResult(t, listening), served
}

func awaitResult[T any](t *testing.T, result <-chan T) T {
	t.Helper()
	select {
	case value := <-result:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("等待测试事件超时")
		var zero T
		return zero
	}
}

func TestWaitDeadlineDoesNotCancelUntilRequested(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	s, addr, served := startTestServer(t, Config{}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- r.Context()
		<-release
	}))
	request := make(chan struct{})
	go func() {
		defer close(request)
		resp, _ := http.Get("http://" + addr)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	taskCtx := awaitResult(t, entered)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := awaitResult(t, served); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未限制排空等待: %v", err)
	}
	if taskCtx.Err() != nil {
		t.Fatal("Wait 超时不应自行取消请求")
	}
	s.Cancel()
	awaitResult(t, taskCtx.Done())
	awaitResult(t, request)
	ctxAfterCancel, cancelWait := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancelWait()
	if err := s.Wait(ctxAfterCancel); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Cancel 后 Handler 未返回时 Wait = %v, 期望超时", err)
	}
	unblock()
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal("Handler 返回后未完成排空", err)
	}
}
