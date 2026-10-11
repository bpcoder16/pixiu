package apprun_test

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

func TestHTTPShutdownThenDependencyCleanup(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		name := "完整响应后退出"
		if timeout {
			name = "组件超时后继续清理"
		}
		t.Run(name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := listener.Addr().String()
			_ = listener.Close()
			manager, err := apprun.New(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var dependencyClosed atomic.Bool
			dependencyStop := make(chan struct{})
			if err := manager.Register(apprun.Service{
				Name: "dependency",
				Run: func() error {
					<-dependencyStop
					return nil
				},
				Shutdown: func() error {
					dependencyClosed.Store(true)
					close(dependencyStop)
					return nil
				},
			}); err != nil {
				t.Fatal(err)
			}
			entered := make(chan context.Context, 1)
			release := make(chan struct{})
			handlerDone := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			drainTimeout := 2 * time.Second
			if timeout {
				drainTimeout = 80 * time.Millisecond
			}
			server, err := httpserver.New(httpserver.Config{
				Addr:            addr,
				ShutdownTimeout: drainTimeout,
			}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/healthz" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				defer close(handlerDone)
				entered <- r.Context()
				if timeout {
					<-r.Context().Done()
				}
				<-release
				if !timeout && dependencyClosed.Load() {
					t.Error("请求处理期间依赖被关闭")
				}
				_, _ = io.WriteString(w, "complete")
			}))
			if err != nil {
				t.Fatal(err)
			}
			stopping := make(chan struct{})
			if err := manager.Register(apprun.Service{
				Name: "http",
				Run: func() error {
					err := server.Run()
					if errors.Is(err, httpserver.ErrClosed) {
						return nil
					}
					return err
				},
				Shutdown: func() error {
					close(stopping)
					return server.Shutdown()
				},
			}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- manager.Run(ctx) }()
			client := &http.Client{Timeout: 5 * time.Second}
			defer client.CloseIdleConnections()
			waitHTTP(t, client, "http://"+addr+"/healthz")
			requestDone := make(chan error, 1)
			go func() {
				resp, err := client.Get("http://" + addr + "/work")
				if err == nil {
					body, readErr := io.ReadAll(resp.Body)
					_ = resp.Body.Close()
					err = readErr
					if err == nil && string(body) != "complete" {
						err = errors.New("响应不完整")
					}
				}
				requestDone <- err
			}()
			requestCtx := receive(t, entered)
			defer func() {
				unblock()
				receive(t, handlerDone)
			}()
			cancel()
			receive(t, stopping)
			select {
			case <-requestCtx.Done():
				t.Fatal("排空开始时提前取消了请求")
			case err := <-done:
				t.Fatalf("请求未排空就返回: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			if timeout {
				receive(t, requestCtx.Done())
				// 保持 handler 阻塞，HTTP 自身超时后就应继续清理，不耗尽总期限。
			} else {
				unblock()
			}
			err = receive(t, done)
			if errors.Is(err, apprun.ErrShutdownTimeout) || timeout && !errors.Is(err, context.DeadlineExceeded) || !timeout && err != nil {
				t.Fatalf("关闭结果: %v", err)
			}
			if timeout {
				select {
				case <-handlerDone:
					t.Fatal("测试中的 handler 应仍在等待释放")
				default:
				}
			}
			requestErr := receive(t, requestDone)
			if !timeout && requestErr != nil {
				t.Fatal(requestErr)
			}
			if timeout && requestErr == nil {
				t.Fatal("HTTP 超时未强制关闭请求连接")
			}
			if !dependencyClosed.Load() {
				t.Fatal("未关闭依赖")
			}
		})
	}
}

func TestHTTPListenFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	server, err := httpserver.New(httpserver.Config{
		Addr: listener.Addr().String(),
	}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	var manager apprun.Manager
	if err := manager.Register(apprun.Service{
		Name:     "http",
		Run:      server.Run,
		Shutdown: server.Shutdown,
	}); err != nil {
		t.Fatal(err)
	}
	err = manager.Run(context.Background())
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr.Op != "listen" {
		t.Fatalf("监听错误丢失: %v", err)
	}
	if err := server.Run(); !errors.Is(err, httpserver.ErrClosed) {
		t.Fatalf("监听失败后未关闭服务: %v", err)
	}
}

func TestHTTPShutdownBeforeRunEntersServer(t *testing.T) {
	server, err := httpserver.New(httpserver.Config{
		Addr: "127.0.0.1:0",
	}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	var manager apprun.Manager
	entered, proceed := make(chan struct{}), make(chan struct{})
	if err := manager.Register(apprun.Service{
		Name: "http",
		Run: func() error {
			close(entered)
			<-proceed
			err := server.Run()
			if errors.Is(err, httpserver.ErrClosed) {
				return nil
			}
			return err
		},
		Shutdown: func() error {
			err := server.Shutdown()
			close(proceed)
			return err
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	receive(t, entered)
	cancel()
	if err := receive(t, done); err != nil {
		t.Fatalf("启动与停机竞争误报失败: %v", err)
	}
}

func waitHTTP(t *testing.T, client *http.Client, url string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("HTTP 未开始监听")
}

func receive[T any](t *testing.T, event <-chan T) T {
	t.Helper()
	select {
	case value := <-event:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("等待集成测试事件超时")
		var zero T
		return zero
	}
}
