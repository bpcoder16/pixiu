package apprun_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

func TestHTTPHandlerWaitIsBoundedByManagerShutdownTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	server, err := httpserver.New(httpserver.Config{
		Addr:            addr,
		ShutdownTimeout: 20 * time.Millisecond,
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		close(entered)
		// 连接被强制关闭后仍保持业务运行，直到测试主动释放。
		<-release
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		unblock()
		_ = server.Shutdown()
		_ = server.Wait()
	}()
	manager, err := apprun.New(80 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Register(apprun.Service{
		Name: "http",
		Run:  server.Run,
		Shutdown: func() error {
			_ = server.Shutdown()
			return server.Wait()
		},
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	waitHTTP(t, client, "http://"+addr+"/healthz")
	requestDone := make(chan error, 1)
	go func() {
		response, err := client.Get("http://" + addr + "/work")
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	receive(t, entered)
	cancel()
	if err := receive(t, done); !errors.Is(err, apprun.ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("业务未退出时没有按总期限返回: %v", err)
	}
	if err := receive(t, requestDone); err == nil {
		t.Fatal("HTTP 组件排空超时没有关闭连接")
	}
}
