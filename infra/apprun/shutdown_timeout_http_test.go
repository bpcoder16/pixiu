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

func TestHTTPShutdownIsBoundedByManagerTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	handlerDone := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	server, err := httpserver.New(httpserver.Config{
		Addr:            addr,
		ShutdownTimeout: time.Second,
	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		defer close(handlerDone)
		close(entered)
		// HTTP 仍在排空时，管理器可先按总期限返回。
		<-release
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		unblock()
		_ = server.Shutdown()
	}()
	manager, err := apprun.New(80 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Register(apprun.Service{
		Name:     "http",
		Run:      server.Run,
		Shutdown: server.Shutdown,
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
		response, err := client.Get("http://" + addr + "/work")
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- err
	}()
	receive(t, entered)
	defer func() {
		unblock()
		receive(t, handlerDone)
	}()
	cancel()
	if err := receive(t, done); !errors.Is(err, apprun.ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("业务未退出时没有按总期限返回: %v", err)
	}
	select {
	case <-handlerDone:
		t.Fatal("总期限返回时 handler 应仍在等待释放")
	default:
	}
	unblock()
	if err := receive(t, requestDone); err != nil {
		t.Fatalf("总期限不应强制中断 HTTP 排空: %v", err)
	}
}
