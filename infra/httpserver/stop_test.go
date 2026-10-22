package httpserver

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

// stopAndWait 仅用于测试清理；生产调用方显式传递统一的排空期限。
func stopAndWait(stop func() error, wait func(context.Context) error, cancel func()) error {
	err := stop()
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()
	waitErr := wait(ctx)
	if waitErr != nil {
		cancel()
	}
	return errors.Join(err, waitErr)
}

func TestCancelBeforeRun(t *testing.T) {
	s, err := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopAndWait(s.Stop, s.Wait, s.Cancel) })
	s.Cancel()
	s.Cancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Wait(ctx); err != nil {
		t.Errorf("未启动实例 Cancel 后未完成关闭: %v", err)
	}
	if err := s.Run(); !errors.Is(err, ErrClosed) {
		t.Errorf("Cancel 后 Run = %v, 期望 ErrClosed", err)
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal("Cancel 后重复停收", err)
	}
}

type startupWriteFunc func([]byte) (int, error)

func (f startupWriteFunc) Write(p []byte) (int, error) { return f(p) }

func TestWaitIncludesStartup(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	logger := logit.MustNew(logit.OptWriter(logit.NewWriter(startupWriteFunc(func(p []byte) (int, error) {
		// 在监听成功、listener 尚未交给 Serve 时暂停启动。
		close(entered)
		<-release
		return len(p), nil
	}))))
	previous := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(previous)
		_ = logit.Close(logger)
	})
	s, err := New(Config{Addr: addr}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- s.Run() }()
	t.Cleanup(func() {
		unblock()
		_ = stopAndWait(s.Stop, s.Wait, s.Cancel)
	})
	awaitResult(t, entered)
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("启动未收尾时 Wait = %v, 期望超时", err)
	}
	unblock()
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	// Wait 成功后 listener 必须已经释放，允许其他实例接管地址。
	ln, err = net.Listen("tcp", addr)
	if err != nil {
		t.Fatal("Wait 完成后端口未释放", err)
	}
	_ = ln.Close()
	if err := awaitResult(t, served); err != nil {
		t.Fatal(err)
	}
}
