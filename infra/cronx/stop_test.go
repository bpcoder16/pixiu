package cronx

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
	s, err := New(Config{})
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
	ran := make(chan error, 1)
	go func() { ran <- s.Run() }()
	select {
	case err := <-ran:
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Cancel 后 Run = %v, 期望 ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Error("Cancel 后 Run 未返回")
	}
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal("Cancel 后重复停收", err)
	}
}

func TestCancelDuringRunWaitsForTask(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	s, ran := runScheduler(t, Config{}, Job{
		Name:  "blocked",
		Every: 100 * time.Millisecond,
		Handler: func(ctx context.Context) error {
			select {
			case entered <- ctx:
			default:
			}
			<-release
			return ctx.Err()
		},
	})
	var taskCtx context.Context
	select {
	case taskCtx = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("任务未启动")
	}
	s.Cancel()
	if !errors.Is(taskCtx.Err(), context.Canceled) {
		t.Fatalf("Cancel 未取消任务 context: %v", taskCtx.Err())
	}
	select {
	case err := <-ran:
		if err != nil {
			t.Errorf("运行中 Cancel 后 Run = %v", err)
		}
	case <-time.After(time.Second):
		t.Error("运行中 Cancel 后 Run 未返回")
	}
	if err := s.Run(); !errors.Is(err, ErrClosed) {
		t.Errorf("Cancel 后重复 Run = %v, 期望 ErrClosed", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("任务未返回时 Wait = %v, 期望超时", err)
	}
	finish()
	ctx, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := s.Wait(ctx); err != nil {
		t.Fatal("任务返回后 Wait 未完成", err)
	}
}
