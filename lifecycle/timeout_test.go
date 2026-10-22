package lifecycle

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCloseSharesDeadlineAndFreezesResult(t *testing.T) {
	stack, err := New(60 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	var skipped atomic.Bool
	release, finished := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	want := errors.New("已完成的关闭失败")
	_ = stack.Register(func() error {
		skipped.Store(true)
		return nil
	})
	_ = stack.Register(func() error {
		defer close(finished)
		<-release
		return errors.New("迟到错误")
	})
	_ = stack.Register(func() error {
		time.Sleep(20 * time.Millisecond)
		return want
	})
	started := time.Now()
	err = stack.Close()
	if !errors.Is(err, ErrCloseTimeout) || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, want) {
		t.Fatalf("关闭结果丢失期限或已完成错误: %v", err)
	}
	if elapsed := time.Since(started); elapsed < 60*time.Millisecond || elapsed > time.Second {
		t.Fatalf("总期限未生效: %v", elapsed)
	}
	if skipped.Load() || stack.Close() != err {
		t.Fatal("超时后继续关闭或重复调用改变了结果")
	}
	finish()
	<-finished
	if stack.Close() != err {
		t.Fatal("迟到回调覆盖了已发布的超时结果")
	}
}

func TestNewCloseTimeout(t *testing.T) {
	if _, err := New(-time.Second); err == nil {
		t.Fatal("接受负期限")
	}
	s, err := New(0)
	if err != nil || s.closeTimeout != 20*time.Second {
		t.Fatalf("默认期限不是 20 秒: %+v, %v", s, err)
	}
}
