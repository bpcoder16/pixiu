package lifecycle

import (
	"context"
	"errors"
	"strings"
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

type blockingPanicValue struct {
	formatted chan struct{}
	release   <-chan struct{}
}

func (p *blockingPanicValue) String() string {
	select {
	case p.formatted <- struct{}{}:
	default:
	}
	<-p.release
	return "阻塞的 panic 文本"
}

func TestCloseTimeoutDoesNotFormatPanicValue(t *testing.T) {
	stack, err := New(500 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	value := &blockingPanicValue{
		formatted: make(chan struct{}, 1),
		release:   release,
	}
	started, panicked := make(chan struct{}), make(chan struct{})
	_ = stack.Register(func() error {
		close(started)
		<-release // 故意阻塞后续回收，验证 panic 不妨碍总期限生效。
		return nil
	})
	_ = stack.Register(func() error {
		close(panicked)
		panic(value)
	})
	firstDone := make(chan struct{})
	var recovered any
	go func() {
		defer close(firstDone)
		defer func() { recovered = recover() }()
		_ = stack.Close()
	}()
	defer func() {
		finish()
		select {
		case <-firstDone:
		case <-time.After(3 * time.Second):
			t.Error("解除阻塞后首次 Close 未结束")
		}
	}()
	select {
	case <-panicked:
	case <-time.After(3 * time.Second):
		t.Fatal("发生 panic 的回调未启动")
	}
	waiterDone := make(chan error, 1)
	go func() { waiterDone <- stack.Close() }()
	select {
	case <-firstDone:
	case <-value.formatted:
		t.Fatal("Close 同步格式化 panic 值，可能阻塞总回收期限")
	case <-time.After(3 * time.Second):
		t.Fatal("Close 未在总回收期限后返回")
	}
	if recovered != value {
		t.Fatal("首次 Close 未传播原始 panic 值")
	}
	select {
	case <-started:
	default:
		t.Fatal("panic 后未继续回收其余资源")
	}
	select {
	case err = <-waiterDone:
	case <-time.After(3 * time.Second):
		t.Fatal("并发 Close 未解除等待")
	}
	if !errors.Is(err, ErrCloseTimeout) || !errors.Is(err, context.DeadlineExceeded) || stack.Close() != err {
		t.Fatal("后续 Close 未保留相同的超时结果")
	}
	finish()
	if !strings.Contains(err.Error(), "lifecycle: close panic: 阻塞的 panic 文本") {
		t.Fatalf("汇总错误丢失 panic 信息: %v", err)
	}
}
