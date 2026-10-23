package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"
)

var (
	// ErrClosed 表示关闭已开始，不能再登记关闭函数。
	ErrClosed = errors.New("lifecycle: stack closed")
	// ErrNilCloser 表示登记了 nil 关闭函数。
	ErrNilCloser = errors.New("lifecycle: nil close function")
	// ErrCloseTimeout 表示整个资源栈的回收期限耗尽；调用方应退出进程。
	ErrCloseTimeout = errors.New("lifecycle: close timeout")
)

// Stack 按登记逆序回收资源，全部回收共享期限；零值默认 20 秒，使用后不可复制。
type Stack struct {
	closeTimeout time.Duration
	mu           sync.Mutex
	closers      []func() error
	done         chan struct{}
	err          error
}

// closePanicError 延迟格式化 panic 值，避免用户的格式化方法阻塞总回收期限。
type closePanicError struct {
	value any
}

func (e *closePanicError) Error() string {
	return fmt.Sprintf("lifecycle: close panic: %v", e.value)
}

// New 设置整个栈的回收期限；零值使用 20 秒，负值非法。期限创建后不可修改。
func New(closeTimeout time.Duration) (*Stack, error) {
	if closeTimeout < 0 {
		return nil, fmt.Errorf("lifecycle: negative close timeout: %s", closeTimeout)
	}
	if closeTimeout == 0 {
		closeTimeout = 20 * time.Second
	}
	return &Stack{closeTimeout: closeTimeout}, nil
}

// Register 登记一个关闭函数。nil 函数返回 ErrNilCloser；关闭开始后返回 ErrClosed。
func (s *Stack) Register(closeFunc func() error) error {
	if closeFunc == nil {
		return ErrNilCloser
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done != nil {
		return ErrClosed
	}
	s.closers = append(s.closers, closeFunc)
	return nil
}

// Close 逆序执行全部关闭函数并汇总错误；后续调用等待并返回已收集的错误。
// 关闭函数不得递归调用同一个 Stack 的 Close。
// 关闭函数 panic 时继续执行其余关闭函数，随后向首次调用者传播第一个 panic。
// 后续调用返回包含 panic 信息及关闭错误的汇总结果。
// 超时停止发起后续关闭；正在执行的回调可能继续运行，调用方须随后退出进程。
func (s *Stack) Close() (err error) {
	s.mu.Lock()
	if s.done != nil {
		done := s.done
		s.mu.Unlock()
		<-done
		return s.err
	}
	done := make(chan struct{})
	s.done = done
	closers := s.closers
	s.closers = nil
	s.mu.Unlock()
	timeout := s.closeTimeout
	if timeout == 0 {
		timeout = 20 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var errs []error
	var firstPanic any
	// 在传播 panic 前发布汇总结果，放行其他等待 Close 的调用。
	defer func() {
		result := errors.Join(errs...)
		s.mu.Lock()
		s.err = result
		close(done)
		s.mu.Unlock()
		err = result
		if firstPanic != nil {
			panic(firstPanic)
		}
	}()
	for _, closer := range slices.Backward(closers) {
		if ctx.Err() != nil {
			errs = append(errs, ErrCloseTimeout, ctx.Err())
			return nil
		}
		type result struct {
			err        error
			panicValue any
		}
		// 缓冲允许超时后迟到的回调结束，不把调用方重新绑回无限等待。
		completed := make(chan result, 1)
		go func() {
			closeErr, recovered := callCloser(closer)
			completed <- result{
				err:        closeErr,
				panicValue: recovered,
			}
		}()
		var r result
		select {
		case r = <-completed:
		case <-ctx.Done():
			errs = append(errs, ErrCloseTimeout, ctx.Err())
			return nil
		}
		closeErr, recovered := r.err, r.panicValue
		if recovered != nil {
			if firstPanic == nil {
				firstPanic = recovered
			}
			errs = append(errs, &closePanicError{value: recovered})
		} else if closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	if ctx.Err() != nil {
		errs = append(errs, ErrCloseTimeout, ctx.Err())
	}
	return nil
}

func callCloser(closer func() error) (err error, recovered any) {
	defer func() { recovered = recover() }()
	return closer(), nil
}
