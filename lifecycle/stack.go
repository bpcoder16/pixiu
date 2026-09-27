package lifecycle

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	// ErrClosed 表示关闭已开始，不能再登记关闭函数。
	ErrClosed = errors.New("lifecycle: stack closed")
	// ErrNilCloser 表示登记了 nil 关闭函数。
	ErrNilCloser = errors.New("lifecycle: nil close function")
)

// Stack 按登记的逆序关闭资源；零值可用。
type Stack struct {
	mu      sync.Mutex
	closers []func() error
	done    chan struct{}
	err     error
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
		closeErr, recovered := callCloser(closer)
		if recovered != nil {
			if firstPanic == nil {
				firstPanic = recovered
			}
			errs = append(errs, fmt.Errorf("lifecycle: close panic: %v", recovered))
		} else if closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	return nil
}

func callCloser(closer func() error) (err error, recovered any) {
	defer func() { recovered = recover() }()
	return closer(), nil
}
