package lifecycle

import (
	"errors"
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
// 关闭函数 panic 时继续向上传播；尚未执行的关闭函数不会执行。
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
	// 即使关闭函数 panic，也要放行其他等待 Close 的调用。
	defer func() {
		result := errors.Join(errs...)
		s.mu.Lock()
		s.err = result
		close(done)
		s.mu.Unlock()
		err = result
	}()
	for _, closer := range slices.Backward(closers) {
		if closeErr := closer(); closeErr != nil {
			errs = append(errs, closeErr)
		}
	}
	return nil
}
