package logit

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
)

// Writer 是日志落盘目标的接口。实现:同步直写(NewWriter/OpenFile)
// 与轮转文件(NewRotateFile)。
//
// 契约:Write 必须并发安全且单次调用写入完整一行(如 O_APPEND 文件的单次 Write)。
// WriterKey 必须返回生命周期内不变的非零 key；同一 Writer 的副本共享 key。
type Writer interface {
	Write(p []byte) (int, error)
	Close() error
	WriterKey() WriterKey
}

// WriterKey 是可复制的 Writer 身份标识；零值无效。
type WriterKey struct{ id uint64 }

var writerKeySequence atomic.Uint64

// NewWriterKey 为自定义 Writer 创建新的身份标识。
func NewWriterKey() WriterKey {
	id := writerKeySequence.Add(1)
	if id == 0 {
		panic("logit: WriterKey exhausted")
	}
	return WriterKey{id: id}
}

// WriteErrorStats 暴露被日志调用吞掉的底层写入错误,供监控与健康检查使用。
type WriteErrorStats interface {
	WriteErrors() int64
	LastWriteError() error
}

type writeErrorState struct{ err error }

type writeErrorTracker struct {
	count atomic.Int64
	last  atomic.Pointer[writeErrorState]
}

func (s *writeErrorTracker) record(err error) {
	s.count.Add(1)
	s.last.Store(&writeErrorState{err: err})
}

func (s *writeErrorTracker) countValue() int64 { return s.count.Load() }

func (s *writeErrorTracker) lastValue() error {
	if state := s.last.Load(); state != nil {
		return state.err
	}
	return nil
}

// NewWriter 把任意 io.Writer 适配为并发安全的 Writer;底层实现 io.Closer 时
// Close 透传。非 *os.File 目标发生短写时会在同一把锁内继续，避免与其他调用交错。
func NewWriter(w io.Writer) Writer {
	// os.File 自身并发安全；额外锁让高并发写入先在 Go 互斥锁上排队。
	// io.Discard 无状态，仍使用无锁适配器。
	if f, ok := w.(*os.File); ok {
		return &fileWriter{f: f, key: NewWriterKey()}
	}
	if reflect.TypeOf(w) == reflect.TypeOf(io.Discard) {
		return &discardWriter{key: NewWriterKey()}
	}
	if wc, ok := w.(io.WriteCloser); ok {
		return &closerWriter{w: wc, key: NewWriterKey()}
	}
	return &plainWriter{w: w, key: NewWriterKey()}
}

// standardStreamWriter 借用进程标准流；关闭 Logger 不应关闭 stdout/stderr。
type standardStreamWriter struct{ *fileWriter }

func (*standardStreamWriter) Close() error { return nil }

var (
	stdoutWriter = &standardStreamWriter{&fileWriter{f: os.Stdout, key: NewWriterKey()}}
	stderrWriter = &standardStreamWriter{&fileWriter{f: os.Stderr, key: NewWriterKey()}}
)

// Stdout 返回共享的标准输出 Writer；Close 不关闭进程标准输出。
func Stdout() Writer { return stdoutWriter }

// Stderr 返回共享的标准错误输出 Writer；Close 不关闭进程标准错误输出。
func Stderr() Writer { return stderrWriter }

type discardWriter struct{ key WriterKey }

func (*discardWriter) Write(p []byte) (int, error) { return len(p), nil }
func (*discardWriter) Close() error                { return nil }
func (w *discardWriter) WriterKey() WriterKey      { return w.key }

type fileWriter struct {
	mu  sync.Mutex
	f   *os.File
	key WriterKey
}

func (w *fileWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Write(p)
}
func (w *fileWriter) Close() error         { return w.f.Close() }
func (w *fileWriter) Sync() error          { return w.f.Sync() }
func (w *fileWriter) WriterKey() WriterKey { return w.key }

// Fd 透传底层文件描述符，供 HookStdout/HookStderr 使用普通文件目标。
func (w *fileWriter) Fd() uintptr { return w.f.Fd() }

type plainWriter struct {
	mu  sync.Mutex
	w   io.Writer
	key WriterKey
}

func (p *plainWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return writeAll(p.w, b)
}
func (p *plainWriter) Close() error         { return nil }
func (p *plainWriter) WriterKey() WriterKey { return p.key }

// Sync 透传底层可能存在的 fsync 能力(如 *os.File)。
func (p *plainWriter) Sync() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.w.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return nil
}

type closerWriter struct {
	mu  sync.Mutex
	w   io.WriteCloser
	key WriterKey
}

func (c *closerWriter) Write(b []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return writeAll(c.w, b)
}
func (c *closerWriter) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.w.Close()
}
func (c *closerWriter) WriterKey() WriterKey { return c.key }

// Sync 透传底层可能存在的 fsync 能力(如 *os.File)。
func (c *closerWriter) Sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if s, ok := c.w.(interface{ Sync() error }); ok {
		return s.Sync()
	}
	return nil
}

func writeAll(w io.Writer, p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		n, err := w.Write(p)
		written += n
		if err != nil {
			return written, err
		}
		if n <= 0 || n > len(p) {
			return written, io.ErrShortWrite
		}
		p = p[n:]
	}
	return written, nil
}

func normalizeWriteError(n, want int, err error) error {
	if err != nil {
		return err
	}
	if n != want {
		return io.ErrShortWrite
	}
	return nil
}

func openFileAppend(path string) (*os.File, error) {
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("log: create log dir: %w", err)
		}
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

// OpenFile 以追加模式打开日志文件(自动创建父目录),返回同步 Writer。
func OpenFile(path string) (Writer, error) {
	f, err := openFileAppend(path)
	if err != nil {
		return nil, err
	}
	return NewWriter(f), nil
}
