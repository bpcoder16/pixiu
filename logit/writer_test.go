package logit

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestFilePathsMustBeAbsolute(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	rel, err := filepath.Rel(cwd, filepath.Join(t.TempDir(), "app.log"))
	if err != nil {
		t.Fatal(err)
	}
	if filepath.IsAbs(rel) {
		t.Fatalf("测试路径不是相对路径: %q", rel)
	}

	w, err := OpenFile(rel)
	if w != nil {
		_ = w.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Errorf("OpenFile(%q) = %v, 预期绝对路径错误", rel, err)
	}

	l, err := New(OptFileName(rel))
	if l != nil {
		_ = Close(l)
	}
	if err == nil || !strings.Contains(err.Error(), "absolute") {
		t.Errorf("OptFileName(%q) = %v, 预期绝对路径错误", rel, err)
	}
}

func TestNewWriterSerializesConcurrentWrites(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	const goroutines = 16
	const writesPerGoroutine = 500

	var wg sync.WaitGroup
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range writesPerGoroutine {
				if _, err := w.Write([]byte("x\n")); err != nil {
					t.Errorf("Write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	if got, want := bytes.Count(buf.Bytes(), []byte{'\n'}), goroutines*writesPerGoroutine; got != want {
		t.Fatalf("complete lines = %d, want %d", got, want)
	}
}

type shortWriter struct{ buf bytes.Buffer }

func (w *shortWriter) Write(p []byte) (int, error) {
	if len(p) > 2 {
		p = p[:2]
	}
	return w.buf.Write(p)
}

func TestNewWriterCompletesShortWrites(t *testing.T) {
	underlying := &shortWriter{}
	w := NewWriter(underlying)
	input := []byte("complete line\n")
	n, err := w.Write(input)
	if err != nil || n != len(input) {
		t.Fatalf("Write = %d, %v; want %d, nil", n, err, len(input))
	}
	if !bytes.Equal(underlying.buf.Bytes(), input) {
		t.Fatalf("underlying content = %q, want %q", underlying.buf.Bytes(), input)
	}
}

func TestNewWriterPanicsOnNil(t *testing.T) {
	var typedNil *bytes.Buffer
	for _, w := range []interface{ Write([]byte) (int, error) }{nil, typedNil} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("NewWriter(%T) 未在调用时 panic", w)
				}
			}()
			_ = NewWriter(w)
		}()
	}
}

func TestStandardWriterKeysStayStable(t *testing.T) {
	stdout, stderr := Stdout(), Stderr()
	if stdout.WriterKey() == (WriterKey{}) || stderr.WriterKey() == (WriterKey{}) {
		t.Fatal("标准流 WriterKey 不得为零")
	}
	if stdout.WriterKey() != Stdout().WriterKey() || stderr.WriterKey() != Stderr().WriterKey() {
		t.Fatal("同一标准流应复用 WriterKey")
	}
	if stdout.WriterKey() == stderr.WriterKey() {
		t.Fatal("stdout 和 stderr 不得共享 WriterKey")
	}
}
