package logit

import (
	"bytes"
	"sync"
	"testing"
)

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
