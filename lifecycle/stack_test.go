package lifecycle_test

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestStackCloseReverseOrderAndJoinErrors(t *testing.T) {
	var stack lifecycle.Stack
	var order []string
	firstErr := errors.New("first close failed")
	lastErr := errors.New("last close failed")
	for _, item := range []struct {
		name string
		err  error
	}{
		{name: "first", err: firstErr},
		{name: "middle"},
		{name: "last", err: lastErr},
	} {
		item := item
		if err := stack.Register(func() error {
			order = append(order, item.name)
			return item.err
		}); err != nil {
			t.Fatalf("Register(%s): %v", item.name, err)
		}
	}

	err := stack.Close()
	if !reflect.DeepEqual(order, []string{"last", "middle", "first"}) {
		t.Fatalf("close order = %v", order)
	}
	if !errors.Is(err, firstErr) || !errors.Is(err, lastErr) {
		t.Fatalf("Close() = %v, want both errors", err)
	}
}

func TestStackConcurrentCloseRunsOnce(t *testing.T) {
	var stack lifecycle.Stack
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	wantErr := errors.New("close failed")
	if err := stack.Register(func() error {
		calls.Add(1)
		close(started)
		<-release
		return wantErr
	}); err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)
	go func() { results <- stack.Close() }()
	<-started
	go func() { results <- stack.Close() }()
	if err := stack.Register(func() error { return nil }); !errors.Is(err, lifecycle.ErrClosed) {
		t.Fatalf("Register after Close started = %v, want ErrClosed", err)
	}
	close(release)
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, wantErr) {
				t.Fatalf("Close() = %v, want %v", err, wantErr)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("concurrent Close did not finish")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("close calls = %d, want 1", got)
	}
	if err := stack.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("repeat Close() = %v, want %v", err, wantErr)
	}
}

func TestStackRejectsNilAndLateRegistration(t *testing.T) {
	var stack lifecycle.Stack
	if err := stack.Register(nil); !errors.Is(err, lifecycle.ErrNilCloser) {
		t.Fatalf("Register(nil) = %v, want ErrNilCloser", err)
	}
	if err := stack.Close(); err != nil {
		t.Fatalf("empty Close() = %v", err)
	}
	if err := stack.Register(func() error { return nil }); !errors.Is(err, lifecycle.ErrClosed) {
		t.Fatalf("Register after Close = %v, want ErrClosed", err)
	}
}

func TestStackPanicUnblocksWaitingClose(t *testing.T) {
	var stack lifecycle.Stack
	started := make(chan struct{})
	release := make(chan struct{})
	if err := stack.Register(func() error {
		close(started)
		<-release
		panic("close failed")
	}); err != nil {
		t.Fatal(err)
	}
	panics := make(chan any, 1)
	go func() {
		defer func() { panics <- recover() }()
		_ = stack.Close()
	}()
	<-started
	waiting := make(chan error, 1)
	go func() { waiting <- stack.Close() }()
	close(release)
	select {
	case recovered := <-panics:
		if recovered != "close failed" {
			t.Fatalf("panic = %v", recovered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Close did not panic")
	}
	select {
	case err := <-waiting:
		if err != nil {
			t.Fatalf("waiting Close() = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiting Close remained blocked after panic")
	}
}
