package taskpool

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func testConfig() Config {
	return Config{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1}
}

func waitUntil(t *testing.T, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("等待状态变化超时")
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{MinWorkers: 2, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1},
		{MinWorkers: 1, MaxWorkers: 1, MaxRetries: 1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 0},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 101},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: int(^uint(0) >> 1)},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, SubmitTimeout: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, IdleTimeout: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, DrainTimeout: -1},
	} {
		if pool, err := New(context.Background(), cfg); err == nil || pool != nil {
			t.Fatalf("New(%+v) = %v, %v, want error", cfg, pool, err)
		}
	}
}

func TestNewRejectsInvalidContext(t *testing.T) {
	if p, err := New(nil, testConfig()); p != nil || !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("New(nil) = %v, %v, want ErrInvalidConfig", p, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if p, err := New(ctx, testConfig()); p != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("New(canceled) = %v, %v, want context canceled", p, err)
	}
}

func TestNewAcceptsMaximumRetries(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 100
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultDrainTimeout(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.DrainTimeout != 15*time.Second {
		t.Fatalf("DrainTimeout = %v, want 15s", p.cfg.DrainTimeout)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitWaitsForSpaceAndHonorsContext(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "first", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "second", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := p.Submit(ctx, "third", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("满队列 Submit = %v, want deadline exceeded", err)
	}
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats(); got.Succeeded != 2 || got.Accepted != 2 {
		t.Fatalf("Stats = %+v, want 2 accepted and succeeded", got)
	}
}

func TestConfiguredSubmitTimeoutLimitsFullQueueWait(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = 20 * time.Millisecond
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), "timed-out", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("配置超时后的 Submit = %v, want deadline exceeded", err)
	}
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats(); got.Accepted != 2 {
		t.Fatalf("Stats = %+v, want 2 accepted", got)
	}
}

func TestDefaultSubmitTimeoutLimitsFullQueueWait(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		close(release)
		if err := p.Shutdown(); err != nil {
			t.Error(err)
		}
	}()
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	type submitResult struct {
		err     error
		elapsed time.Duration
	}
	result := make(chan submitResult, 1)
	go func() {
		start := time.Now()
		err := p.Submit(context.Background(), "timed-out", func(context.Context) error { return nil })
		result <- submitResult{err: err, elapsed: time.Since(start)}
	}()
	select {
	case got := <-result:
		if !errors.Is(got.err, context.DeadlineExceeded) || got.elapsed < time.Second {
			t.Fatalf("默认满队列 Submit = %v, 等待 %v，want deadline exceeded after at least 1s", got.err, got.elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("默认满队列 Submit 未在 3 秒内返回")
	}
}

func TestCallerCancellationPreemptsConfiguredSubmitTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = time.Second
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(10*time.Millisecond, cancel)
	defer timer.Stop()
	if err := p.Submit(ctx, "canceled", func(context.Context) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("调用方取消后的 Submit = %v, want context canceled", err)
	}
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestConfiguredSubmitTimeoutDoesNotLimitImmediateAdmission(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = time.Nanosecond
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), "available", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("空队列 Submit = %v", err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestAcceptedTaskSurvivesSubmitContextCancellation(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	start := make(chan struct{})
	result := make(chan error, 1)
	if err := p.Submit(ctx, "independent", func(taskCtx context.Context) error {
		<-start
		result <- taskCtx.Err()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	cancel()
	close(start)
	if got := <-result; got != nil {
		t.Fatalf("task context after submit cancellation = %v", got)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestWaitingSubmitSucceedsAfterSpaceOpens(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = time.Second
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- p.Submit(context.Background(), "waiting", func(context.Context) error { return nil })
	}()
	select {
	case err := <-result:
		t.Fatalf("队列仍满时 Submit 提前返回: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("有空位后 Submit 未返回")
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats(); got.Succeeded != 3 {
		t.Fatalf("Stats = %+v, want 3 succeeded", got)
	}
}

func TestWorkersExpandAndShrinkWithinRange(t *testing.T) {
	cfg := Config{MinWorkers: 1, MaxWorkers: 3, QueueSize: 3, MaxRetries: 1, IdleTimeout: 20 * time.Millisecond}
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	started := make(chan struct{}, 3)
	for range 3 {
		if err := p.Submit(context.Background(), "busy", func(context.Context) error {
			started <- struct{}{}
			<-release
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("未扩容到 3 个消费者")
		}
	}
	if got := p.Stats(); got.Workers != 3 || got.Running != 3 {
		t.Fatalf("扩容后 Stats = %+v", got)
	}
	close(release)
	waitUntil(t, func() bool {
		got := p.Stats()
		return got.Workers == 1 && got.Succeeded == 3
	})
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestRetryDoesNotBlockOnFullQueue(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 1
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var firstFailureAt time.Time
	var retryElapsed time.Duration
	if err := p.Submit(context.Background(), "retry", func(context.Context) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
			firstFailureAt = time.Now()
			return errors.New("temporary")
		}
		retryElapsed = time.Since(firstFailureAt)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "queued", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := p.Shutdown(); err != nil {
		t.Fatalf("Shutdown with full queue retry: %v", err)
	}
	if calls.Load() != 2 || p.Stats().Succeeded != 2 {
		t.Fatalf("calls = %d, Stats = %+v", calls.Load(), p.Stats())
	}
	if retryElapsed < time.Second {
		t.Fatalf("重试间隔 = %v, want at least 1s", retryElapsed)
	}
}

func TestShutdownDrainsAndRejectsWaiters(t *testing.T) {
	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- p.Submit(context.Background(), "waiter", func(context.Context) error { return nil }) }()
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown() }()
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("等待中的 Submit = %v, want ErrClosed", err)
	}
	close(release)
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if got := p.Stats(); got.Succeeded != 2 || got.Workers != 0 {
		t.Fatalf("关闭后 Stats = %+v", got)
	}
	if err := p.Submit(context.Background(), "late", func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后 Submit = %v", err)
	}
}

func TestParentCancellationDrainsAndRejectsWaiters(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	p, err := New(runCtx, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	taskCtxErr := make(chan error, 2)
	if err := p.Submit(context.Background(), "running", func(ctx context.Context) error {
		close(started)
		<-release
		taskCtxErr <- ctx.Err()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(ctx context.Context) error {
		taskCtxErr <- ctx.Err()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	submitting := make(chan struct{})
	go func() {
		close(submitting)
		result <- p.Submit(context.Background(), "waiting", func(context.Context) error { return nil })
	}()
	<-submitting
	select {
	case err := <-result:
		t.Fatalf("队列仍满时 Submit 提前返回: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	stop()
	select {
	case err := <-result:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("停机后的 Submit = %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("停机后等待中的 Submit 未返回")
	}
	waited := make(chan error, 1)
	go func() { waited <- p.Wait() }()
	select {
	case err := <-waited:
		t.Fatalf("运行中任务尚未完成时 Wait 提前返回: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := <-taskCtxErr; err != nil {
			t.Fatalf("排空期间任务 context 被取消: %v", err)
		}
	}
	if got := p.Stats(); got.Succeeded != 2 || got.Workers != 0 {
		t.Fatalf("停机排空后 Stats = %+v", got)
	}
	if err := p.Submit(context.Background(), "late", func(context.Context) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("停机后 Submit = %v, want ErrClosed", err)
	}
}

func TestWaitBeforeStopDoesNotStartDrainTimeout(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(runCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- p.Wait() }()
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.terminalCalled
	})
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-waited:
		t.Fatalf("停机前 Wait 提前返回: %v", err)
	default:
	}
	if err := p.Submit(context.Background(), "still-open", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("停机前 Submit = %v", err)
	}
	stop()
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalMethodsCanOnlyBeCalledOnce(t *testing.T) {
	expectPanic := func(t *testing.T, call func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("重复调用未 panic")
			}
		}()
		call()
	}

	p, err := New(context.Background(), testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	expectPanic(t, func() { _ = p.Shutdown() })
	expectPanic(t, func() { _ = p.Wait() })

	runCtx, stop := context.WithCancel(context.Background())
	p, err = New(runCtx, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	stop()
	if err := p.Wait(); err != nil {
		t.Fatal(err)
	}
	expectPanic(t, func() { _ = p.Wait() })
	expectPanic(t, func() { _ = p.Shutdown() })
}

func TestConcurrentTerminalMethodsOnlyOneSucceeds(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	p, err := New(runCtx, testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan bool, 2)
	call := func(fn func() error) {
		go func() {
			<-start
			defer func() { results <- recover() != nil }()
			_ = fn()
		}()
	}
	call(p.Wait)
	call(p.Shutdown)
	close(start)
	stop()
	first, second := <-results, <-results
	if first == second {
		t.Fatalf("并发终止调用的 panic 结果 = %v, %v, want exactly one", first, second)
	}
}

func TestWaitDeadlineAfterStopAbortsTasks(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(runCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error {
		t.Error("放弃的任务不应执行")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stop()
	if err := p.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("停机 Wait = %v, want deadline exceeded", err)
	}
	if got := p.Stats(); got.Abandoned != 1 {
		t.Fatalf("停机超时后 Stats = %+v, want one abandoned task", got)
	}
	waitUntil(t, func() bool { return p.Stats().Workers == 0 })
}

func TestGraceTimeoutRequiresWaitToAbort(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(runCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(ctx context.Context) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := p.Submit(context.Background(), "pending", func(context.Context) error {
		t.Error("放弃的任务不应执行")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	stop()
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.state == stateDraining
	})
	select {
	case <-p.graceExpired:
	case <-time.After(3 * time.Second):
		t.Fatal("停机宽限期未结束")
	}
	if got := p.Stats(); got.Pending != 1 || got.Running != 1 || got.Abandoned != 0 {
		t.Fatalf("未调用 Wait 时任务不应中止: %+v", got)
	}
	if err := p.ctx.Err(); err != nil {
		t.Fatalf("未调用 Wait 时执行 context = %v, want nil", err)
	}
	if err := p.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("宽限期后调用 Wait = %v, want deadline exceeded", err)
	}
	if got := p.Stats(); got.Abandoned != 1 {
		t.Fatalf("Wait 超时后 Stats = %+v, want one abandoned task", got)
	}
	waitUntil(t, func() bool { return p.Stats().Workers == 0 })
}

func TestShutdownWaitsForWorkersWithoutStopSignal(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	if err := p.Submit(context.Background(), "running", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown() }()
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.state == stateDraining
	})
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-shutdown:
		close(release)
		t.Fatalf("Shutdown 未等待 worker: %v", err)
	default:
	}
	close(release)
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestShutdownIgnoresGraceTimeout(t *testing.T) {
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(runCtx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	if err := p.Submit(context.Background(), "ignores-cancel", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown() }()
	stop()
	waitUntil(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.state == stateDraining
	})
	select {
	case <-p.graceExpired:
	case <-time.After(3 * time.Second):
		t.Fatal("停机宽限期未结束")
	}
	if got := p.Stats(); got.Running != 1 || got.Abandoned != 0 {
		t.Fatalf("Shutdown 不应因宽限期中止任务: %+v", got)
	}
	if err := p.ctx.Err(); err != nil {
		t.Fatalf("Shutdown 期间执行 context = %v, want nil", err)
	}
	select {
	case err := <-shutdown:
		t.Fatalf("宽限期结束时 Shutdown 提前返回: %v", err)
	default:
	}
	release <- struct{}{}
	select {
	case err := <-shutdown:
		if err != nil {
			t.Fatalf("Shutdown = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("worker 退出后 Shutdown 未返回")
	}
}

func TestTaskErrorsAndPanicGoToStderr(t *testing.T) {
	read, write, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStderr := os.Stderr
	os.Stderr = write
	t.Cleanup(func() {
		os.Stderr = oldStderr
		write.Close()
		read.Close()
	})
	cfg := testConfig()
	cfg.MaxRetries = 1
	p, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	if err := p.Submit(context.Background(), "fails", func(context.Context) error {
		calls.Add(1)
		return errors.New("failed")
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Submit(context.Background(), "panics", func(context.Context) error { panic("broken") }); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	write.Close()
	data, err := io.ReadAll(read)
	if err != nil {
		t.Fatal(err)
	}
	output := string(data)
	if calls.Load() != 2 || p.Stats().Failed != 2 || !strings.Contains(output, "task=\"fails\"") || !strings.Contains(output, "attempt=2/2") || !strings.Contains(output, "task=\"panics\"") || !strings.Contains(output, "goroutine") {
		t.Fatalf("calls=%d Stats=%+v stderr=%q", calls.Load(), p.Stats(), output)
	}
}
