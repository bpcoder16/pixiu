package taskpool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
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
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 101},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: int(^uint(0) >> 1)},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, SubmitTimeout: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, IdleTimeout: -1},
		{MinWorkers: 1, MaxWorkers: 1, QueueSize: 1, MaxRetries: 1, DrainTimeout: -1},
	} {
		if pool, err := New(cfg); err == nil || pool != nil {
			t.Fatalf("New(%+v) = %v, %v, want error", cfg, pool, err)
		}
	}
}

func TestZeroRetriesExecutesFailureOnce(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 0
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	var calls atomic.Int32
	if err := p.Submit(context.Background(), "no-retry", func(context.Context) error {
		calls.Add(1)
		return errors.New("permanent failure")
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if got := p.Stats(); calls.Load() != 1 || got.Failed != 1 || got.Succeeded != 0 || got.Running != 0 {
		t.Fatalf("calls=%d Stats=%+v, want one failed attempt", calls.Load(), got)
	}
}

func TestNewAcceptsMaximumRetries(t *testing.T) {
	cfg := testConfig()
	cfg.MaxRetries = 100
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultDrainTimeout(t *testing.T) {
	p, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if p.cfg.DrainTimeout != 20*time.Second {
		t.Fatalf("DrainTimeout = %v, want 20s", p.cfg.DrainTimeout)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
}

func TestSubmitWaitsForSpaceAndHonorsContext(t *testing.T) {
	p, err := New(testConfig())
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
	p, err := New(cfg)
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
	p, err := New(testConfig())
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
	p, err := New(cfg)
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

func TestExpiredSubmitTimeoutRejectsAvailableQueue(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = time.Nanosecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	if err := p.Submit(context.Background(), "available", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("预算已过期的空队列 Submit = %v, want deadline exceeded", err)
	}
	if got := p.Stats(); got.Accepted != 0 {
		t.Fatalf("超时任务不应被接收: %+v", got)
	}
}

func TestSubmitTimeoutIncludesInitialLockWait(t *testing.T) {
	cfg := testConfig()
	cfg.SubmitTimeout = 20 * time.Millisecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	// 模拟首次获取锁超过提交预算；队列始终有空位，取得锁后仍应拒绝过期任务。
	p.mu.Lock()
	time.AfterFunc(100*time.Millisecond, p.mu.Unlock)
	if err := p.Submit(context.Background(), "lock-wait", func(context.Context) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("获取锁耗尽预算后的 Submit = %v, want deadline exceeded", err)
	}
	if got := p.Stats(); got.Accepted != 0 {
		t.Fatalf("超时任务不应被接收: %+v", got)
	}
}

func TestAcceptedTaskSurvivesSubmitContextCancellation(t *testing.T) {
	p, err := New(testConfig())
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
	p, err := New(cfg)
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
	p, err := New(cfg)
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
	p, err := New(cfg)
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
	p, err := New(testConfig())
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

func TestShutdownPreservesTaskContextWhileDraining(t *testing.T) {
	p, err := New(testConfig())
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
	shutdown := make(chan error, 1)
	go func() { shutdown <- p.Shutdown() }()
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
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
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

func TestWaitBeforeShutdownDoesNotStartDrainTimeout(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- p.Wait() }()
	time.Sleep(50 * time.Millisecond)
	select {
	case err := <-waited:
		t.Fatalf("停机前 Wait 提前返回: %v", err)
	default:
	}
	if err := p.Submit(context.Background(), "still-open", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("停机前 Submit = %v", err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := <-waited; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalMethodsCanBeRepeated(t *testing.T) {
	p, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Shutdown(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := p.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := p.Shutdown(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConcurrentShutdownAndWait(t *testing.T) {
	p, err := New(testConfig())
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 20)
	for i := range 20 {
		go func() {
			<-start
			if i%2 == 0 {
				results <- p.Wait()
			} else {
				results <- p.Shutdown()
			}
		}()
	}
	close(start)
	for range 20 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("并发关闭或等待未返回")
		}
	}
	if got := p.Stats(); got.Workers != 0 {
		t.Fatalf("关闭后仍有消费者: %+v", got)
	}
}

func TestWaitReturnsSavedTimeoutAfterWorkersExit(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		_ = p.Shutdown()
		_ = p.Wait()
	})
	if err := p.Submit(context.Background(), "running", func(ctx context.Context) error {
		close(started)
		<-release
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
	shutdownErr := p.Shutdown()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded", shutdownErr)
	}
	release <- struct{}{}
	if err := p.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("停机 Wait = %v, want deadline exceeded", err)
	}
	if got := p.Stats(); got.Abandoned != 1 {
		t.Fatalf("停机超时后 Stats = %+v, want one abandoned task", got)
	}
	if got := p.Stats(); got.Workers != 0 || got.Running != 0 {
		t.Fatalf("Wait 返回时消费者尚未退出: %+v", got)
	}
}

func TestShutdownCancelsRetryWait(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Wait()
	defer p.Shutdown()
	var calls atomic.Int32
	attempted := make(chan struct{}, 2)
	if err := p.Submit(context.Background(), "retry", func(context.Context) error {
		calls.Add(1)
		attempted <- struct{}{}
		return errors.New("temporary")
	}); err != nil {
		t.Fatal(err)
	}
	<-attempted
	shutdownErr := p.Shutdown()
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("Shutdown = %v, want deadline exceeded", shutdownErr)
	}
	if err := p.Wait(); err != shutdownErr {
		t.Fatalf("Wait = %v, want saved error %v", err, shutdownErr)
	}
	if got := p.Stats(); calls.Load() != 1 || got.Failed != 1 || got.Workers != 0 {
		t.Fatalf("取消重试后 calls=%d Stats=%+v", calls.Load(), got)
	}
}

func TestShutdownAbandonsPendingWithoutCancelingRunningTask(t *testing.T) {
	cfg := testConfig()
	cfg.DrainTimeout = 20 * time.Millisecond
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan context.Context, 1)
	release := make(chan struct{})
	t.Cleanup(func() {
		close(release)
		_ = p.Shutdown()
		_ = p.Wait()
	})
	var runningCalls atomic.Int32
	if err := p.Submit(context.Background(), "running", func(ctx context.Context) error {
		runningCalls.Add(1)
		started <- ctx
		<-release
		return errors.New("task failed after pool timeout")
	}); err != nil {
		t.Fatal(err)
	}
	taskCtx := <-started
	var pendingCalls atomic.Int32
	if err := p.Submit(context.Background(), "pending", func(context.Context) error {
		pendingCalls.Add(1)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	shutdownErr := p.Shutdown()
	if p.ctx.Err() != context.Canceled || taskCtx.Done() != nil || taskCtx.Err() != nil || context.Cause(taskCtx) != nil {
		t.Fatal("池控制 context 与任务 context 的取消边界错误")
	}
	if !errors.Is(shutdownErr, context.DeadlineExceeded) || !strings.Contains(shutdownErr.Error(), "unfinished=2") {
		t.Fatalf("Shutdown = %v, want timeout with two unfinished tasks", shutdownErr)
	}
	release <- struct{}{}
	// 不调用 Wait，回调结束后 worker 也应退出，且不能发起新的重试。
	waitUntil(t, func() bool { return p.Stats().Workers == 0 })
	if got := p.Stats(); runningCalls.Load() != 1 || pendingCalls.Load() != 0 || got.Abandoned != 1 || got.Failed != 1 || got.Workers != 0 {
		t.Fatalf("自动中止后 calls=%d Stats=%+v", pendingCalls.Load(), got)
	}
}

func TestShutdownTimeoutThenWaitForCleanup(t *testing.T) {
	for _, waitBeforeShutdown := range []bool{false, true} {
		t.Run(fmt.Sprintf("waitBeforeShutdown=%t", waitBeforeShutdown), func(t *testing.T) {
			cfg := testConfig()
			cfg.DrainTimeout = 20 * time.Millisecond
			p, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer func() {
				releaseOnce.Do(func() { close(release) })
				_ = p.Shutdown()
				_ = p.Wait()
			}()
			if err := p.Submit(context.Background(), "cleanup", func(ctx context.Context) error {
				close(started)
				// 池超时不会取消回调，Wait 必须等待业务实际结束。
				<-release
				return errors.New("cleanup failed")
			}); err != nil {
				t.Fatal(err)
			}
			<-started
			waited := make(chan error, 1)
			if waitBeforeShutdown {
				go func() { waited <- p.Wait() }()
			}
			shutdown := make(chan error, 1)
			go func() { shutdown <- p.Shutdown() }()
			var shutdownErr error
			select {
			case shutdownErr = <-shutdown:
				if !errors.Is(shutdownErr, context.DeadlineExceeded) {
					t.Fatalf("Shutdown = %v, want deadline exceeded", shutdownErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Shutdown 未按宽限时间返回")
			}
			if !waitBeforeShutdown {
				go func() { waited <- p.Wait() }()
			}
			select {
			case err := <-waited:
				t.Fatalf("任务仍在清理时 Wait 返回: %v", err)
			case <-time.After(20 * time.Millisecond):
			}
			// 多个观察者共用同一次超时结果，不重新启动排空或倒计时。
			results := make(chan error, 8)
			for range 8 {
				go func() { results <- p.Shutdown() }()
			}
			for range 8 {
				select {
				case err := <-results:
					if err != shutdownErr {
						t.Fatalf("重复 Shutdown = %v, want saved error %v", err, shutdownErr)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("超时后重复 Shutdown 未返回")
				}
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-waited:
				if err != shutdownErr {
					t.Fatalf("Wait = %v, want saved error %v", err, shutdownErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("清理结束后 Wait 未返回")
			}
			if got := p.Stats(); got.Workers != 0 || got.Running != 0 || got.Failed != 1 {
				t.Fatalf("Wait 后 Stats = %+v", got)
			}
			if err := p.Wait(); err != shutdownErr {
				t.Fatalf("再次 Wait = %v, want saved error %v", err, shutdownErr)
			}
			if err := p.Shutdown(); err != shutdownErr {
				t.Fatalf("退出后 Shutdown = %v, want saved error %v", err, shutdownErr)
			}
		})
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
	p, err := New(cfg)
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
