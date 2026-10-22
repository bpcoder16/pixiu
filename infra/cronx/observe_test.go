package cronx

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-co-op/gocron/v2"
)

type writeFunc func([]byte) (int, error)

func (f writeFunc) Write(p []byte) (int, error) { return f(p) }

func captureLogs(t *testing.T, write writeFunc, options ...logit.Option) {
	t.Helper()
	previous := logit.Default()
	options = append(options, logit.OptWriter(logit.NewWriter(write)))
	logger := logit.MustNew(options...)
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(previous)
		_ = logit.Close(logger)
	})
}

func countResults(records []string, status string) int {
	count := 0
	for _, record := range records {
		if strings.Contains(record, "CronJobResult") && strings.Contains(record, "status=["+status+"]") {
			count++
		}
	}
	return count
}

func recordLogs(t *testing.T) func() []string {
	t.Helper()
	var mu sync.Mutex
	var records []string
	captureLogs(t, func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		records = append(records, string(p))
		return len(p), nil
	})
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), records...)
	}
}

func TestOverlapSkipsWithoutLogging(t *testing.T) {
	records := recordLogs(t)
	entered, release := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	var runs, ticks atomic.Int32
	s, _ := runScheduler(t, Config{}, Job{
		Name:  "slow",
		Every: 100 * time.Millisecond,
		Handler: func(context.Context) error {
			if runs.Add(1) == 1 {
				close(entered)
			}
			<-release
			return nil
		},
	}, Job{
		Name:  "clock",
		Every: 100 * time.Millisecond,
		Handler: func(context.Context) error {
			ticks.Add(1)
			return nil
		},
	})
	<-entered
	// 让同一个引擎推进多个调度周期，慢任务应持续占用自己的执行位置。
	waitFor(t, func() bool { return ticks.Load() >= 4 })
	closed := make(chan error, 1)
	go func() { closed <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	})
	finish()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if runs.Load() != 1 {
		t.Fatalf("重叠触发不得重复执行，实际次数: %d", runs.Load())
	}
	for _, record := range records() {
		if strings.Contains(record, "CronJobSkipped") && strings.Contains(record, "overlap") {
			t.Fatalf("重叠跳过不应再输出日志: %s", record)
		}
	}
}

func TestWaitIncludesShutdownLogging(t *testing.T) {
	logging, release := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	captureLogs(t, func(p []byte) (int, error) {
		if strings.Contains(string(p), "CronStopped") {
			close(logging)
			<-release
		}
		return len(p), nil
	})
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	closed := make(chan error, 1)
	go func() { closed <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	<-logging
	waited := make(chan struct{})
	go func() {
		s.Wait(context.Background())
		close(waited)
	}()
	select {
	case <-waited:
		t.Error("Wait 不应在关闭日志输出结束前返回")
	case <-time.After(20 * time.Millisecond):
	}
	finish()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	<-waited
}

func TestTimeoutWarningAndUnlockFailure(t *testing.T) {
	records := make(chan string, 128)
	captureLogs(t, func(p []byte) (int, error) {
		records <- string(p)
		return len(p), nil
	})
	release := make(chan struct{})
	returned := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	locker := lockerFunc(func(context.Context, string) (gocron.Lock, error) {
		return lockFunc(func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if ctx.Err() != nil || !ok || time.Until(deadline) > time.Second {
				t.Error("解锁需要独立、有界、未取消的 context")
			}
			return errors.New("release failure")
		}), nil
	})
	s, _ := runScheduler(t, Config{Locker: locker}, Job{
		Name:    "timeout-job",
		Every:   100 * time.Millisecond,
		Timeout: 20 * time.Millisecond,
		Handler: func(context.Context) error {
			defer close(returned)
			<-release
			return nil
		},
	})
	for {
		select {
		case record := <-records:
			if !strings.Contains(record, "CronJobTimeout") {
				continue
			}
			if !strings.Contains(record, "timeout-job") || !strings.Contains(record, "logId") {
				t.Fatalf("超时日志缺少关联字段: %s", record)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("任务实际返回前应输出超时告警")
		}
		break
	}
	select {
	case <-returned:
		t.Fatal("超时告警不能代替实际结束")
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	})
	finish()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	close(records)
	var results []string
	unlockFailed := false
	for record := range records {
		results = append(results, record)
		if strings.Contains(record, "CronUnlockFailed") && strings.Contains(record, "release failure") {
			unlockFailed = true
		}
	}
	if countResults(results, "timeout") != 1 || countResults(results, "success") != 0 {
		t.Fatalf("超时后返回 nil 不能记为成功: %v", results)
	}
	if !unlockFailed {
		t.Fatal("解锁失败必须记录日志")
	}
}

func TestShutdownPreservesBusinessFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		result func(context.Context) error
		status string
		level  string
	}{
		{
			name: "business-error",
			result: func(context.Context) error {
				return errors.New("save checkpoint failed")
			},
			status: "error",
			level:  "ERROR",
		},
		{
			name: "cooperative-cancellation",
			result: func(ctx context.Context) error {
				return ctx.Err()
			},
			status: "canceled",
			level:  "DEBUG",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			records := recordLogs(t)
			entered := make(chan struct{})
			s, _ := runScheduler(t, Config{}, Job{
				Name:  "job",
				Every: 100 * time.Millisecond,
				Handler: func(ctx context.Context) error {
					close(entered)
					<-ctx.Done()
					return tc.result(ctx)
				},
			})
			<-entered
			_ = s.Stop()
			s.Cancel()
			if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
				t.Fatal(err)
			}
			if countResults(records(), tc.status) != 1 {
				t.Fatalf("关闭取消不应掩盖业务结果: %v", records())
			}
			for _, record := range records() {
				if strings.Contains(record, "CronJobResult") && strings.Contains(record, tc.level) {
					if tc.status == "error" && !strings.Contains(record, "save checkpoint failed") {
						t.Fatalf("业务错误应保留原因: %s", record)
					}
					return
				}
			}
			t.Fatalf("未按 %s 输出任务结果", tc.level)
		})
	}
}

func TestResultDurationExcludesTimeoutLogWait(t *testing.T) {
	warning, releaseLog := make(chan struct{}), make(chan struct{})
	finishLog := sync.OnceFunc(func() { close(releaseLog) })
	defer finishLog()
	result := make(chan []byte, 1)
	captureLogs(t, func(p []byte) (int, error) {
		if strings.Contains(string(p), "CronJobTimeout") {
			close(warning)
			<-releaseLog
		}
		if strings.Contains(string(p), "CronJobResult") {
			result <- append([]byte(nil), p...)
		}
		return len(p), nil
	}, logit.OptEncoder(logit.DefaultJSONEncoder))
	entered, returned, releaseJob := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishJob := sync.OnceFunc(func() { close(releaseJob) })
	defer finishJob()
	var started time.Time
	s, _ := runScheduler(t, Config{}, Job{
		Name:    "job",
		Every:   100 * time.Millisecond,
		Timeout: 20 * time.Millisecond,
		Handler: func(context.Context) error {
			started = time.Now()
			close(entered)
			<-releaseJob
			defer close(returned)
			return nil
		},
	})
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	})
	<-warning
	finishJob()
	<-returned
	// 日志 Writer 仍阻塞，留出调度余量，让 Handler 返回与日志收尾形成明显时间差。
	time.Sleep(80 * time.Millisecond)
	beforeLogRelease := time.Now()
	finishLog()
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	var record struct {
		Duration float64 `json:"duration"`
	}
	if err := json.Unmarshal(<-result, &record); err != nil {
		t.Fatal(err)
	}
	duration := time.Duration(record.Duration * float64(time.Millisecond))
	if duration >= beforeLogRelease.Sub(started)-20*time.Millisecond {
		t.Fatalf("执行耗时不应包含等待超时日志的时间: duration=%s wall=%s", duration, beforeLogRelease.Sub(started))
	}
}

func TestWaitDeadlineIncludesLifecycleLogging(t *testing.T) {
	logging, release := make(chan struct{}), make(chan struct{})
	captureLogs(t, func(p []byte) (int, error) {
		if strings.Contains(string(p), "CronStopped") {
			close(logging)
			<-release
		}
		return len(p), nil
	})
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Stop()
	<-logging
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("日志阻塞未受期限限制: %v", err)
	}
	close(release)
	if err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
