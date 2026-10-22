package cronx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-co-op/gocron/v2"
)

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("等待状态超时")
		}
		time.Sleep(time.Millisecond)
	}
}

func runScheduler(t *testing.T, cfg Config, jobs ...Job) (*Scheduler, <-chan error) {
	t.Helper()
	s, err := New(cfg, jobs...)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- s.Run() }()
	t.Cleanup(func() {
		_ = stopAndWait(s.Stop, s.Wait, s.Cancel)
	})
	return s, result
}

func TestConfiguration(t *testing.T) {
	job := Job{
		Name:    "job",
		Every:   time.Second,
		Handler: func(context.Context) error { return nil },
	}
	for _, tc := range []struct {
		name string
		edit func(*Config, *Job)
	}{
		{"name", func(_ *Config, j *Job) { j.Name = " job" }},
		{"handler", func(_ *Config, j *Job) { j.Handler = nil }},
		{"missing-schedule", func(_ *Config, j *Job) { j.Every = 0 }},
		{"negative-every", func(_ *Config, j *Job) { j.Every = -1 }},
		{"multiple", func(_ *Config, j *Job) { j.Cron = "* * * * * *" }},
		{"five-fields", func(_ *Config, j *Job) {
			j.Every = 0
			j.Cron = "* * * * *"
		}},
		{"timezone", func(_ *Config, j *Job) {
			j.Every = 0
			j.Cron = "TZ=UTC * * * * *"
		}},
		{"impossible", func(_ *Config, j *Job) {
			j.Every = 0
			j.Cron = "0 0 0 31 2 *"
		}},
		{"random-range", func(_ *Config, j *Job) {
			j.Every = 0
			j.RandomEvery = &RandomInterval{
				Min: time.Second,
				Max: time.Second,
			}
		}},
		{"timeout", func(_ *Config, j *Job) { j.Timeout = -1 }},
		{"level", func(_ *Config, j *Job) { j.SuccessLevel = logit.WarnLevel }},
		{"locked-timeout", func(c *Config, _ *Job) {
			c.Locker = lockerFunc(func(context.Context, string) (gocron.Lock, error) { return nil, errors.New("unused") })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, j := Config{}, job
			tc.edit(&cfg, &j)
			if s, err := New(cfg, j); err == nil {
				_ = stopAndWait(s.Stop, s.Wait, s.Cancel)
				t.Fatal("应拒绝无效配置")
			}
		})
	}
	if s, err := New(Config{}, job, job); err == nil {
		_ = stopAndWait(s.Stop, s.Wait, s.Cancel)
		t.Fatal("应拒绝重复任务")
	}
}

func TestMinimumInterval(t *testing.T) {
	for _, schedule := range []string{"every", "randomEvery"} {
		for _, interval := range []time.Duration{time.Nanosecond, 100*time.Millisecond - time.Nanosecond, 100 * time.Millisecond} {
			t.Run(schedule+"/"+interval.String(), func(t *testing.T) {
				job := Job{
					Name:    "job",
					Handler: func(context.Context) error { return nil },
				}
				if schedule == "every" {
					job.Every = interval
				} else {
					job.RandomEvery = &RandomInterval{
						Min: interval,
						Max: interval + time.Nanosecond,
					}
				}
				s, err := New(Config{}, job)
				if s != nil {
					t.Cleanup(func() {
						if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
							t.Error(err)
						}
					})
				}
				if interval < 100*time.Millisecond {
					if err == nil || !strings.Contains(err.Error(), "100ms") || s != nil {
						t.Fatalf("小于 100ms 应在构造时拒绝并说明下限: schedulerNil=%v err=%v", s == nil, err)
					}
				} else if err != nil {
					t.Fatalf("恰好 100ms 应允许构造: %v", err)
				}
			})
		}
	}
}

func TestCronLocation(t *testing.T) {
	for _, tc := range []struct {
		name         string
		location     *time.Location
		wantUTCClock string
	}{
		{
			name:         "default-shanghai",
			wantUTCClock: "01:00:00",
		},
		{
			name:         "explicit-utc",
			location:     time.UTC,
			wantUTCClock: "09:00:00",
		},
		{
			name:         "unnamed-fixed-zone",
			location:     time.FixedZone("", 8*60*60),
			wantUTCClock: "01:00:00",
		},
		{
			name:         "custom-zone-name",
			location:     time.FixedZone("business-zone", 5*60*60+45*60),
			wantUTCClock: "03:15:00",
		},
		{
			name:         "name-must-not-override-offset",
			location:     time.FixedZone("UTC", -5*60*60),
			wantUTCClock: "14:00:00",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := runScheduler(t, Config{Location: tc.location}, Job{
				Name:    "daily-report",
				Cron:    "0 0 9 * * *",
				Handler: func(context.Context) error { return nil },
			})
			var next time.Time
			waitFor(t, func() bool {
				var err error
				next, err = s.engine.Jobs()[0].NextRun()
				if err != nil {
					t.Fatal(err)
				}
				return !next.IsZero()
			})
			if got := next.UTC().Format("15:04:05"); got != tc.wantUTCClock {
				t.Fatalf("每天 9 点任务的下次执行 UTC 时间 = %s，期望 %s", got, tc.wantUTCClock)
			}
		})
	}
}

func TestSchedulesAndContextIsolation(t *testing.T) {
	seen := make(chan string, 128)
	var completed [3]atomic.Int32
	handler := func(index int) Handler {
		return func(ctx context.Context) error {
			id, ok := logit.LogIDFromContext(ctx)
			if !ok {
				return errors.New("missing logId")
			}
			logit.AddField(ctx, logit.Str("round", id))
			logit.AddDownstreamDurationAuto(ctx, "test", time.Millisecond)
			seen <- id
			completed[index].Add(1)
			return nil
		}
	}
	s, result := runScheduler(t, Config{},
		Job{
			Name:    "fixed",
			Every:   100 * time.Millisecond,
			Handler: handler(0),
		},
		Job{
			Name: "random",
			RandomEvery: &RandomInterval{
				Min: 100 * time.Millisecond,
				Max: 150 * time.Millisecond,
			},
			Handler: handler(1),
		},
		Job{
			Name:    "cron",
			Cron:    "* * * * * *",
			Handler: handler(2),
		},
	)
	waitFor(t, func() bool {
		for i := range completed {
			if completed[i].Load() == 0 {
				return false
			}
		}
		return true
	})
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	close(seen)
	for id := range seen {
		if ids[id] {
			t.Fatal("每轮应生成独立 logId")
		}
		ids[id] = true
	}
}

func TestDrainAllowsCompletionAndUnlock(t *testing.T) {
	entered, finish, unlocking, unlock := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	finishTask := sync.OnceFunc(func() { close(finish) })
	finishUnlock := sync.OnceFunc(func() { close(unlock) })
	defer finishTask()
	defer finishUnlock()
	var canceled atomic.Bool
	locker := lockerFunc(func(_ context.Context, name string) (gocron.Lock, error) {
		if name != "job" {
			return nil, errors.New("unstable name")
		}
		return lockFunc(func(ctx context.Context) error {
			canceled.Store(ctx.Err() != nil)
			close(unlocking)
			<-unlock
			return nil
		}), nil
	})
	s, result := runScheduler(t, Config{
		Locker: locker,
	}, Job{
		Name:    "job",
		Every:   100 * time.Millisecond,
		Timeout: time.Second,
		Handler: func(ctx context.Context) error {
			close(entered)
			<-finish
			if ctx.Err() != nil {
				canceled.Store(true)
			}
			return nil
		},
	})
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	time.Sleep(20 * time.Millisecond)
	finishTask()
	<-unlocking
	select {
	case err := <-stopped:
		t.Fatalf("解锁尚未完成: %v", err)
	default:
	}
	finishUnlock()
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if canceled.Load() {
		t.Fatal("排空任务与解锁 context 不应被引擎取消")
	}
}

func TestLateLockDuringShutdownNeverRuns(t *testing.T) {
	acquiring, release, unlocked := make(chan struct{}), make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	var runs atomic.Int32
	locker := lockerFunc(func(context.Context, string) (gocron.Lock, error) {
		close(acquiring)
		<-release
		return lockFunc(func(ctx context.Context) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			close(unlocked)
			return nil
		}), nil
	})
	s, _ := runScheduler(t, Config{Locker: locker}, Job{
		Name:    "job",
		Every:   100 * time.Millisecond,
		Timeout: time.Second,
		Handler: func(context.Context) error {
			runs.Add(1)
			return nil
		},
	})
	<-acquiring
	result := make(chan error, 1)
	go func() { result <- stopAndWait(s.Stop, s.Wait, s.Cancel) }()
	waitFor(t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.closing
	})
	finish()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	<-unlocked
	if runs.Load() != 0 {
		t.Fatal("关闭后获锁不得进入 Handler")
	}
}

func TestErrorsPanicAndTimeoutKeepSchedulerRunning(t *testing.T) {
	records := recordLogs(t)
	var n atomic.Int32
	release := make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	s, _ := runScheduler(t, Config{}, Job{
		Name:    "job",
		Every:   100 * time.Millisecond,
		Timeout: 30 * time.Millisecond,
		Handler: func(context.Context) error {
			switch n.Add(1) {
			case 1:
				return errors.New("business failure")
			case 2:
				panic("business panic")
			case 3:
				<-release
			}
			return nil
		},
	})
	waitFor(t, func() bool { return n.Load() == 3 })
	// 跨过至少两个 100ms 调度周期，确认超时后仍未返回的任务不会重入。
	time.Sleep(250 * time.Millisecond)
	if n.Load() != 3 {
		t.Fatalf("超时但尚未返回的任务仍应阻止重入，实际次数: %d", n.Load())
	}
	finish()
	waitFor(t, func() bool { return countResults(records(), "success") > 0 })
	if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"error", "panic", "timeout"} {
		if count := countResults(records(), status); count != 1 {
			t.Fatalf("%s 结果应只出现一次，实际次数: %d", status, count)
		}
	}
	found := false
	for _, record := range records() {
		if strings.Contains(record, "CronJobResult") && strings.Contains(record, "business failure") {
			found = true
		}
	}
	if !found {
		t.Fatal("业务失败日志应保留错误原因")
	}
}

func TestConcurrentCloseBeforeRun(t *testing.T) {
	s, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := stopAndWait(s.Stop, s.Wait, s.Cancel); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := s.Run(); !errors.Is(err, ErrClosed) {
		t.Fatalf("关闭后应拒绝启动: %v", err)
	}
}

func TestRunShutdownRace(t *testing.T) {
	for range 25 {
		s, err := New(Config{})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		ran, closed := make(chan error, 1), make(chan error, 1)
		go func() {
			<-start
			ran <- s.Run()
		}()
		go func() {
			<-start
			closed <- stopAndWait(s.Stop, s.Wait, s.Cancel)
		}()
		close(start)
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("启动与关闭竞争造成关闭阻塞")
		}
		select {
		case err := <-ran:
			if err != nil && !errors.Is(err, ErrClosed) {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("关闭后 Run 未返回")
		}
	}
}

type lockerFunc func(context.Context, string) (gocron.Lock, error)

func (f lockerFunc) Lock(ctx context.Context, name string) (gocron.Lock, error) { return f(ctx, name) }

type lockFunc func(context.Context) error

func (f lockFunc) Unlock(ctx context.Context) error { return f(ctx) }

func TestExplicitCancelAfterWaitDeadline(t *testing.T) {
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	s, ran := runScheduler(t, Config{}, Job{Name: "blocked", Every: 100 * time.Millisecond, Handler: func(ctx context.Context) error { entered <- ctx; <-release; return nil }})
	taskCtx := <-entered
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	if err := <-ran; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("未按期限返回: %v", err)
	}
	if taskCtx.Err() != nil {
		t.Fatal("停收或等待提前取消了任务")
	}
	s.Cancel()
	if taskCtx.Err() == nil {
		t.Fatal("没有取消剩余任务")
	}
	close(release)
	if err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestWaitDeadlineIncludesUnlock(t *testing.T) {
	unlocking, release := make(chan struct{}), make(chan struct{})
	finish := sync.OnceFunc(func() { close(release) })
	defer finish()
	locker := lockerFunc(func(context.Context, string) (gocron.Lock, error) {
		return lockFunc(func(context.Context) error {
			close(unlocking)
			<-release
			return nil
		}), nil
	})
	s, _ := runScheduler(t, Config{Locker: locker}, Job{
		Name:    "job",
		Every:   100 * time.Millisecond,
		Timeout: time.Second,
		Handler: func(context.Context) error { return nil },
	})
	<-unlocking
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("解锁未结束，不应假报排空: %v", err)
	}
	s.Cancel()
	finish()
	if err := s.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
