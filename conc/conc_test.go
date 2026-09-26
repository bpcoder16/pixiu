package conc

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunNamedWaitsAndKeepsPartialResults(t *testing.T) {
	failed := errors.New("查询失败")
	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	var report Report
	var err error
	go func() {
		report, err = RunNamed(context.Background(), map[string]Task{
			"first": func(context.Context) (any, error) {
				<-started
				return "部分结果", failed
			},
			"second": func(ctx context.Context) (any, error) {
				close(started)
				<-release
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return 2, nil
			},
		})
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("并发任务未启动")
	}
	select {
	case <-done:
		t.Fatal("未等待第二个任务")
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunNamed 未返回")
	}
	results := report.Results
	if !errors.Is(err, failed) || !strings.Contains(err.Error(), "first") {
		t.Fatalf("聚合错误 = %v，缺少任务名或原始错误", err)
	}
	if results["first"].Value != "部分结果" || !errors.Is(results["first"].Err, failed) {
		t.Fatalf("first 结果 = %+v", results["first"])
	}
	if results["second"].Value != 2 || results["second"].Err != nil {
		t.Fatalf("second 结果 = %+v", results["second"])
	}
	if !results["first"].Started || !results["second"].Started || results["first"].Duration <= 0 || results["second"].Duration <= 0 {
		t.Fatalf("已执行任务缺少耗时 = %v", results)
	}
}

func TestRunNamedSeparatesTotalAndTaskDuration(t *testing.T) {
	taskErr := errors.New("部分失败")
	started := make(chan string, 1)
	release := make(chan struct{})
	var first atomic.Bool
	task := func(name string) Task {
		return func(context.Context) (any, error) {
			if first.CompareAndSwap(false, true) {
				started <- name
				<-release
				return "部分结果", taskErr
			}
			return 2, nil
		}
	}
	type outcome struct {
		report Report
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		report, err := RunNamed(context.Background(), map[string]Task{
			"a": task("a"),
			"b": task("b"),
		}, WithLimit(1))
		done <- outcome{report: report, err: err}
	}()
	var firstName string
	select {
	case firstName = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("首个任务未启动")
	}
	waitStart := time.Now()
	time.Sleep(20 * time.Millisecond)
	waited := time.Since(waitStart)
	close(release)
	var got outcome
	select {
	case got = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunNamed 未返回")
	}
	secondName := "a"
	if firstName == "a" {
		secondName = "b"
	}
	firstResult, secondResult := got.report.Results[firstName], got.report.Results[secondName]
	if !errors.Is(got.err, taskErr) || firstResult.Value != "部分结果" || secondResult.Value != 2 {
		t.Fatalf("报告结果 = %+v, %v", got.report, got.err)
	}
	if !firstResult.Started || !secondResult.Started || firstResult.Duration < waited || secondResult.Duration <= 0 || got.report.Duration < secondResult.Duration+waited {
		t.Fatalf("计时边界错误：总耗时=%s, 首个=%+v, 后续=%+v, 等待=%s", got.report.Duration, firstResult, secondResult, waited)
	}
}

func TestRunNamedLimitsActiveTasks(t *testing.T) {
	started := make(chan string, 3)
	release := make(chan struct{})
	fn := func(name string) Task {
		return func(context.Context) (any, error) {
			started <- name
			<-release
			return name, nil
		}
	}
	done := make(chan struct{})
	var report Report
	var err error
	go func() {
		report, err = RunNamed(context.Background(), map[string]Task{
			"a": fn("a"), "b": fn("b"), "c": fn("c"),
		}, WithLimit(2))
		close(done)
	}()
	for range 2 {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("前两个任务未启动")
		}
	}
	select {
	case name := <-started:
		t.Fatalf("超出限额启动任务 %q", name)
	default:
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RunNamed 未返回")
	}
	results := report.Results
	if err != nil || len(results) != 3 {
		t.Fatalf("RunNamed = %v, %v", results, err)
	}
}

func TestRunNamedSkipsPendingTasksAfterCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan string, 1)
	var first atomic.Bool
	var pendingCalled atomic.Bool
	task := func(name string) Task {
		return func(taskCtx context.Context) (any, error) {
			if first.CompareAndSwap(false, true) {
				started <- name
				<-taskCtx.Done()
				return nil, taskCtx.Err()
			}
			pendingCalled.Store(true)
			return nil, nil
		}
	}
	done := make(chan struct{})
	var report Report
	var err error
	go func() {
		report, err = RunNamed(ctx, map[string]Task{
			"a": task("a"),
			"b": task("b"),
		}, WithLimit(1))
		close(done)
	}()
	var firstName string
	select {
	case firstName = <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("首个任务未启动")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("取消后 RunNamed 未返回")
	}
	results := report.Results
	if pendingCalled.Load() {
		t.Fatal("取消后仍执行等待中的任务")
	}
	secondName := "a"
	if firstName == "a" {
		secondName = "b"
	}
	firstResult, secondResult := results[firstName], results[secondName]
	if len(results) != 2 || !errors.Is(firstResult.Err, context.Canceled) || !errors.Is(secondResult.Err, context.Canceled) || !errors.Is(err, context.Canceled) {
		t.Fatalf("取消结果 = %v, %v", results, err)
	}
	if !firstResult.Started || firstResult.Duration <= 0 || secondResult.Started || secondResult.Duration != 0 {
		t.Fatalf("取消后的任务耗时 = %v", results)
	}
}

func TestRunNamedCancelOnErrorModes(t *testing.T) {
	for _, tc := range []struct {
		name        string
		opts        []Option
		wantCalls   int32
		wantSkipped int
	}{
		{name: "默认继续", opts: []Option{WithLimit(1)}, wantCalls: 2},
		{name: "失败取消", opts: []Option{WithLimit(1), WithCancelOnError()}, wantCalls: 1, wantSkipped: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.New("任务失败")
			var calls atomic.Int32
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			task := func(context.Context) (any, error) {
				if calls.Add(1) == 1 {
					return "部分结果", failure
				}
				return "成功", nil
			}
			report, err := RunNamed(ctx, map[string]Task{"a": task, "b": task}, tc.opts...)
			if calls.Load() != tc.wantCalls || !errors.Is(err, failure) || len(report.Results) != 2 || ctx.Err() != nil {
				t.Fatalf("调用数=%d, 报告=%+v, 错误=%v", calls.Load(), report, err)
			}
			var failed, succeeded, skipped int
			for _, result := range report.Results {
				switch {
				case errors.Is(result.Err, failure):
					if !result.Started || result.Value != "部分结果" {
						t.Fatalf("失败任务结果=%+v", result)
					}
					failed++
				case errors.Is(result.Err, context.Canceled):
					if result.Started || result.Duration != 0 {
						t.Fatalf("跳过任务结果=%+v", result)
					}
					skipped++
				case result.Err == nil:
					if !result.Started || result.Value != "成功" {
						t.Fatalf("成功任务结果=%+v", result)
					}
					succeeded++
				default:
					t.Fatalf("意外的任务结果=%+v", result)
				}
			}
			if failed != 1 || skipped != tc.wantSkipped || succeeded != 1-tc.wantSkipped {
				t.Fatalf("失败=%d, 成功=%d, 跳过=%d", failed, succeeded, skipped)
			}
		})
	}
}

func TestRunNamedCancelOnErrorSignalsRunningTasksAndWaits(t *testing.T) {
	failure := errors.New("任务失败")
	otherStarted := make(chan struct{})
	failNow := make(chan struct{})
	cancelSeen := make(chan struct{})
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	type outcome struct {
		report Report
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		report, err := RunNamed(context.Background(), map[string]Task{
			"failed": func(context.Context) (any, error) {
				<-failNow
				return "部分结果", failure
			},
			"other": func(ctx context.Context) (any, error) {
				close(otherStarted)
				<-ctx.Done()
				close(cancelSeen)
				<-release
				return nil, ctx.Err()
			},
		}, WithCancelOnError())
		done <- outcome{report: report, err: err}
	}()
	select {
	case <-otherStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("另一个任务未启动")
	}
	close(failNow)
	select {
	case <-cancelSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("失败后未通知运行中的任务取消")
	}
	select {
	case <-done:
		t.Fatal("运行中的任务尚未返回，RunNamed 已提前返回")
	default:
	}
	close(release)
	select {
	case got := <-done:
		if !errors.Is(got.err, failure) || !errors.Is(got.err, context.Canceled) || got.report.Results["failed"].Value != "部分结果" || !got.report.Results["other"].Started || !errors.Is(got.report.Results["other"].Err, context.Canceled) {
			t.Fatalf("取消后的报告=%+v, 错误=%v", got.report, got.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("运行中的任务结束后 RunNamed 未返回")
	}
}

func TestRunNamedCancelOnErrorHandlesPanicAndNilTask(t *testing.T) {
	for _, tc := range []struct {
		name    string
		badTask Task
		wantErr error
	}{
		{name: "panic", badTask: func(context.Context) (any, error) { panic("boom") }},
		{name: "nil", badTask: nil, wantErr: ErrNilTask},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			report, err := RunNamed(ctx, map[string]Task{
				"bad": tc.badTask,
				"other": func(ctx context.Context) (any, error) {
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}, WithCancelOnError())
			if !errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || !errors.Is(report.Results["other"].Err, context.Canceled) || len(report.Results) != 2 {
				t.Fatalf("取消报告=%+v, 错误=%v", report, err)
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || !errors.Is(report.Results["bad"].Err, tc.wantErr) {
					t.Fatalf("失败任务结果=%+v, 错误=%v", report.Results["bad"], err)
				}
			} else {
				var panicErr *PanicError
				if !errors.As(err, &panicErr) || panicErr.TaskName != "bad" {
					t.Fatalf("panic 错误=%v", err)
				}
			}
		})
	}
}

func TestRunNamedRecoversPanic(t *testing.T) {
	report, err := RunNamed(context.Background(), map[string]Task{
		"panic": func(context.Context) (any, error) { panic("boom") },
		"ok":    func(context.Context) (any, error) { return "ok", nil },
	})
	results := report.Results
	var panicErr *PanicError
	if !errors.As(err, &panicErr) || panicErr.TaskName != "panic" || panicErr.Value != "boom" || len(panicErr.Stack) == 0 {
		t.Fatalf("panic 错误 = %v", err)
	}
	if !errors.As(results["panic"].Err, &panicErr) || results["ok"].Value != "ok" {
		t.Fatalf("panic 后的结果 = %v", results)
	}
	if !results["panic"].Started || results["panic"].Duration <= 0 {
		t.Fatalf("panic 任务缺少执行耗时: %+v", results["panic"])
	}
}

func TestRunNamedJoinsErrors(t *testing.T) {
	first := errors.New("first error")
	second := errors.New("second error")
	report, err := RunNamed(context.Background(), map[string]Task{
		"b": func(context.Context) (any, error) { return nil, second },
		"a": func(context.Context) (any, error) { return nil, first },
	})
	results := report.Results
	if !errors.Is(err, first) || !errors.Is(err, second) || len(results) != 2 {
		t.Fatalf("聚合结果 = %v, %v", results, err)
	}
	message := err.Error()
	if !strings.Contains(message, `任务 "a"`) || !strings.Contains(message, `任务 "b"`) {
		t.Fatalf("聚合错误缺少任务名: %s", message)
	}
}

func TestRunNamedRejectsInvalidInput(t *testing.T) {
	invalid, err := RunNamed(nil, nil)
	if !errors.Is(err, ErrInvalidContext) || invalid.Results != nil || invalid.Duration != 0 {
		t.Fatalf("nil context 报告 = %+v, %v", invalid, err)
	}
	if _, err := RunNamed(context.Background(), nil, WithLimit(0)); !errors.Is(err, ErrInvalidLimit) {
		t.Fatalf("无效限额错误 = %v", err)
	}
	report, err := RunNamed(context.Background(), map[string]Task{"nil": nil})
	results := report.Results
	if len(results) != 1 || !errors.Is(err, ErrNilTask) || !errors.Is(results["nil"].Err, ErrNilTask) {
		t.Fatalf("nil 任务结果 = %v, %v", results, err)
	}
	if results["nil"].Started || results["nil"].Duration != 0 {
		t.Fatalf("nil 任务不应有执行耗时: %+v", results["nil"])
	}
	empty, err := RunNamed(context.Background(), nil)
	if err != nil || empty.Results == nil || len(empty.Results) != 0 {
		t.Fatalf("空任务结果 = %v, %v", empty, err)
	}
}
