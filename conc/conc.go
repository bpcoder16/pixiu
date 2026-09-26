package conc

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidContext   = errors.New("conc: context 不能为空")
	ErrInvalidLimit     = errors.New("conc: 并发限额必须大于零")
	ErrInvalidTimeout   = errors.New("conc: 超时时间必须大于零")
	ErrTimeoutWithLimit = errors.New("conc: WithTimeout 与 WithLimit 不能同时使用")
	ErrNilTask          = errors.New("conc: 任务函数不能为空")
)

// Task 是一次调用内的同步任务；返回快照前收集到的返回值和错误会一并保留。
type Task func(context.Context) (any, error)

// Result 保存一个任务返回的值、错误和实际执行耗时。
type Result struct {
	Value any
	Err   error
	// Started 表示任务函数实际被调用；跳过的任务为 false。
	Started bool
	// Duration 仅包含已完成任务函数的执行耗时；未执行或超时未完成时为零。
	Duration time.Duration
	// TimedOut 表示整体超时返回时该任务尚未完成，结果不会再更新。
	TimedOut bool
}

// Report 保存本次调用的全部任务结果和整体耗时。
type Report struct {
	Results map[string]Result
	// Duration 包含截至返回时的任务调度、限流等待、执行和结果汇总。
	Duration time.Duration
}

// PanicError 表示任务发生 panic；堆栈来自发生 panic 的 goroutine。
type PanicError struct {
	TaskName string
	Value    any
	Stack    []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("conc: 任务 %q panic: %v", e.TaskName, e.Value)
}

type options struct {
	limit         int
	limitSet      bool
	cancelOnError bool
	timeout       time.Duration
	timeoutSet    bool
}

// Option 设置单次调用的执行选项。
type Option func(*options)

// WithLimit 限制本次调用同时执行的任务数；n 必须大于零，且不能与 WithTimeout 同时使用。
func WithLimit(n int) Option {
	return func(o *options) {
		o.limit = n
		o.limitSet = true
	}
}

// WithCancelOnError 在任务失败时取消传给任务的派生 context，不取消调用方的 context。
// 未设置 WithTimeout 时，已启动的任务仍会等待其返回。
func WithCancelOnError() Option {
	return func(o *options) { o.cancelOnError = true }
}

// WithTimeout 设置本次调用的整体超时；d 必须大于零。
// 到时返回已完成结果，未完成任务以 Result.TimedOut 标记；不能与 WithLimit 同时使用。
func WithTimeout(d time.Duration) Option {
	return func(o *options) {
		o.timeout = d
		o.timeoutSet = true
	}
}

// RunNamed 返回整体耗时、逐项结果和带任务名的聚合错误。
// 默认等待全部任务结束；设置 WithTimeout 后，到时返回已收集结果的快照。
// 整体耗时包含调度与限流等待；逐项耗时只包含已完成任务的实际执行。
// 任务启动顺序和聚合错误的文本顺序不保证。
// 调用期间不得修改传入的任务 map；默认任务失败不取消其他任务。
func RunNamed(ctx context.Context, tasks map[string]Task, opts ...Option) (report Report, err error) {
	if ctx == nil {
		return Report{}, ErrInvalidContext
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.limitSet && o.limit <= 0 {
		return Report{}, ErrInvalidLimit
	}
	if o.timeoutSet && o.timeout <= 0 {
		return Report{}, ErrInvalidTimeout
	}
	if o.limitSet && o.timeoutSet {
		return Report{}, ErrTimeoutWithLimit
	}
	start := time.Now()
	defer func() { report.Duration = time.Since(start) }()

	items := make([]namedTask, 0, len(tasks))
	for name, fn := range tasks {
		items = append(items, namedTask{name: name, fn: fn})
	}
	if len(items) == 0 {
		report.Results = make(map[string]Result)
		return report, nil
	}
	runCtx := ctx
	var deadline time.Time
	var cancelTimeout context.CancelFunc
	if o.timeoutSet {
		deadline = start.Add(o.timeout)
		runCtx, cancelTimeout = context.WithDeadline(ctx, deadline)
		defer cancelTimeout()
	}
	var cancelOnError context.CancelFunc
	if o.cancelOnError {
		runCtx, cancelOnError = context.WithCancel(runCtx)
		defer cancelOnError()
	}
	results := runTasks(runCtx, items, o.limit, deadline, cancelOnError)
	return collectResults(items, results)
}
