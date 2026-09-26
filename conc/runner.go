package conc

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

type namedTask struct {
	name string
	fn   Task
}

type taskJob struct {
	index int
	item  namedTask
}

type taskState struct {
	result   Result
	finished bool
}

type runState struct {
	sync.Mutex
	tasks    []taskState
	deadline time.Time
	closed   bool
}

func (s *runState) start(index int) bool {
	s.Lock()
	defer s.Unlock()
	if s.closed || !time.Now().Before(s.deadline) {
		return false
	}
	s.tasks[index].result.Started = true
	return true
}

func (s *runState) finish(index int, result Result) {
	if s.deadline.IsZero() {
		// 普通模式等待全部 worker 退出，每个任务只写自己的槽位。
		s.tasks[index].result = result
		return
	}
	s.Lock()
	if !s.closed {
		s.tasks[index].result = result
		s.tasks[index].finished = true
	}
	s.Unlock()
}

func (s *runState) snapshot() []Result {
	if s.deadline.IsZero() {
		results := make([]Result, len(s.tasks))
		for index, task := range s.tasks {
			results[index] = task.result
		}
		return results
	}
	s.Lock()
	defer s.Unlock()
	results := make([]Result, len(s.tasks))
	for index, task := range s.tasks {
		if !task.finished {
			results[index] = Result{Err: context.DeadlineExceeded, Started: task.result.Started, TimedOut: true}
			continue
		}
		results[index] = task.result
	}
	// 超时后尚未返回的任务不会修改快照，也不应让其值留在后台 worker 中。
	s.closed = true
	s.tasks = nil
	return results
}

func runTask(ctx context.Context, job taskJob, cancelOnError context.CancelFunc, state *runState) {
	var result Result
	switch {
	case job.item.fn == nil:
		result.Err = ErrNilTask
	case ctx.Err() != nil:
		// 取消后保留任务名，但不调用尚未开始的函数。
		result.Err = ctx.Err()
	case !state.deadline.IsZero() && !state.start(job.index):
		result.Err = context.DeadlineExceeded
	default:
		result = callTask(ctx, job.item)
	}
	if result.Err != nil && cancelOnError != nil {
		cancelOnError()
	}
	state.finish(job.index, result)
}

func runTasks(ctx context.Context, items []namedTask, limit int, deadline time.Time, cancelOnError context.CancelFunc) []Result {
	state := &runState{tasks: make([]taskState, len(items)), deadline: deadline}
	jobs := make(chan taskJob)
	workers := len(items)
	if limit > 0 && limit < workers {
		workers = limit
	}
	var wg sync.WaitGroup
	var done chan struct{}
	if deadline.IsZero() {
		wg.Add(workers)
	} else {
		// 超时返回后 worker 仍可能结束；完成信号不能让它阻塞。
		done = make(chan struct{}, len(items))
	}
	for range workers {
		go func() {
			if deadline.IsZero() {
				defer wg.Done()
			}
			for job := range jobs {
				runTask(ctx, job, cancelOnError, state)
				if done != nil {
					done <- struct{}{}
				}
			}
		}()
	}

	var deadlineCh <-chan time.Time
	if !deadline.IsZero() {
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		deadlineCh = timer.C
	}

	expired := false
	dispatched := 0
	for index, item := range items {
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			expired = true
			break
		}
		select {
		case jobs <- taskJob{index: index, item: item}:
			dispatched++
		case <-deadlineCh:
			expired = true
		}
		if expired {
			break
		}
	}
	close(jobs)
	if deadline.IsZero() {
		wg.Wait()
		return state.snapshot()
	}
	for completed := 0; completed < dispatched && !expired; {
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-done:
			completed++
		case <-deadlineCh:
			expired = true
		}
	}
	return state.snapshot()
}

func collectResults(items []namedTask, results []Result) (Report, error) {
	report := Report{Results: make(map[string]Result, len(items))}
	var taskErrors []error
	timedOut := false
	for index, item := range items {
		result := results[index]
		report.Results[item.name] = result
		if result.TimedOut {
			timedOut = true
		} else if result.Err != nil {
			taskErrors = append(taskErrors, fmt.Errorf("conc: 任务 %q: %w", item.name, result.Err))
		}
	}
	if timedOut {
		taskErrors = append(taskErrors, context.DeadlineExceeded)
	}
	return report, errors.Join(taskErrors...)
}

func callTask(ctx context.Context, item namedTask) (result Result) {
	result.Started = true
	start := time.Now()
	defer func() {
		recovered := recover()
		result.Duration = time.Since(start)
		if recovered != nil {
			result.Err = &PanicError{
				TaskName: item.name,
				Value:    recovered,
				Stack:    debug.Stack(),
			}
		}
	}()
	result.Value, result.Err = item.fn(ctx)
	return result
}
