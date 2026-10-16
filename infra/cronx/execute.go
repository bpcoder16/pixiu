package cronx

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

func (s *Scheduler) execute(job Job) {
	// 即使已经获锁，关闭与函数进入之间仍有竞争，必须再次经过接纳门。
	if !s.admit() {
		return
	}
	defer s.work.Done()
	ctx := logit.NewDurationScope(logit.NewContextScope(s.ctx))
	logit.AddMeta(ctx, logit.Str(logit.LogId, logit.NewLogID()))
	logit.AddField(ctx, logit.Str("job_name", job.Name))
	var cancel context.CancelFunc
	if job.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, job.Timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	started := time.Now()
	// 超时立即告警，但等待回调结束后才输出最终结果；收尾等待告警本身，避免日志关闭后迟到写入。
	warned := make(chan struct{})
	stopWarning := context.AfterFunc(ctx, func() {
		defer close(warned)
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			logit.Warn(ctx, "CronJobTimeout", logit.Dur("duration", time.Since(started)))
		}
	})
	err, panicked, stack := invoke(ctx, job.Handler)
	// 结果以业务实际返回时为准，后续日志等待不增加执行耗时，也不改变取消归类。
	ended, ctxErr := time.Now(), ctx.Err()
	if !stopWarning() {
		<-warned
	}
	status, level := "success", job.SuccessLevel
	switch {
	case panicked:
		status, level = "panic", logit.ErrorLevel
	case errors.Is(ctxErr, context.DeadlineExceeded):
		status, level, err = "timeout", logit.WarnLevel, errors.Join(err, ctxErr)
	case ctxErr != nil && (err == nil || errors.Is(err, ctxErr)):
		status, level, err = "canceled", logit.DebugLevel, errors.Join(err, ctxErr)
	case err != nil:
		status, level = "error", logit.ErrorLevel
	}
	fields := []logit.Field{logit.Str("status", status), logit.Dur("duration", ended.Sub(started))}
	if err != nil {
		fields = append(fields, logit.Err(err))
	}
	if stack != "" {
		fields = append(fields, logit.Str("stack", stack))
	}
	logit.Output(ctx, level, 0, "CronJobResult", fields...)
}

func invoke(ctx context.Context, handler Handler) (err error, panicked bool, stack string) {
	defer func() {
		if value := recover(); value != nil {
			err, panicked, stack = fmt.Errorf("panic: %v", value), true, string(debug.Stack())
		}
	}()
	err = handler(ctx)
	return
}
