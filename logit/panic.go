package logit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"
)

var panicLoggerPtr atomic.Pointer[Logger]

// ErrPanicLoggerNotConfigured 表示尚未设置 panic 专用 Logger。
var ErrPanicLoggerNotConfigured = errors.New("logit: panic logger not configured")

// PanicLogger 返回 panic 专用 Logger；未调用 SetPanicLogger 时返回 nil。
func PanicLogger() Logger {
	if p := panicLoggerPtr.Load(); p != nil {
		return *p
	}
	return nil
}

// SetPanicLogger 设置 panic 专用 Logger(推荐指向独立 panic 文件,如
// NewRotateFile("log/panic.log"));传入实例应使用 OptNoExit 构造,
// 否则 ReportPanic 会在尝试写入和 Sync 后退出进程。
func SetPanicLogger(l Logger) { panicLoggerPtr.Store(&l) }

// ReportPanic 单行记录一次 panic:panic 值、转义为单行的 goroutine 栈、
// pid 与进程启动时间;以 Fatal 级别同步写入并尽力 Sync,
// 写入或 Sync 失败通过 WriteErrorStats 和 OptOnWriteError 观察，不承诺必然落盘。
// 未配置 panic Logger 时返回 ErrPanicLoggerNotConfigured；配 OptNoExit 时不退出进程。
// 供 recover 处理器调用,也可用更省心的 RecoverAndReport。
func ReportPanic(ctx context.Context, recovered any, fields ...Field) error {
	l := PanicLogger()
	if l == nil {
		return ErrPanicLoggerNotConfigured
	}

	stack := make([]byte, 4096)
	n := runtime.Stack(stack, false)
	stack = stack[:n]

	all := make([]Field, 0, len(fields)+4)
	all = append(all, fields...)
	all = append(all,
		Int("pid", os.Getpid()),
		Str("processStart", processStart),
		Str("panic", fmt.Sprint(recovered)),
		Str("stack", strings.ReplaceAll(string(stack), "\n", "\\n")),
	)
	l.Output(ctx, FatalLevel, 1, "panic", all...)
	return nil
}

// RecoverAndReport 是 defer 用的便捷 recover 处理器:
// 未配置 panic Logger 时重新抛出原 panic，避免静默吞掉异常。
//
//	defer logit.RecoverAndReport(ctx)
func RecoverAndReport(ctx context.Context) {
	if r := recover(); r != nil {
		if err := ReportPanic(ctx, r); err != nil {
			panic(r)
		}
	}
}

var processStart = time.Now().Format("2006-01-02 15:04:05")
