package logit

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

var panicLoggerPtr atomic.Pointer[Logger]

// ErrPanicLoggerNotConfigured 表示尚未设置 panic 专用 Logger。
var ErrPanicLoggerNotConfigured = errors.New("logit: panic logger not configured")

// panicTextEncoder 复用文本编码器，仅让内部堆栈字段保留物理换行。
// 其他业务字段继续转义，避免同名自定义字段注入额外日志行。
type panicTextEncoder struct{ TextEncoder }

func (panicTextEncoder) AppendField(buf []byte, f Field) []byte {
	if f.typ != panicStackType {
		return DefaultTextEncoder.AppendField(buf, f)
	}
	buf = appendTextString(buf, f.Key)
	buf = append(buf, "=[\n"...)
	buf = append(buf, f.str...)
	return append(buf, "] "...)
}

// PanicLogger 返回 panic 专用 Logger；未调用 SetPanicLogger 时返回 nil。
func PanicLogger() Logger {
	if p := panicLoggerPtr.Load(); p != nil {
		return *p
	}
	return nil
}

// SetPanicLogger 从 Writer 构造 panic 专用 Logger(推荐使用独立 panic 文件,如
// NewRotateFile("/var/log/app/panic.log"))。只路由 Fatal，固定文本编码、无 caller，
// 写入并尽力 Sync 后继续运行；无效 Writer 在此处 panic。
func SetPanicLogger(w Writer) {
	if isNilInterface(w) {
		panic("logit: nil panic writer")
	}
	l := MustNew(
		OptDispatch(Target{Levels: []Level{FatalLevel}, Writer: w}),
		OptEncoder(panicTextEncoder{}),
		OptCaller(false),
		OptNoExit(),
	)
	panicLoggerPtr.Store(&l)
}

// ReportPanic 记录一次 panic:panic 值、保留物理换行的 goroutine 栈、
// pid 与进程启动时间;以 Fatal 级别同步写入并尽力 Sync,
// 写入或 Sync 失败通过 PanicLogger 的 WriteErrorStats 观察，不承诺必然落盘。
// 未配置 panic Logger 时返回 ErrPanicLoggerNotConfigured；记录后不退出进程。
// 供 recover 处理器调用,也可用更省心的 RecoverAndReport。
func ReportPanic(ctx context.Context, recovered any, fields ...Field) error {
	l := PanicLogger()
	if l == nil {
		return ErrPanicLoggerNotConfigured
	}

	stack := make([]byte, 4096)
	for {
		n := runtime.Stack(stack, false)
		if n < len(stack) {
			stack = stack[:n]
			break
		}
		stack = make([]byte, len(stack)*2)
	}

	all := make([]Field, 0, len(fields)+4)
	all = append(all, fields...)
	all = append(all,
		Int("pid", os.Getpid()),
		Str("processStart", processStart),
		Str("panic", fmt.Sprint(recovered)),
		Field{Key: "stack", typ: panicStackType, str: string(stack)},
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
