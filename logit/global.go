package logit

import (
	"context"
	"sync/atomic"
)

var defaultLogger atomic.Pointer[Logger]

func init() {
	var l = MustNew(OptWriter(Stdout()))
	defaultLogger.Store(&l)
}

// Default 返回当前默认 Logger。
func Default() Logger { return *defaultLogger.Load() }

// SetDefault 替换默认 Logger(应用启动期调用一次)，nil Logger 会 panic。
func SetDefault(l Logger) {
	if isNilInterface(l) {
		panic("logit: nil logger")
	}
	defaultLogger.Store(&l)
}

// Swap 原子替换默认 Logger 并返回旧值；nil Logger 会 panic。测试捕获输出用:
//
//	old := logit.Swap(logit.MustNew(logit.OptWriter(buf)))
//	defer logit.Swap(old)
func Swap(l Logger) (old Logger) {
	if isNilInterface(l) {
		panic("logit: nil logger")
	}
	previous := defaultLogger.Swap(&l)
	return *previous
}

func Debug(ctx context.Context, msg string, fields ...Field) {
	(*defaultLogger.Load()).Output(ctx, DebugLevel, 1, msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...Field) {
	(*defaultLogger.Load()).Output(ctx, InfoLevel, 1, msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...Field) {
	(*defaultLogger.Load()).Output(ctx, WarnLevel, 1, msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...Field) {
	(*defaultLogger.Load()).Output(ctx, ErrorLevel, 1, msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...Field) {
	(*defaultLogger.Load()).Output(ctx, FatalLevel, 1, msg, fields...)
}

// DebugEnabled 报告默认 Logger 的 Debug 是否输出,超热路径守卫用:
//
//	if logit.DebugEnabled() { logit.Debug(ctx, "detail", logit.Defer(...)) }
func DebugEnabled() bool { return Default().Enabled(DebugLevel) }
func InfoEnabled() bool  { return Default().Enabled(InfoLevel) }
func WarnEnabled() bool  { return Default().Enabled(WarnLevel) }
func ErrorEnabled() bool { return Default().Enabled(ErrorLevel) }

// With 返回基于默认 Logger 预埋固定字段的子 Logger(如模块名)。
func With(fields ...Field) Logger { return Default().With(fields...) }
