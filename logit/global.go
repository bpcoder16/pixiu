package logit

import (
	"context"
	"sync/atomic"
)

var defaultLogger atomic.Pointer[Logger]

type loggerNameKey struct{}

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

// WithLoggerName 在 ctx 中设置命名 Logger 的路由键；空名字表示使用默认 Logger。
// 此路由不要求预先调用 WithContext，也不会写入日志字段。
func WithLoggerName(ctx context.Context, name string) context.Context {
	return context.WithValue(ctx, loggerNameKey{}, name)
}

// LoggerFromContext 返回 ctx 指定的命名 Logger；无名字或未注册时返回默认 Logger。
func LoggerFromContext(ctx context.Context) Logger {
	if ctx != nil {
		if name, ok := ctx.Value(loggerNameKey{}).(string); ok && name != "" {
			return Named(name)
		}
	}
	return Default()
}

func Debug(ctx context.Context, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, DebugLevel, 1, msg, fields...)
}

func Info(ctx context.Context, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, InfoLevel, 1, msg, fields...)
}

func Warn(ctx context.Context, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, WarnLevel, 1, msg, fields...)
}

func Error(ctx context.Context, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, ErrorLevel, 1, msg, fields...)
}

func Fatal(ctx context.Context, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, FatalLevel, 1, msg, fields...)
}

// Output 按 ctx 选择 Logger 并输出；callDepth 为 0 时 caller 指向直接调用者。
func Output(ctx context.Context, level Level, callDepth int, msg string, fields ...Field) {
	LoggerFromContext(ctx).Output(ctx, level, callDepth+1, msg, fields...)
}

// DebugEnabled 报告 ctx 选中的 Logger 是否输出 Debug,超热路径守卫用:
//
//	if logit.DebugEnabled(ctx) { logit.Debug(ctx, "detail", logit.Defer(...)) }
//
// ctx 未指定命名 Logger 时检查默认 Logger。
func DebugEnabled(ctx context.Context) bool { return LoggerFromContext(ctx).Enabled(DebugLevel) }
func InfoEnabled(ctx context.Context) bool  { return LoggerFromContext(ctx).Enabled(InfoLevel) }
func WarnEnabled(ctx context.Context) bool  { return LoggerFromContext(ctx).Enabled(WarnLevel) }
func ErrorEnabled(ctx context.Context) bool { return LoggerFromContext(ctx).Enabled(ErrorLevel) }

// With 返回基于默认 Logger 预埋固定字段的子 Logger(如模块名)。
func With(fields ...Field) Logger { return Default().With(fields...) }
