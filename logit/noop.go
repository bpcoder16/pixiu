package logit

import "context"

// noopLogger 让仅使用其他 Pixiu 能力的应用无需配置日志。
// 不持有 Writer 或字段；Fatal 也不退出，显式配置的 Logger 保留原有语义。
type noopLogger struct{}

var _ Logger = noopLogger{}

func (noopLogger) Debug(context.Context, string, ...Field)              {}
func (noopLogger) Info(context.Context, string, ...Field)               {}
func (noopLogger) Warn(context.Context, string, ...Field)               {}
func (noopLogger) Error(context.Context, string, ...Field)              {}
func (noopLogger) Fatal(context.Context, string, ...Field)              {}
func (noopLogger) Output(context.Context, Level, int, string, ...Field) {}
func (noopLogger) Enabled(Level) bool                                   { return false }
func (l noopLogger) With(...Field) Logger                               { return l }
func (noopLogger) Close() error                                         { return nil }
