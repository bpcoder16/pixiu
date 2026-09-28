package sqlitex

import (
	"context"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// traceLogger 为 SQLite 提供 GORM Logger 入口，不附加端点标识。
type traceLogger struct {
	*gormcore.Logger
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config) *traceLogger {
	return &traceLogger{Logger: gormcore.New(gormcore.Config{
		Message:        "SQLite",
		Name:           cfg.Name,
		DurationPrefix: "sqlite",
		SlowThreshold:  cfg.SlowThreshold,
		LogSQL:         cfg.LogSQL,
		InterpolateSQL: cfg.InterpolateSQL,
	})}
}

func (l *traceLogger) LogMode(level logger.LogLevel) logger.Interface {
	clone := *l
	clone.Logger = l.Logger.WithLevel(level)
	return &clone
}

func (l *traceLogger) Info(ctx context.Context, format string, args ...any) {
	l.Logger.Diagnostic(ctx, logger.Info, logit.InfoLevel, format, args...)
}

func (l *traceLogger) Warn(ctx context.Context, format string, args ...any) {
	l.Logger.Diagnostic(ctx, logger.Warn, logit.WarnLevel, format, args...)
}

func (l *traceLogger) Error(ctx context.Context, format string, args ...any) {
	l.Logger.Diagnostic(ctx, logger.Error, logit.ErrorLevel, format, args...)
}

func (l *traceLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	l.Logger.LogTrace(ctx, begin, fc, err)
}
