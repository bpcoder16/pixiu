package sqlitex

import (
	"context"
	"errors"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/mattn/go-sqlite3"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const downstreamSQLiteMessage = "SQLite"

// traceLogger 为 SQLite 提供 GORM Logger 入口，不附加端点标识。
type traceLogger struct {
	*gormcore.Logger
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config) *traceLogger {
	return &traceLogger{Logger: gormcore.New(gormcore.Config{
		Message:        downstreamSQLiteMessage,
		Name:           cfg.Name,
		DurationPrefix: downstreamSQLiteMessage + "_" + cfg.Name,
		SlowThreshold:  cfg.SlowThreshold,
		LogSQL:         cfg.LogSQL,
		InterpolateSQL: cfg.InterpolateSQL,
		ErrorDetails:   addSQLiteErrorDetails,
	})}
}

func addSQLiteErrorDetails(err error, details map[string]any) {
	if sqliteErr, ok := errors.AsType[sqlite3.Error](err); ok {
		details["sqlite_code"] = sqliteErr.Code
		details["sqlite_extended_code"] = sqliteErr.ExtendedCode
	}
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
