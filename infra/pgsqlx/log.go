package pgsqlx

import (
	"context"
	"errors"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const downstreamPostgreSQLMessage = "PostgreSQL"

// traceLogger 为 PostgreSQL 提供 GORM Logger 入口和 SQLSTATE 字段。
type traceLogger struct {
	*gormcore.Logger
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config, endpointType, endpoint string) *traceLogger {
	return &traceLogger{Logger: gormcore.New(gormcore.Config{
		Message:        downstreamPostgreSQLMessage,
		Name:           cfg.Name,
		DurationPrefix: downstreamPostgreSQLMessage + "_" + cfg.Name,
		Endpoint: &gormcore.Endpoint{
			Type: endpointType,
			Name: endpoint,
		},
		SlowThreshold:  cfg.SlowThreshold,
		LogSQL:         cfg.LogSQL,
		InterpolateSQL: cfg.InterpolateSQL,
		ErrorDetails:   addSQLState,
	})}
}

func addSQLState(err error, details map[string]any) {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		details["sqlstate"] = pgErr.SQLState()
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
