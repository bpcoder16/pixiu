package mysqlx

import (
	"context"
	"errors"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	mysqldriver "github.com/go-sql-driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const downstreamMySQLMessage = "MySQL"

// traceLogger 为 MySQL 提供 GORM Logger 入口和主从端点标识。
type traceLogger struct {
	*gormcore.Logger
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config, endpointType, endpoint string) *traceLogger {
	return &traceLogger{Logger: gormcore.New(gormcore.Config{
		Message:        downstreamMySQLMessage,
		Name:           cfg.Name,
		DurationPrefix: downstreamMySQLMessage,
		Endpoint: &gormcore.Endpoint{
			Type: endpointType,
			Name: endpoint,
		},
		SlowThreshold:  cfg.SlowThreshold,
		LogSQL:         cfg.LogSQL,
		InterpolateSQL: cfg.InterpolateSQL,
		ErrorDetails:   addMySQLErrorDetails,
	})}
}

func addMySQLErrorDetails(err error, details map[string]any) {
	if mysqlErr, ok := errors.AsType[*mysqldriver.MySQLError](err); ok {
		details["mysql_errno"] = mysqlErr.Number
		if mysqlErr.SQLState != [5]byte{} {
			details["sqlstate"] = string(mysqlErr.SQLState[:])
		}
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
