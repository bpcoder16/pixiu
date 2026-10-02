package clickhousex

import (
	"context"
	"errors"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const downstreamClickHouseMessage = "ClickHouse"

// traceLogger 为 ClickHouse 提供 GORM Logger 入口、主从端点标识和服务端错误码。
type traceLogger struct {
	*gormcore.Logger
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config, endpointType, endpoint string) *traceLogger {
	return &traceLogger{Logger: gormcore.New(gormcore.Config{
		Message:        downstreamClickHouseMessage,
		Name:           cfg.Name,
		DurationPrefix: downstreamClickHouseMessage + "_" + cfg.Name,
		Endpoint: &gormcore.Endpoint{
			Type: endpointType,
			Name: endpoint,
		},
		SlowThreshold:  cfg.SlowThreshold,
		LogSQL:         cfg.LogSQL,
		InterpolateSQL: cfg.InterpolateSQL,
		ErrorDetails:   addClickHouseErrorDetails,
	})}
}

func addClickHouseErrorDetails(err error, details map[string]any) {
	if exception, ok := errors.AsType[*clickhouse.Exception](err); ok {
		details["clickhouse_code"] = exception.Code
		if exception.Name != "" {
			details["clickhouse_name"] = exception.Name
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
