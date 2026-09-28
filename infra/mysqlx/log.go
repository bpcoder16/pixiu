package mysqlx

import (
	"context"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

const downstreamMySQLMessage = "MySQL"

// traceLogger 把 GORM 的查询结果和诊断消息写入 logit，并按配置保留或展开 SQL 参数。
type traceLogger struct {
	name           string
	endpointType   string
	endpoint       string
	slowThreshold  time.Duration
	logSQL         bool
	interpolateSQL bool
	level          logger.LogLevel
}

var _ logger.Interface = (*traceLogger)(nil)
var _ gorm.ParamsFilter = (*traceLogger)(nil)

func newTraceLogger(cfg Config, endpointType, endpoint string) *traceLogger {
	return &traceLogger{
		name:           cfg.Name,
		endpointType:   endpointType,
		endpoint:       endpoint,
		slowThreshold:  cfg.SlowThreshold,
		logSQL:         cfg.LogSQL,
		interpolateSQL: cfg.InterpolateSQL,
		level:          logger.Info,
	}
}

func (l *traceLogger) LogMode(level logger.LogLevel) logger.Interface {
	clone := *l
	clone.level = level
	return &clone
}

func (l *traceLogger) ParamsFilter(_ context.Context, sql string, params ...any) (string, []any) {
	if l.interpolateSQL {
		return sql, params
	}
	return sql, nil
}

func (l *traceLogger) Info(ctx context.Context, format string, args ...any) {
	l.diagnostic(ctx, logger.Info, logit.InfoLevel, format, args...)
}

func (l *traceLogger) Warn(ctx context.Context, format string, args ...any) {
	l.diagnostic(ctx, logger.Warn, logit.WarnLevel, format, args...)
}

func (l *traceLogger) Error(ctx context.Context, format string, args ...any) {
	l.diagnostic(ctx, logger.Error, logit.ErrorLevel, format, args...)
}

// GORM 的诊断消息没有实际查询耗时，因此耗时记为 0，不填充 SQL 和行数字段。
func (l *traceLogger) diagnostic(ctx context.Context, gormLevel logger.LogLevel, level logit.Level, format string, args ...any) {
	if l.level < gormLevel || !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	details := map[string]any{
		"endpoint_type": l.endpointType,
		"endpoint":      l.endpoint,
		"msg":           fmt.Sprintf(format, args...),
	}
	fields := logit.DownstreamFields(downstreamMySQLMessage, l.name, 0, details)
	logit.Output(ctx, level, 1, downstreamMySQLMessage, fields...)
}

func (l *traceLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	elapsed := time.Since(begin)
	logit.AddDownstreamDurationAuto(ctx, "mysql", elapsed)
	if l.level <= logger.Silent {
		return
	}
	var level logit.Level
	switch {
	case err != nil && l.level >= logger.Error:
		level = logit.ErrorLevel
	case elapsed > l.slowThreshold && l.level >= logger.Warn:
		level = logit.WarnLevel
	case l.logSQL && l.level >= logger.Info:
		level = logit.InfoLevel
	default:
		return
	}
	if !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	sql, rows := fc()
	details := map[string]any{
		"endpoint_type": l.endpointType,
		"endpoint":      l.endpoint,
		"rows":          rows,
		"sql":           sql,
	}
	if err != nil {
		details["err"] = err.Error()
	}
	fields := logit.DownstreamFields(downstreamMySQLMessage, l.name, elapsed, details)
	logit.Output(ctx, level, 0, downstreamMySQLMessage, fields...)
}
