package gorm

import (
	"context"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm/logger"
)

// Endpoint 是可选的主从端点标识；未提供时不输出端点字段。
type Endpoint struct {
	Type string
	Name string
}

// Config 配置 GORM 日志核心。调用方负责先归一化慢查询阈值。
type Config struct {
	Message        string
	Name           string
	DurationPrefix string
	Endpoint       *Endpoint
	SlowThreshold  time.Duration
	LogSQL         bool
	InterpolateSQL bool
}

// Logger 处理 GORM 查询与诊断日志；数据库模块实现 GORM Logger 入口。
type Logger struct {
	message        string
	name           string
	durationPrefix string
	endpoint       *Endpoint
	slowThreshold  time.Duration
	logSQL         bool
	interpolateSQL bool
	level          logger.LogLevel
}

// New 创建日志核心。
func New(cfg Config) *Logger {
	l := &Logger{
		message:        cfg.Message,
		name:           cfg.Name,
		durationPrefix: cfg.DurationPrefix,
		slowThreshold:  cfg.SlowThreshold,
		logSQL:         cfg.LogSQL,
		interpolateSQL: cfg.InterpolateSQL,
		level:          logger.Info,
	}
	if cfg.Endpoint != nil {
		endpoint := *cfg.Endpoint
		l.endpoint = &endpoint
	}
	return l
}

// WithLevel 复制日志核心并设置 GORM 日志级别。
func (l *Logger) WithLevel(level logger.LogLevel) *Logger {
	clone := *l
	clone.level = level
	return &clone
}

// ParamsFilter 控制是否将 SQL 参数交给 GORM 方言展开。
func (l *Logger) ParamsFilter(_ context.Context, sql string, params ...any) (string, []any) {
	if l.interpolateSQL {
		return sql, params
	}
	return sql, nil
}

// Diagnostic 记录 GORM 诊断消息；caller 指向数据库模块对应的级别入口。
func (l *Logger) Diagnostic(ctx context.Context, gormLevel logger.LogLevel, level logit.Level, format string, args ...any) {
	if l.level < gormLevel || !logit.LoggerFromContext(ctx).Enabled(level) {
		return
	}
	details := map[string]any{
		"msg": fmt.Sprintf(format, args...),
	}
	l.addEndpoint(details)
	fields := logit.DownstreamFields(l.message, l.name, 0, details)
	logit.Output(ctx, level, 1, l.message, fields...)
}

// LogTrace 记录查询结果和请求耗时；caller 指向数据库模块的 Trace 入口。
func (l *Logger) LogTrace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	elapsed := time.Since(begin)
	logit.AddDownstreamDurationAuto(ctx, l.durationPrefix, elapsed)
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
		"rows": rows,
		"sql":  sql,
	}
	l.addEndpoint(details)
	if err != nil {
		details["err"] = err.Error()
	}
	fields := logit.DownstreamFields(l.message, l.name, elapsed, details)
	logit.Output(ctx, level, 1, l.message, fields...)
}

func (l *Logger) addEndpoint(details map[string]any) {
	if l.endpoint != nil {
		details["endpoint_type"] = l.endpoint.Type
		details["endpoint"] = l.endpoint.Name
	}
}
