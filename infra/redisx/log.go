package redisx

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/redis/go-redis/v9"
)

const downstreamRedisMessage = "Redis"

type loggerHook struct {
	name           string
	durationPrefix string
	slowThreshold  time.Duration
	logCommands    bool
}

var _ redis.Hook = (*loggerHook)(nil)

func newLoggerHook(name string, slowThreshold time.Duration, logCommands bool) *loggerHook {
	// 构造时固定耗时前缀，避免逐条命令重复拼接。
	return &loggerHook{
		name:           name,
		durationPrefix: downstreamRedisMessage + "_" + name,
		slowThreshold:  slowThreshold,
		logCommands:    logCommands,
	}
}

func (*loggerHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h *loggerHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		name := cmd.Name()
		if ctx.Value(startupPingKey{}) != nil || isInternalCommand(name) {
			return next(ctx, cmd)
		}
		begin := time.Now()
		err := next(ctx, cmd)
		elapsed := time.Since(begin)
		logit.AddDownstreamDurationAuto(ctx, h.durationPrefix, elapsed)
		h.logResult(ctx, elapsed, name, cmd, err)
		return err
	}
}

func (h *loggerHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if ctx.Value(startupPingKey{}) != nil || allInternal(cmds) {
			return next(ctx, cmds)
		}
		begin := time.Now()
		err := next(ctx, cmds)
		elapsed := time.Since(begin)
		logit.AddDownstreamDurationAuto(ctx, h.durationPrefix, elapsed)
		h.logBatch(ctx, elapsed, cmds, err)
		return err
	}
}

func isInternalCommand(name string) bool {
	switch strings.ToLower(name) {
	case "hello", "auth", "client", "select", "readonly":
		return true
	default:
		return false
	}
}

func allInternal(cmds []redis.Cmder) bool {
	if len(cmds) == 0 {
		return true
	}
	for _, cmd := range cmds {
		if !isInternalCommand(cmd.Name()) {
			return false
		}
	}
	return true
}

func (h *loggerHook) logResult(ctx context.Context, elapsed time.Duration, command string, cmd redis.Cmder, err error) {
	status := "ok"
	if errors.Is(err, redis.Nil) {
		status = "miss"
	} else if err != nil {
		status = "error"
	}
	level, enabled := h.level(ctx, elapsed, status)
	if !enabled {
		return
	}
	details := map[string]any{
		"command":    strings.ToLower(command),
		"status":     status,
		"error_type": classifyError(err),
		"args":       commandArgs(cmd),
	}
	fields := logit.DownstreamFields(downstreamRedisMessage, h.name, elapsed, details)
	logit.Output(ctx, level, 0, downstreamRedisMessage, fields...)
}

func commandArgs(cmd redis.Cmder) []any {
	args := cmd.Args()
	if len(args) <= 1 {
		return []any{}
	}
	return args[1:]
}

func (h *loggerHook) logBatch(ctx context.Context, elapsed time.Duration, cmds []redis.Cmder, resultErr error) {
	transaction := len(cmds) >= 2 && cmds[0].Name() == "multi" && cmds[len(cmds)-1].Name() == "exec"
	start, end := 0, len(cmds)
	if transaction {
		start, end = 1, len(cmds)-1
	}
	misses, failures := 0, 0
	var firstError error
	for _, cmd := range cmds[start:end] {
		err := cmd.Err()
		switch {
		case errors.Is(err, redis.Nil):
			misses++
		case err != nil:
			failures++
			if firstError == nil {
				firstError = err
			}
		}
	}
	// 连接或事务包装层失败时，业务命令可能都没有独立的错误。
	if firstError == nil && resultErr != nil && !errors.Is(resultErr, redis.Nil) {
		firstError = resultErr
		failures = 1
	}
	if misses == 0 && errors.Is(resultErr, redis.Nil) {
		misses = 1
	}
	status := "ok"
	if firstError != nil {
		status = "error"
	} else if misses > 0 {
		status = "miss"
	}
	level, enabled := h.level(ctx, elapsed, status)
	if !enabled {
		return
	}
	details := map[string]any{
		"command":     "pipeline",
		"status":      status,
		"error_type":  classifyError(firstError),
		"count":       end - start,
		"misses":      misses,
		"errors":      failures,
		"transaction": transaction,
	}
	commands := make([]map[string]any, 0, end-start)
	for _, cmd := range cmds[start:end] {
		// 混合批量中也不能把 AUTH 等握手命令的参数写入日志。
		if isInternalCommand(cmd.Name()) {
			continue
		}
		commands = append(commands, map[string]any{
			"command": strings.ToLower(cmd.Name()),
			"args":    commandArgs(cmd),
		})
	}
	details["commands"] = commands
	fields := logit.DownstreamFields(downstreamRedisMessage, h.name, elapsed, details)
	logit.Output(ctx, level, 0, downstreamRedisMessage, fields...)
}

func (h *loggerHook) level(ctx context.Context, elapsed time.Duration, status string) (logit.Level, bool) {
	var level logit.Level
	switch {
	case status == "error":
		level = logit.ErrorLevel
	case elapsed > h.slowThreshold:
		level = logit.WarnLevel
	case h.logCommands:
		level = logit.DebugLevel
	default:
		return 0, false
	}
	return level, logit.LoggerFromContext(ctx).Enabled(level)
}

func classifyError(err error) string {
	if err == nil || errors.Is(err, redis.Nil) {
		return ""
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if networkError, ok := errors.AsType[net.Error](err); ok {
		if networkError.Timeout() {
			return "timeout"
		}
		return "network"
	}
	if _, ok := errors.AsType[redis.Error](err); ok {
		return "redis"
	}
	return "other"
}
