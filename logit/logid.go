package logit

import (
	"context"
	"uuid"
)

// NewLogID 返回 RFC 9562 UUIDv4 的小写 canonical 字符串。
func NewLogID() string {
	return uuid.NewV4().String()
}

// SetLogID 把 logId 写入 meta 作用域(已有则覆盖)。
func SetLogID(ctx context.Context, id string) {
	mustStore(ctx, ctxKeyMeta).add(Str(logIdKey, id), AllLevels)
}

// LogID 返回 ctx 中的 logId,不存在返回空串。
func LogID(ctx context.Context) string {
	if f, ok := FindMeta(ctx, logIdKey); ok {
		return f.str
	}
	return ""
}

// NewTraceContext 返回已初始化字段存储并携带 logId 的 ctx:
// 入口没有上游 logId(如 MQ 消费者、独立任务)时一步建立新链路。
func NewTraceContext(ctx context.Context) context.Context {
	ctx = WithContext(ctx)
	if LogID(ctx) == "" {
		SetLogID(ctx, NewLogID())
	}
	return ctx
}
