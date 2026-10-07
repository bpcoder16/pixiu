package logit

import (
	"context"
	"slices"
	"uuid"
)

// LogId 是链路 ID 的统一日志字段名；通过 AddMeta(ctx, Str(LogId, id)) 设置传播值。
const LogId = "logId"

// NewLogID 返回 RFC 9562 UUIDv4 的小写 canonical 字符串。
func NewLogID() string {
	return uuid.NewV4().String()
}

// WithContextLogID 初始化日志字段存储，并生成、追加一个新的链路 ID。
// 每次调用都会追加新 ID，应只在入口调用；后续直接传递返回的 context。
func WithContextLogID(ctx context.Context) context.Context {
	ctx = WithContext(ctx)
	AddMeta(ctx, Str(LogId, NewLogID()))
	return ctx
}

// LogIDFromContext 返回 meta 中最后一个非空字符串 LogId。
// 普通字段、非字符串与空值不参与传播，也不会执行惰性字段回调。
func LogIDFromContext(ctx context.Context) (string, bool) {
	store := findStore(ctx, ctxKeyMeta)
	if store == nil {
		return "", false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	for _, v := range slices.Backward(store.fields) {
		field := v.field
		if field.Key == LogId && field.typ == strType && field.str != "" {
			return field.str, true
		}
	}
	return "", false
}
