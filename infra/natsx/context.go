package natsx

import (
	"context"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
)

// TraceHeader 是消息使用的日志追踪 Header。
const TraceHeader = "X-Log-Id"

// MessageContext 为一条入站消息建立独立日志作用域并还原追踪 ID。
// 保留 parent 的取消和值，但不共享其日志字段存储。
// 精确读取 X-Log-Id，信任其中的非空 ID，仅在缺失或为空时生成。
func MessageContext(parent context.Context, header nats.Header) context.Context {
	id := header.Get(TraceHeader)
	if id == "" {
		id = logit.NewLogID()
	}
	ctx := logit.NewContextScope(parent)
	logit.AddMeta(ctx, logit.Str(logit.LogId, id))
	return ctx
}

func setMessageLogID(ctx context.Context, msg *nats.Msg) string {
	id, ok := logit.LogIDFromContext(ctx)
	if !ok {
		id = logit.NewLogID()
	}
	if msg.Header == nil {
		msg.Header = make(nats.Header)
	}
	msg.Header.Set(TraceHeader, id)
	return id
}
