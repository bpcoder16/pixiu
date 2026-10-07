package taskpool

import (
	"context"

	"github.com/bpcoder16/pixiu/logit"
)

// newTaskContext 在接收时保存链路 ID，为每个任务创建独立日志作用域。
// 回调不继承提交方或池的取消，其他 context 值仍保留。
func newTaskContext(parent context.Context) context.Context {
	id, ok := logit.LogIDFromContext(parent)
	if !ok {
		id = logit.NewLogID()
	}
	ctx := logit.NewContextScope(context.WithoutCancel(parent))
	logit.AddMeta(ctx, logit.Str(logit.LogId, id))
	return ctx
}
