package lockctx

import (
	"context"
	"time"
)

// Err 检查取消和本地期限；ctx 必须非 nil，由调用方先校验。
// 已有取消错误时优先保留其原因，未取消且期限已到时返回 DeadlineExceeded。
func Err(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// 取消计时器可能尚未调度，期限本身仍必须有效。
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
