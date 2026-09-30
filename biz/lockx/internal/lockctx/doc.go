// Package lockctx 为 lockx 公共流程及其后端统一检查 context 取消与期限。
// 仅供 biz/lockx 及其子包使用，生产代码只依赖标准库。
//
// 以下示例需导入 context 和
// github.com/bpcoder16/pixiu/biz/lockx/internal/lockctx：
//
//	func check(ctx context.Context) error {
//	    return lockctx.Err(ctx)
//	}
//
// ctx 必须非 nil，由调用方负责参数校验。Err 优先返回 ctx.Err()，
// 再补充期限检查，覆盖期限已到但取消计时器尚未调度的窗口。
package lockctx
