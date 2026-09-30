// Package lockx 定义分布式阻塞与非阻塞锁契约，提供获取、执行和释放的通用流程。
// 业务依赖 Locker（等待获取）或 TryLocker（单次尝试），初始化时选择具体后端。
// mysqllock 提供会话与固定租期阻塞锁，redislock 同时提供固定租期的两种获取方式。
//
// 以下示例需导入 context 和 github.com/bpcoder16/pixiu/biz/lockx；
// locker 由启动流程注入，confirm 为应用提供的业务函数：
//
//	func confirmOrder(
//	    ctx context.Context,
//	    locker lockx.Locker,
//	    orderID string,
//	    confirm func(context.Context) error,
//	) error {
//	    return lockx.Do(ctx, locker, "orders:confirm:"+orderID, confirm)
//	}
//
// 使用 TryLocker 时通过 TryDo 调用：
//
//	func tryConfirmOrder(
//	    ctx context.Context,
//	    locker lockx.TryLocker,
//	    confirm func(context.Context) error,
//	) (bool, error) {
//	    return lockx.TryDo(ctx, locker, "orders:confirm:123", confirm)
//	}
//
// TryDo 的 false、nil 表示竞争失败，true 只表示进入过回调，不代表业务已提交。
// 两个函数同步执行回调最多一次，工作期限取请求与 Lock.Deadline 的较早者。
// 会话锁不额外限制工作时间，获取阶段等待预算不会带入工作；租期锁不自动续租。
// 回调返回或 panic 后使用独立的 1 秒期限尽力释放；正常返回合并错误，保留原始 panic。
//
// 手工调用 Lock/TryLock 时应立即登记释放，租期句柄的 Deadline 包含成功尝试耗时。
// 获取 context 取消不会自动释放已交付的句柄。Unlock 原子校验持有身份，
// Redis 对不再持有、到期或重复释放幂等返回 nil，MySQL 当前返回 ErrNotHeld。
// 释放的 nil 不代表仍持有锁，业务本身的租期超时仍会返回；取消不能强制停止业务。
//
// 不同后端或协议的同名锁互不排斥；同一资源的竞争者必须使用相同实现。
// 迁移前先停止旧竞争者并完成旧临界区。锁不替代幂等、事务或数据端版本约束，
// 过期或会话断开的旧任务仍可能执行。详细契约见 docs/lockx-design.md。
package lockx
