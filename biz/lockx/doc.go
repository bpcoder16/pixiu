// Package lockx 定义分布式阻塞与非阻塞锁契约，提供获取、执行和释放的通用流程。
// 业务依赖 Locker（等待获取）或 TryLocker（单次尝试），初始化时选择具体后端。
// 当前由 redislock 提供固定租期的阻塞与非阻塞锁。
//
// 需要复用配置时通过 New(backend) 绑定为 Executor，调用 executor.Do/TryDo 无需再次传入后端。
// NewNamed(name, backend) 创建命名执行器，NewDefault(name, backend) 同时将其设为默认；
// Default() 与对应的 Named(name) 返回同一个实例，New 和 NewNamed 不隐式设置默认实例。
// 空白名称、重复名称、第二个默认实例和 nil 后端返回错误，失败不占用名称或默认位置；
// 获取未登记的实例会 panic。启动阶段完成全部登记后再运行业务，不支持配置热切换。
// 执行器不拥有后端资源，也不提供 Close；应用须先停止业务，再关闭共享客户端。
// 名称仅选择配置，不参与 key 构造；同一 Redis 数据库的相同 key 在不同执行器之间仍然互斥。
// 使用 biz/bootstrap 时可由 MustRegisterRedisLock 按代码声明，在 Redis 就绪后统一装配。
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
// 批量非阻塞执行使用 TryDoMany 或 executor.TryDoMany，Backend 已要求实现 TryManyLocker：
//
//	func tryConfirmBatch(
//	    ctx context.Context,
//	    keys []string,
//	    confirm func(context.Context) error,
//	) (bool, error) {
//	    return lockx.Default().TryDoMany(ctx, keys, confirm)
//	}
//
// keys 必须非空且不能包含空白 key；复制并按原值去重，不修剪、不改大小写。
// 整批获取成功后才执行一次回调；任一冲突时整批失败。单 key 与批量使用相同 key 时互斥。
// Backend 内嵌 Locker、TryLocker 和 TryManyLocker，三种能力在编译期约束，执行器直接调用。
// 仅增加非阻塞批量能力，不提供 DoMany / LockMany。应用应限制单次 key 数量，避免过大批次占用后端。
// TryDo / TryDoMany 的 false、nil 表示竞争失败，true 只表示进入过回调，不代表业务已提交。
// 上述执行函数同步执行回调最多一次，工作期限取请求与 Lock.Deadline 的较早者。
// 获取阶段等待预算不会带入工作；内置 Redis 租期锁不自动续租。
// 回调返回或 panic 后使用独立的 1 秒期限尽力释放；正常返回合并错误，保留原始 panic。
//
// 手工调用 Lock/TryLock 时应立即登记释放，租期句柄的 Deadline 包含成功尝试耗时。
// 获取 context 取消不会自动释放已交付的句柄。Unlock 原子校验持有身份，
// 内置实现对不再持有、到期或重复释放幂等返回 nil，真实下游错误仍返回。
// 释放的 nil 不代表仍持有锁，业务本身的租期超时仍会返回；取消不能强制停止业务。
//
// 不同后端或协议的同名锁互不排斥；同一资源的竞争者必须使用相同实现。
// 迁移前先停止旧竞争者并完成旧临界区。锁不替代幂等、事务或数据端版本约束，
// 租期过期后的旧任务仍可能执行。
package lockx
