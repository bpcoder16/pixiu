// Package taskpool 提供本地异步任务池，支持按实例使用或注册为进程级默认池，零第三方依赖。
// 设计与边界见 docs/taskpool-design.md。
//
// 初始化时设置等待队列容量、消费者区间和失败重试次数；
// MaxRetries 为 0 到 100，表示首次失败后最多再尝试的次数，零值不重试。
// 消费者会按积压在区间内自动扩缩容；失败重试固定间隔 1 秒，
// SubmitTimeout 为 0 时默认 1 秒，IdleTimeout 为 0 时默认 5 分钟，
// DrainTimeout 为 0 时默认 15 秒。
//
// 主协程创建独立任务池，并负责在退出时调用 Shutdown：
//
//	pool, err := taskpool.New(taskpool.Config{
//	    MinWorkers:    2,
//	    MaxWorkers:    8,
//	    QueueSize:     256,
//	    SubmitTimeout: time.Second, // 一次提交的超时预算为 1 秒
//	    MaxRetries:    3,           // 首次执行失败后最多再试 3 次
//	})
//	if err != nil {
//	    return err
//	}
//
// 业务入口直接向该实例提交任务。SubmitTimeout 从参数校验通过后、首次获取锁前开始计时，
// 获取锁与等待队列空位共享预算；即使队列有空位，检查时已超时也会拒绝提交。
// 内部锁等待不可中断，函数可能在取得锁后才返回超时；requestCtx 可以更早取消提交。
// SubmitTimeout 为 0 时默认 1 秒，正值按配置使用，不设 100ms 下限。
// 成功接收的任务使用任务池提供的 taskCtx 执行，不会因请求结束或
// Shutdown 开始排空而立刻中断。
// 示例中的 requestCtx 和 sendNotice 由应用提供：
//
//	if err := pool.Submit(requestCtx, "send-notice", func(taskCtx context.Context) error {
//	    return sendNotice(taskCtx) // 业务函数应响应 taskCtx 取消
//	}); err != nil {
//	    return err
//	}
//
// 应用自行处理停机信号，先停止上游提交，再调用 Shutdown 获取限时关闭结果。
// 任务池只通过 Shutdown 发起关闭；宽限倒计时从首次调用开始，不因后续调用而重置。
// 全部任务提前完成时正常返回，宽限到期仍有任务时中止排空并返回超时错误：
//
//	if err := pool.Shutdown(); err != nil {
//	    return err
//	}
//
// Shutdown 超时返回时任务可能仍在清理资源。只要求限时尽力关闭时，可直接继续退出。
// Wait 是可选操作：需要确认全部任务已退出时，再调用 pool.Wait()；
// 它没有等待上限，完成后仍返回此前的关闭错误。Wait 只观察完成，不发起关闭；
// 如果先调用 Wait，需要其他 goroutine 调用 Shutdown。两个方法均允许重复或并发调用。
//
// 程序需要全局默认池时，可将上面的 New 调用改为 NewDefault；
// 它创建并注册默认池，业务代码可直接调用包级 Submit，退出时可调用
// 包级 Shutdown，并按需调用 Wait。New 仍可同时创建其他独立任务池，分别通过
// 各自的 Submit、Wait、Shutdown 管理；全局入口只作用于默认池。
//
//	if _, err := taskpool.NewDefault(taskpool.Config{
//	    MinWorkers: 2,
//	    MaxWorkers: 8,
//	    QueueSize:  256,
//	    MaxRetries: 3,
//	}); err != nil {
//	    return err
//	}
//	if err := taskpool.Submit(requestCtx, "send-notice", sendNotice); err != nil {
//	    return err
//	}
//	return taskpool.Shutdown()
//
// 未注册默认池时，全局 Submit、Wait、Shutdown 返回 ErrNoDefault。
// Default 返回当前池或 nil；Swap 可替换并返回旧池，传入 nil 可清除注册。
// SetDefault 和 Swap 均不会关闭旧池，调用方仍负责旧池的退出。
// 全局 Wait/Shutdown 与实例方法的语义相同；若可能切换默认池，
// 应保留实例指针，通过同一个实例执行 Shutdown 和 Wait。
//
// 使用 lifecycle.Stack 同时回收任务池和任务依赖的 client 时，
// client 指应用创建、供任务使用且具有 Close() error 方法的资源，
// 例如 *sql.DB；它不是 taskpool 或 lifecycle 提供的对象。
// 先登记 client，再登记任务池。Stack.Close 逆序执行，确保任务结束后才关闭 client。
// 此时调用 Shutdown 发起排空，随后用 Wait 确认全部 worker 退出：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(client.Close); err != nil {
//	    return err
//	}
//	if err := stack.Register(func() error {
//	    shutdownErr := pool.Shutdown()
//	    _ = pool.Wait() // 等待任务清理完毕，再允许 Stack 关闭 client
//	    return shutdownErr
//	}); err != nil {
//	    return err
//	}
//	// 停止上游提交任务后：
//	return stack.Close()
//
// 任务没有这类依赖时，省略 client.Close 的注册即可。
// Shutdown 发起的 DrainTimeout 到期会放弃排队任务并取消执行 context，不依赖调用 Wait。
// Go 无法强制终止不响应取消的函数，这种任务会让 Wait 一直等待；
// 只收到 Shutdown 的超时结果时，不能立即关闭任务仍在使用的 client。
// 如果还有日志资源，应先登记日志关闭函数，使其最后关闭。
//
// Submit 返回 nil 只表示任务已接收。任务每次执行失败都会写入 stderr；
// panic 会输出堆栈且不自动重试。可用 Stats 查看队列、消费者及累计结果。
// 任务只保存在内存中，进程异常退出后不会恢复；需要可靠交付时应使用持久化队列。
package taskpool
