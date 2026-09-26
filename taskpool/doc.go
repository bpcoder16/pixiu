// Package taskpool 提供按实例创建的本地异步任务池，零第三方依赖。
// 设计与边界见 docs/taskpool-design.md。
//
// 初始化时设置等待队列容量、消费者区间和失败重试次数；
// MaxRetries 为 1 到 100，表示首次失败后最多再尝试的次数。
// 消费者会按积压在区间内自动扩缩容；失败重试固定间隔 1 秒，
// SubmitTimeout 为 0 时默认 1 秒，IdleTimeout 为 0 时默认 5 分钟，
// DrainTimeout 为 0 时默认 15 秒。
//
// 主协程可把系统信号转为停机 context，再创建任务池：
//
//	stopCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer stop()
//	pool, err := taskpool.New(stopCtx, taskpool.Config{
//	    MinWorkers:    2,
//	    MaxWorkers:    8,
//	    QueueSize:     256,
//	    SubmitTimeout: time.Second, // 满队列最多等待 1 秒
//	    MaxRetries:    3,           // 首次执行失败后最多再试 3 次
//	})
//	if err != nil {
//	    return err
//	}
//
// 业务入口直接提交任务。队列满时，SubmitTimeout 限制等待时间；
// requestCtx 可以更早取消等待。stopCtx 取消时停止接收并开始排空；
// 成功接收的任务使用任务池提供的 taskCtx 执行，不会因请求结束或
// stopCtx 取消而立刻中断。
// 示例中的 requestCtx 和 sendNotice 由应用提供：
//
//	if err := pool.Submit(requestCtx, "send-notice", func(taskCtx context.Context) error {
//	    return sendNotice(taskCtx) // 业务函数应响应 taskCtx 取消
//	}); err != nil {
//	    return err
//	}
//
// 收到停机信号后先停止上游提交，再等待排空。宽限倒计时从 stopCtx
// 取消时开始，不因 Wait 调用时间而重置：
//
//	<-stopCtx.Done()
//	if err := pool.Wait(); err != nil {
//	    return err
//	}
//
// 使用 lifecycle.Stack 同时回收任务池和任务依赖的 client 时，
// client 指应用创建、供任务使用且具有 Close() error 方法的资源，
// 例如 *sql.DB；它不是 taskpool 或 lifecycle 提供的对象。
// 先登记 client，再登记任务池。Stack.Close 逆序执行，确保任务结束后才关闭 client。
// Shutdown 可在停机信号之前主动排空，并一直等待全部 worker 退出：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(client.Close); err != nil {
//	    return err
//	}
//	if err := stack.Register(func() error {
//	    return pool.Shutdown()
//	}); err != nil {
//	    return err
//	}
//	// 停止上游提交任务后：
//	return stack.Close()
//
// 任务没有这类依赖时，省略 client.Close 的注册即可。
// Shutdown 只等待 done，不观察 DrainTimeout，到期也不会中止任务；
// 因此可能一直等待无法结束的任务。若使用 Wait，宽限到期后会返回超时错误，
// 但不响应取消的任务可能仍在运行；
// 此时不能立即关闭任务依赖的 client。Wait 与 Shutdown 在同一实例上合计只能调用一次，
// 重复或并发调用均 panic。需要保证依赖资源的关闭顺序时使用上述 Shutdown 方案。
// 如果还有日志资源，应先登记日志关闭函数，使其最后关闭。
//
// Wait 观察到 stopCtx 取消后的宽限期到期时，才放弃未开始的任务，
// 并取消执行中的任务。
// Submit 返回 nil 只表示任务已接收。任务每次执行失败都会写入 stderr；
// panic 会输出堆栈且不自动重试。可用 Stats 查看队列、消费者及累计结果。
// 任务只保存在内存中，进程异常退出后不会恢复；需要可靠交付时应使用持久化队列。
package taskpool
