// Package lifecycle 提供实例式资源关闭栈，零第三方依赖且没有进程级全局状态。
// 设计文档见 docs/lifecycle-design.md。
//
// Stack 零值总回收期限为 20 秒。使用 New(timeout) 可自定义整个栈的期限；0 沿用默认值，负值非法。调用方登记自己负责关闭的资源，Close 按登记的逆序执行；
// 即使某个关闭函数返回错误或 panic，仍在剩余期限内尝试其余函数。首次 Close 在
// 全部尝试后重新抛出第一个 panic；后续 Close 返回包含 panic 信息的汇总错误。
// Close 不调用 panic 值的格式化方法；读取汇总错误文本时才生成 panic 信息。
//
// 典型用法（logger 和 client 已由调用方创建）：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error { return logit.Close(logger) }); err != nil {
//	    return err
//	}
//	if err := stack.Register(client.Close); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	// 服务排空结束或超时取消后：
//	return stack.Close()
//
// 日志关闭函数先登记，因此在其他资源关闭后才执行。共享资源只登记一次；
// 模块不需要依赖 lifecycle，也不应自行注册到全局关闭列表。
// 回调在独立 goroutine 执行，Close 在整体期限内逐个等待；超时返回 ErrCloseTimeout，
// 同时保留 context.DeadlineExceeded 和已完成错误，不再发起后续回收。迟到回调可能继续运行。
// 调用方须在回收超时后立即退出进程；库不调用 os.Exit。重复 Close 共用首次结果与期限。
// 自定义示例（需导入 time）：
//
//	stack, err := lifecycle.New(15 * time.Second)
//	if err != nil {
//		return err
//	}
//	return stack.Close()
package lifecycle
