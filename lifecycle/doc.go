// Package lifecycle 提供实例式资源关闭栈，零第三方依赖且没有进程级全局状态。
// 设计文档见 docs/lifecycle-design.md。
//
// Stack 的零值可用。调用方登记自己负责关闭的资源，Close 按登记的逆序执行；
// 即使某个关闭函数返回错误，仍会尝试其余函数，并汇总错误返回。
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
//	// 停止使用资源的任务后：
//	return stack.Close()
//
// 日志关闭函数先登记，因此在其他资源关闭后才执行。共享资源只登记一次；
// 模块不需要依赖 lifecycle，也不应自行注册到全局关闭列表。
package lifecycle
