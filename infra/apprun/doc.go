// Package apprun 提供实例式常驻服务监督、统一停收和并行排空，依赖标准库与 logit。
// 设计见 docs/apprun-design.md。Manager 零值默认 30 秒，New 可设置整个停收与排空期限。
// Stop 只封闭入口，Wait(ctx) 等待实际工作与收尾，Cancel 必须立即发出取消通知并返回。
// 全部回调必填，停收可先于 Run；未停收时 Run 返回 nil 视为 ErrUnexpectedExit。
//
// 以 HTTP 为例（需导入 context、errors、net/http、time、apprun 和 httpserver）：
//
//	server, err := httpserver.New(httpserver.Config{
//		Addr: ":8888",
//	}, http.NotFoundHandler())
//	if err != nil {
//		return err
//	}
//	var manager apprun.Manager
//	if err := manager.Register(apprun.Service{
//		Name: "http",
//		Run: func() error {
//			err := server.Run()
//			if errors.Is(err, httpserver.ErrClosed) {
//				return nil
//			}
//			return err
//		},
//		Stop: server.Stop,
//		Wait: server.Wait,
//		Cancel: server.Cancel,
//	}); err != nil {
//		_ = server.Stop()
//		server.Cancel()
//		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
//		defer cancel()
//		return errors.Join(err, server.Wait(ctx))
//	}
//	return manager.RunSignals()
//
// Run 的 context 只通知停机，不提前取消业务。收到信号、取消或服务退出后，向全部服务发起 Stop，
// 并行等待 Wait 与 Run；同一截止时间从停机开始计算，不逐组件重置。已接收的队列工作属于存量。
// 错误与 panic 保留；超时返回 ErrShutdownTimeout 并取消未完成服务，不再等待业务响应。
// Wait 返回错误或 panic 时按未确认排空处理，返回前 Cancel 兜底；超时错误包含未完成服务名与关闭阶段。
// 无论是否超时，入口随后调用资源栈回收；lifecycle.Stack 默认整体 20 秒，可通过 New 自定义。
// 资源回收超时须立即退出进程；本包不接管资源栈或调用 os.Exit，不能强杀 goroutine。
// 登记顺序不提供服务就绪或停机依赖保证；已停收的业务消费者不保证接收在途任务发出的新请求。
// RunSignals 订阅 SIGINT/SIGTERM，首次停机后恢复原信号行为；应用只应有一个信号所有者。
// SIGKILL/SIGSTOP 不可捕获，SIGHUP/SIGQUIT 保持原行为。回调不得同步等待 Manager.Run 或使用 runtime.Goexit。
package apprun
