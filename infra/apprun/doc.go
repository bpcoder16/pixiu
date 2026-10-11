// Package apprun 提供实例式常驻服务管理，监督运行并按登记逆序关闭。
// 只依赖标准库和 logit，不认识具体 HTTP、cron、NATS 或任务池类型。
// 设计见 docs/apprun-design.md。
//
// Manager 零值可用。应用先创建全部依赖，再登记服务；依赖先登记、生产者后登记。
// Run 并发执行回调，ctx 只通知管理器停机，不传给业务请求。服务退出或 ctx 取消后，
// 总期限内逐个调用 Shutdown，并等该服务 Run 返回，才关闭下一个服务。管理器只允许运行一次。
// 总关闭期限仅通过 New 设置，创建后不可修改；零值默认 20 秒，负值在创建时返回错误。
// 全部关闭回调和退出等待共享此期限。
// 未发起该服务关闭时 Run 返回 nil 视为 ErrUnexpectedExit；真实错误汇总保留原始错误链。
// 预取消不启动 Run，但仍在总期限内逆序关闭已登记服务。回调 panic 转为带堆栈的 PanicError。
//
// 应用可将现有 HTTP 服务交给管理器（需导入 context、errors、net/http，
// 以及 github.com/bpcoder16/pixiu/infra/apprun 和 infra/httpserver）：
//
//	manager, err := apprun.New(0) // 使用默认 20 秒总关闭期限。
//	if err != nil {
//		return err
//	}
//	server, err := httpserver.New(httpserver.Config{
//		Addr: ":8888",
//	}, http.NotFoundHandler())
//	if err != nil {
//		return err
//	}
//	if err := manager.Register(apprun.Service{
//		Name: "http",
//		Run: func() error {
//			err := server.Run()
//			// 处理收到停机请求时，Run 尚未进入 HTTP 启动代码的竞争。
//			if errors.Is(err, httpserver.ErrClosed) {
//				return nil
//			}
//			return err
//		},
//		Shutdown: func() error {
//			_ = server.Shutdown()
//			return server.Wait() // 保留关闭结果,同时等待 handler 收尾。
//		},
//	}); err != nil {
//		return errors.Join(err, server.Shutdown())
//	}
//	return manager.RunSignals(context.Background())
//
// Run 不安装信号处理；RunSignals 显式订阅 SIGINT/SIGTERM，首次停机后立即取消订阅，
// 无其他订阅者且原来使用默认处理时，后续中断恢复直接退出。应用只使用一个信号所有者。
// SIGKILL 和 SIGSTOP 无法捕获；SIGHUP、SIGQUIT 保持原行为。包不会调用 os.Exit。
//
// 回调须支持 Shutdown 先于或并发于 Run，正常关闭状态错误由上层适配为 nil。
// 不得在回调中等待 Manager.Run，不得使用 runtime.Goexit 或 testing.T.Fatal。
// 回调自己启动的 goroutine 不受管理。Shutdown panic 会立即记录堆栈，并与普通关闭错误
// 一样在剩余总关闭期限内等待 Run 返回，再继续回收依赖；超时时同时保留 PanicError。
//
// 总期限从进入停机流程开始计算，不限制正常运行时间，也不包含应用资源清理时间。
// 总期限耗尽返回 ErrShutdownTimeout，并停止发起后续关闭；已执行的回调可能继续运行。
// 超时分支非阻塞地收集已到达的 Shutdown 结果，不等待迟到结果。
// 此错误不代表服务已经停止；应用若继续清理资源，须接受未退出任务与清理并发，随后终止进程。
// 本包不强制终止 goroutine。HTTP 适配在 Shutdown 后调用 Wait，在总期限内等待 handler 收尾；
// Run 返回也不能证明组件未等待的业务回调已退出，其他组件同样须等待实际业务完成。
// 正常退出先等待服务，再释放客户端，最后关闭日志；应用负责资源清理及最终进程期限。
// 已交给管理器的服务不重复登记到 lifecycle.Stack；资源栈继续负责其他资源。
// 启动 Run 不等于服务 ready；首版不提供就绪依赖、自动重启或运行期追加服务。
package apprun
