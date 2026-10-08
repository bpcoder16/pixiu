// Package httpserver 提供独立 HTTP 服务端和限时关闭,不管理 WebSocket 连接。
//
// 零值配置面向普通 JSON API：请求头读取期限 5 秒，完整请求读取期限 10 秒，
// 响应写入期限 15 秒，空闲连接期限 60 秒，请求头上限 64 KiB。
// 上传、下载或流式服务可按需增大读写期限；ReadTimeout、WriteTimeout
// 设为负值(例如 -1)可分别禁用对应超时，其他限制不允许负值。
// 网络读写超时不代替业务 context 的执行期限，请求体大小应由 handler 单独限制。
//
// Run 和 Shutdown 均不接收外部 context。应用处理停机信号后主动调用 Shutdown，
// 排空期限从首次调用开始，ShutdownTimeout 为零时默认 20 秒。
// 实例内部 context 在排空完成或超时强制关闭连接后取消，不提前取消正在排空的请求。
//
// 以下示例需导入 net/http、os、os/signal、syscall 和
// github.com/bpcoder16/pixiu/infra/httpserver：
//
//	server, err := httpserver.New(httpserver.Config{
//		Addr: ":8888",
//	}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
//		w.WriteHeader(http.StatusNoContent)
//	}))
//	if err != nil {
//		return err
//	}
//	stop := make(chan os.Signal, 1)
//	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
//	defer signal.Stop(stop)
//	served := make(chan error, 1)
//	go func() {
//		served <- server.Run()
//	}()
//	select {
//	case err := <-served:
//		_ = server.Shutdown()
//		return err
//	case <-stop:
//		shutdownErr := server.Shutdown()
//		runErr := <-served
//		if shutdownErr != nil {
//			return shutdownErr
//		}
//		return runErr
//	}
//
// 第一版由 Run 按 Config.Addr 创建普通 TCP listener。
// Gin Engine 等实现 http.Handler 的路由器可直接传给 New，再调用 Run。
// Shutdown 是唯一关闭入口，允许重复或并发调用，共享首次关闭的期限和结果。
// 超时会强制关闭普通连接，但不能强制终止忽略请求 context 的业务代码。
// WebSocket 等已接管连接需独立关闭；日志由应用最后关闭。
package httpserver
