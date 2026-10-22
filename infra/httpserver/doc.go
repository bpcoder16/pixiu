// Package httpserver 提供独立 HTTP 服务端、普通请求停收和实际排空，不管理 WebSocket 等已接管连接。
// 默认请求头读取 5 秒、完整请求读取 10 秒、响应写入 15 秒、空闲连接 60 秒、请求头上限 64 KiB。
// ReadTimeout/WriteTimeout 负值禁用对应网络期限；网络期限不替代业务 context 的执行期限。
//
// 应用可通过 apprun 统一管理；独立使用时由调用方提供排空 context（需导入 context、errors、
// net/http、os、os/signal、syscall、time 和 httpserver）：
//
//	server, err := httpserver.New(httpserver.Config{
//		Addr: ":8888",
//	}, http.NotFoundHandler())
//	if err != nil {
//		return err
//	}
//	stop, release := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
//	defer release()
//	ran := make(chan error, 1)
//	go func() { ran <- server.Run() }()
//	var runErr error
//	select {
//	case runErr = <-ran:
//	case <-stop.Done():
//	}
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	stopErr := server.Stop()
//	waitErr := server.Wait(ctx)
//	if waitErr != nil {
//		server.Cancel()
//	}
//	return errors.Join(runErr, stopErr, waitErr)
//
// Stop 封闭入口并异步排空，不等待 Handler。请求接纳与停收同步，停收后不再启动新业务 Handler。
// Run 的监听退出不代表请求完成；Wait(ctx) 等待启动过程、实际 Handler 与连接收尾，期限仅控制等待。
// 排空期间请求 context 保持有效；Cancel 先复用 Stop，再取消剩余请求并关闭普通连接，不强杀业务。
// Stop/Cancel 可先于或并发于 Run，停收后禁止重启；重复 Stop 复用同一次排空。
// 组件不设独立关闭计时。入口无论排空是否超时都进行有界资源回收，日志最后关闭，随后退出。
package httpserver
