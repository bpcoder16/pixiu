// Package websocketx 提供独立的标准 http.Handler WebSocket 传输，不依赖 Gin 或消息中间件。
// 它保持 Text/Binary 消息边界，提供认证、心跳、可选资格复核、有界队列及分阶段关闭。
// 身份、消息协议、用户/组索引与广播总线由上层业务决定。
//
// # 基本用法
//
// 构造 Endpoint 后挂载普通 HTTP 路由；HTTP 服务和升级后的连接需要分别停止：
//
//	endpoint, err := websocketx.New(websocketx.Config{
//		MaxConnections: 1000,
//		OnMessage: func(ctx context.Context, session *websocketx.Session, message websocketx.Message) error {
//			// Text 和 Binary 均完整回送，不修改空白、换行或二进制字节。
//			return session.Send(ctx, message)
//		},
//	})
//	if err != nil {
//		return err
//	}
//	mux := http.NewServeMux()
//	mux.Handle("GET /ws", endpoint)
//	server := &http.Server{
//		Addr: ":8080",
//		Handler: mux,
//		ReadHeaderTimeout: 5 * time.Second,
//	}
//	// 应用自行启动 server.ListenAndServe 并接管信号；停机时共享同一个等待期限。
//	_ = endpoint.Stop()
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	httpErr := server.Shutdown(ctx)
//	wsErr := endpoint.Wait(ctx)
//	if wsErr != nil {
//		endpoint.Cancel()
//	}
//	return errors.Join(httpErr, wsErr)
//
// 原始传输默认允许匿名，生产应用可配置 Authenticate 校验凭据并返回不可变 Identity。
// AllowedOrigins 默认为完整 scheme/host/port 同源匹配；缺失 Origin 的非浏览器客户端默认允许。
// HTTP 的 80、HTTPS 的 443 默认端口省略与显式填写等价；非默认端口仍须匹配。
// 代理后的外部 Origin 应显式配置，组件不信任 X-Forwarded-*。
// RequireSubprotocol 要求至少一个配置子协议被客户端提供，握手失败不会执行业务消息回调。
//
// # 与 ginx、httpserver 共用端口
//
// 三个包在应用层组合：ginx 负责路由，httpserver 负责唯一的端口监听，websocketx 管理升级后的连接。
// 下例的普通 HTTP 接口 http://localhost:8080/ping 与 WebSocket 地址 ws://localhost:8080/ws 共用端口。
// 代码放在返回 error 的函数中，需导入 context、errors、net/http、os、os/signal、syscall、time，
// 以及 github.com/gin-gonic/gin 和 github.com/bpcoder16/pixiu/infra 下的 ginx、httpserver、websocketx：
//
//	router, err := ginx.New(ginx.Config{})
//	if err != nil {
//		return err
//	}
//	endpoint, err := websocketx.New(websocketx.Config{
//		OnMessage: func(ctx context.Context, session *websocketx.Session, message websocketx.Message) error {
//			return session.Send(ctx, message)
//		},
//	})
//	if err != nil {
//		return err
//	}
//	router.GET("/ping", func(c *gin.Context) {
//		c.String(http.StatusOK, "pong")
//	})
//	router.GET("/ws", gin.WrapH(endpoint))
//	server, err := httpserver.New(httpserver.Config{
//		Addr: ":8080",
//	}, router)
//	if err != nil {
//		endpoint.Cancel()
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
//	// 先让两个组件开始停收和排空，再用同一个期限等待。
//	wsStopErr := endpoint.Stop()
//	httpStopErr := server.Stop()
//	httpWaitErr := server.Wait(ctx)
//	wsWaitErr := endpoint.Wait(ctx)
//	if httpWaitErr != nil {
//		server.Cancel()
//	}
//	if wsWaitErr != nil {
//		endpoint.Cancel()
//	}
//	return errors.Join(runErr, wsStopErr, httpStopErr, httpWaitErr, wsWaitErr)
//
// HTTP 服务不负责回收已升级的 WebSocket 连接，因此两个组件需要分别 Stop/Wait，失败时分别 Cancel。
//
// # 限额与所有权
//
// 默认握手期限 5 秒；认证同步执行，与升级共用期限。回调必须配合 ctx，不得异步保存请求对象。
// Session.Context 不继承 HTTP 请求、HTTP Server 或应用信号的取消和可变日志作用域。
// OnOpen、每次 OnMessage、Revalidate 均获得独立的日志/计时作用域；业务应使用回调 ctx 调用下游。
// OnOpen 与 OnMessage 串行，Revalidate 可与消息处理并发，身份和业务共享状态必须并发安全。
//
// 默认收发单条上限各 1 MiB；接收 16 条/4 MiB、发送 64 条/4 MiB，包含在途操作。
// 默认共享 Budget 接收 128 MiB、发送 1 GiB；多个 Endpoint 只有显式注入同一个 Budget 才共享。
// Budget.Usage 和 Session.Usage 是瞬时逻辑占用，包含读取中预留空间，不等于 RSS。
// Budget.Limits 返回包含默认值的配置副本，供组合层判断过载恢复水位，不提供运行期改限额。
// MaxPendingHandshakes 默认 256，MaxConnections 默认 10000；挂起回调和待收尾连接仍占名额。
// 默认连接上限按 Endpoint 分别计数，多入口部署须自行分配连接上限并按需共享 Budget。
// 1 GiB 发送预算可覆盖一轮 10000×64 KiB（625 MiB）逻辑积压；同时多轮仍可能超额。
// 单条 1 MiB 上限不代表能同时为全部连接积压 1 MiB，预算和连接数须按部署资源显式配置。
// 默认每 20 秒 Ping、60 秒 Pong 期限、10 秒写入期限、1 秒网络关闭期限。
// DisableHeartbeat 可显式禁用心跳。MessageTimeout 默认 30 秒，排队时间计入；复核默认每 60 秒执行，期限 5 秒。
// 数值负值非法；CompressionLevel 零值默认 1，可设置 1–9，压缩仅在 EnableCompression 时协商。
//
// Send/SendPrepared 不等待网络或可用额度，成功只表示本地接纳。输入只在调用期间不可修改，返回后可复用。
// Prepare 创建不可变帧；同 Endpoint 的多个连接可并发 SendPrepared，每连接仍扣完整逻辑字节预算。
// ExpiresAt 为零时不过期；非零时接纳和实际写入前都检查。晚于入队的 ctx 取消不撤回消息。
// ErrQueueFull 表示本连接满，ErrBudgetExceeded 表示共享预算满；组件不自动重试或把它们混为慢客户端。
// OnMessage 的 Data 仅在回调期间只读有效，异步使用须自行复制并管理其工作生命周期。
//
// # 消息大小与性能建议
//
// 以高频消息的条数吞吐、CPU 和尾延迟为优先时，建议单条完整消息控制在 1 KiB（1024 字节）以内。
// 大小按编码后、解压后的消息正文计算：不含 WebSocket 帧头，使用 realtime 时须包含 JSON 协议字段。
// 该建议来自 M4、Go 1.27.3、双进程各 GOMAXPROCS=4、32 连接、无 TLS/压缩的接收实验，
// 同时比较原始传输与 realtime + MemoryBus，并提高请求限流以测量饱和吞吐。
// 本轮 1 KiB 分别保留扫描最高条数吞吐的约 99.8% 和 90.3%；它不是所有业务的全局最优值。
// 较大消息按业务需要评估 CPU、P99 和积压内存；默认 ReadLimit/MaxSendBytes 仍各为 1 MiB，
// realtime 默认完整消息上限仍为 64 KiB。
//
// # 停收与真实完成
//
// StopInput 仅供上层组合：停止新握手、上行数据和新复核，保留已接纳业务和输出。
// 停收后通过底层读取器跳过未接纳正文，继续处理控制帧；不再解压、校验正文或检查解压后大小。
// 已开始的读取可继续到本次返回，发现停收后释放其预算并跳过剩余正文；底层帧解析和线上限额仍保留。
// 通常直接调用 Stop：先停止输入，等存量业务真正返回，再封闭发送、排空并关闭连接。
// 单独使用 StopInput 必须受有限的整体关闭期限约束：上层排空后调用 Stop/Wait，超时或失败时 Cancel。
// 与 apprun 一致，各阶段共用关闭期限；Endpoint 不另设停收定时器，也不保证每个阶段都能在 Cancel 前完成。
// Wait 只等待真实收尾，超时不取消业务；调用方需要显式 Cancel。
// Cancel 不等待网络、业务或观察器；忽略取消的回调继续占用资源，Done 只在其实际返回后关闭。
// 收尾期间的已知过期、丢弃、写入和回调失败通过 DrainError 汇总；历史失败不污染当前关闭结果。
//
// Session.Close 发起单连接关闭并丢弃未写消息。Detached 表示连接已经不可路由，Done 表示全部收尾完成。
// OnDetach 在内部锁外同步执行，只能做快速内存索引清理；它可早于 OnOpen 返回。
// 网络回收不等待用户钩子，OnClose 等在途业务、读写和复核真实结束后运行一次。
// 所有回调都不能同步 Wait 自身或所属 Endpoint；Observe 应快速、并发安全，不能代替持久化业务审计。
// 组件日志/观察仅含元信息，回调错误和 panic 值不会原样进入组件日志。
// 关闭观察区分正常停机、传输断开、读取超时及协议错误；1006 仅作本地异常断开状态，不发送到网络。
// 回调自身超时保留 context.DeadlineExceeded，先发生的主动取消保留 context.Canceled。
// 1000 正常关闭后按时返回 nil 的回调仍算成功；Endpoint.Cancel、关闭故障及实际队列丢弃仍按失败处理。
//
// HTTP Shutdown 不回收已 Hijack 的连接。应用应先完成服务 Stop/Wait 或超时 Cancel，
// 然后按既有资源栈顺序回收共享客户端，最后关闭日志；不要在资源阶段再次无限等待 Endpoint。
package websocketx
