// Package natsx 提供实例式 NATS 连接、通用消息操作和 logit 结果日志。
// 操作结果成功写 Info，失败写 Error。
// Publish、Request、PublishJetStream 在 ctx 启用 logit.WithStart 时登记请求级耗时，
// 字段为 NATS_<连接名>_<序号>_duration_ms；日志被过滤时仍登记，供 InfoDuration 等汇总。
// 耗时不包含日志构建或写入，消费回调不计入下游调用。
// 操作日志记录完整正文及 Header，不截断或脱敏；Request 同时记录收到的响应。
// 正文按 UTF-8 字符串输出，非 UTF-8 数据使用 Base64，并记录对应的编码。
// 业务 Stream/Consumer 配置、ACK 和重试策略由调用方决定；详见 docs/natsx-design.md。
// 第一版原样记录错误文本并保留原始错误链，不脱敏其中可能包含的 URL、密码或 Token。
//
// 应用启动时创建连接并完成 Subscribe/QueueSubscribe 注册和 ConsumeWithWorkers 启动；运行期间保持消费关系固定。
// 关闭时先停止并等待外部生产者，再通过 Client.Close 统一排空并关闭连接，最后关闭日志。
// 已建立连接意外断开后默认持续重连，心跳间隔默认 20 秒，重连发送缓冲默认 16 MiB；
// 调用方可通过 nats.MaxReconnects、nats.PingInterval、nats.ReconnectBufSize 等官方选项覆盖。
// 首次连接失败仍直接返回错误；重连成功后由官方客户端恢复原有订阅。
// DrainTimeout、CloseTimeout 的零值默认保持 10 秒、20 秒，由调用方通过 Config 按需设置。
// DrainTimeout 仅用于底层 NATS 连接 drain；消费排空、异步确认等待和最终连接关闭
// 共用 CloseTimeout 整体期限，各阶段不重新计时。
// 关闭完成后的连接 LastError 仅对 ErrDrainTimeout、ErrTimeout 记录日志，不作为返回错误。
// 发起连接 Drain 失败及整体关闭超时仍返回错误。
// Config.JetStream 使用 JetStreamConfig，目前仅开放 PublishAsyncTimeout：
// 零值默认 5 秒，正值覆盖，负值在拨号前报错；仅限制默认 JetStream 上下文的异步发布确认等待。
// 确认超时通过 future.Err() 和 Pixiu 错误日志报告，不等于消息未写入服务端。
// 以下为仅发布消息的调用样例：
//
//	client, err := natsx.New(natsx.Config{
//	    Name: "orders",
//	    URLs: []string{"nats://127.0.0.1:4222"},
//	    Username: username,
//	    Password: password,
//	})
//	if err != nil {
//	    return err
//	}
//	defer client.Close()
//	ctx = logit.WithContextLogID(ctx) // 服务入口生成一次，后续发布直接复用 ctx
//	return client.Publish(ctx, "orders.created", payload)
//
// 上例需导入 github.com/bpcoder16/pixiu/infra/natsx 和 github.com/bpcoder16/pixiu/logit。
// 三个发送方法的 Header 均可省略或传入一个（可为 nil），多传返回参数错误；data 允许 nil。
// PublishJetStream 在 data 后要求非空 messageID，再接收可选 Header，不开放 jetstream.PublishOpt。
// 例如：ack, err := client.PublishJetStream(ctx, "orders.created", payload, eventID)。
// messageID 不自动生成，覆盖 Header 的 Nats-Msg-Id；同一事件重试时复用，不同事件使用不同 ID。
// 发布去重仅在同一 Stream 的去重窗口内生效，不代表消费者业务处理只执行一次。
// Publish、Request、PublishJetStream 从 meta 读取 logit.LogId，覆盖消息的 X-Log-Id；
// 缺失或为空时为消息 Header 生成新 ID，不回写调用方 context。
// 操作日志只使用 ctx 已有的 logID，不额外补写或覆盖；需关联时应在入口初始化。
// 出入站完全信任已有的非空 ID，不校验长度或字符，合法性由写入方保证。
// 多次发布需要关联时，应在入口初始化 ID。
// 每次调用期间 data 和 header 必须独占，不得被其他 goroutine 并发读取、修改或用于另一次发布。
// 方法内部创建消息，直接使用 data 和 header，不复制；header 为 nil 时内部创建，非 nil 时原地写入。
// 追踪 ID 和 JetStream messageID 写入的字段在返回后保留，后续发布失败也不回滚。
// 出站追踪仅覆盖 X-Log-Id，其他大小写名称作为普通 Header 原样保留。
// 消费端精确读取 X-Log-Id（区分大小写），在独立日志作用域还原 ID，
// 可通过 logit.LogIDFromContext 读取。
// 服务器地址只能通过 Config.URLs 提供；选项中的 Url 非空或 Servers 长度非零时在拨号前报错。
// DisconnectedErrCB、DisconnectedCB、ReconnectedCB、ClosedCB、AsyncErrorCB 由 Pixiu 管理；
// 选项中任一字段非 nil 时在拨号前报错，不保留或串联用户连接回调。
// 取得 Conn() 后也不得覆盖这些回调；可通过原生 StatusChanged 观察连接状态。
// username、password 由应用读取配置后注入，允许空密码；只有密码而没有用户名时无效。
// Config 仅直接提供用户名/密码认证，两个字段均可不填。
// nats.UserInfo 及自定义选项设置固定用户名/密码会在连接前报错，应通过 Config 提供。
// 显式填写认证字段时，不能混用官方认证选项或 URL 内嵌凭据；TLS 选项仍可组合使用。
// 两个认证字段均为空时，支持 nats.UserInfoHandler 动态获取用户名/密码，
// 并继续支持 nats.Token、nats.UserCredentials 等其他认证选项
// 及 URL 认证；所有入口都未提供凭据时才不认证。配置凭据不裁剪空格；原始错误可能包含凭据。
//
// 启动阶段的边界由应用负责；模块不提供 Start，也不在运行期自动拒绝注册。
// Subscribe(subject, handler) 创建普通订阅，每个匹配的订阅者都会收到消息。
// QueueSubscribe(subject, queue, handler) 创建队列订阅，queue 必须非空，同组内每条消息只交给一个订阅者。
// Subscribe、QueueSubscribe、ConsumeWithWorkers 只应在启动阶段调用，不能从消息回调中追加注册。
// 调用成功后即可接收消息，handler 依赖的资源应预先就绪。
// New、Subscribe、QueueSubscribe、ConsumeWithWorkers 均不接收外部 ctx，生命周期由 Client.Close 管理。
// Core 原生句柄可 Drain/Unsubscribe；worker 消费句柄可 Drain/Stop，Closed 等待全部 worker 完成。
// 提前停止的句柄保留到 Client.Close 时统一处理。
// 每条消息获得独立日志作用域的 ctx，由 Client 在关闭完成或超时后取消；
// Close 排空期间 ctx 保持有效，仍允许回调发布和请求；回调内不得调用 Close。
// Publish、Request、PublishJetStream 仍接收调用方 ctx，回调内可按需派生 WithTimeout。
// ConsumeWithWorkers 使用 Messages 迭代器和固定 worker；ConsumeConfig.Workers 必须为正，
// MaxMessages 为零时等于 Workers；不开放完整拉取选项，其余使用 SDK 默认值。
// ErrorHandler 在运行错误日志之后执行；心跳丢失后继续拉取，其他迭代器错误终止消费。
// 不自动 ACK/NAK/Term，多 worker 不保证业务完成顺序。
// ConsumeWithWorkers 的完整样例展示 Stream/Consumer 创建、显式 ACK、错误回调和关闭顺序。
//
// 连接失败可用 errors.Is(err, natsx.ErrConnect) 判断，同时保留原始错误链，
// 可继续使用 errors.Is 和 errors.AsType 判断底层错误；网络超时不转换为 nats.ErrTimeout。
package natsx
