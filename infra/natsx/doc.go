// Package natsx 提供实例式 NATS 连接、启动期消费注册、通用消息操作和 logit 结果日志。
// 操作结果成功写 Info，失败写 Error。
// Publish、Request、PublishJetStream、PublishJetStreamAsync 在 ctx 启用 logit.WithStart 时登记请求级耗时，
// 字段为 NATS_<连接名>_<序号>_duration_ms；日志被过滤时仍登记，供 InfoDuration 等汇总。
// 耗时不包含日志构建或写入，消费回调不计入下游调用。
// 操作日志记录完整正文及 Header，不截断或脱敏；Request 同时记录收到的响应。
// 正文按 UTF-8 字符串输出，非 UTF-8 数据使用 Base64，并记录对应的编码。
// Stream/Consumer 使用官方配置；JetStream 消费统一通过 RegisterJetStreamConsumers 注册并自动确认。
// 详细契约见 docs/natsx-design.md。
// 第一版原样记录错误文本并保留原始错误链，不脱敏其中可能包含的 URL、密码或 Token。
//
// 共享客户端可在启动阶段使用 NewDefault/NewNamed 创建，由 Default/Named 获取。
// 默认引用与同名实例相同；独立 New 不登记到注册表，仍由调用方显式关闭。
// 以下为共享客户端的发布样例（需导入 context 和 natsx）：
//
//	client, err := natsx.NewDefault(natsx.Config{
//		Name: "events",
//		URLs: []string{"nats://127.0.0.1:4222"},
//	})
//	if err != nil {
//		return err
//	}
//	defer natsx.CloseAll()
//	return client.Publish(context.Background(), "events.created", []byte("hello"))
//
// NewDefault/NewNamed 只在启动阶段串行调用，不能与 CloseAll 并发；失败不占用名称。
// 未初始化、名称不存在或 CloseAll 开始后查询会 panic。CloseAll 只关闭已登记实例，
// 默认实例只关闭一次，并汇总错误；重复调用返回同一次结果。
// 关闭前停止并等待外部生产者，托管消费由 Client.Close 排空；消费者及其发布服务须在
// 启动时持有客户端引用，排空期间不能再次查询 Default/Named。数据库等依赖和日志晚于 NATS 关闭。
// 注册表不保证多实例关闭顺序，各实例使用独立的 CloseTimeout，不支持关闭期跨实例依赖。
//
// 应用启动时在依赖就绪后创建并持有客户端，依次调用 RegisterStreams、
// RegisterJetStreamConsumers 和 RegisterCoreConsumers；运行期间保持消费关系固定。
// 关闭时先停止并等待外部生产者，再通过 Client.Close 统一排空并关闭连接，最后关闭日志。
// Close 拒绝新增注册，排空消费后拒绝新发布与请求，再等待异步发布结果并 drain 连接。
// 排空期间 handler ctx 保持有效，可以继续发布和完成 ACK；Close 不汇总单条发布错误。
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
// Publish、Request、PublishJetStream 的 Header 均可省略或传入一个（可为 nil），多传返回参数错误；data 允许 nil。
// PublishJetStream 在 data 后要求非空 messageID，再接收可选 Header，不开放 jetstream.PublishOpt。
// 例如：ack, err := client.PublishJetStream(ctx, "orders.created", payload, eventID)。
// messageID 不自动生成，覆盖消息 Header 副本的 Nats-Msg-Id；同一事件重试时复用，不同事件使用不同 ID。
// 发布去重仅在同一 Stream 的去重窗口内生效，不代表消费者业务处理只执行一次。
// Publish、Request、PublishJetStream 从 meta 读取 logit.LogId，覆盖消息的 X-Log-Id；
// 缺失或为空时为消息 Header 生成新 ID，不回写调用方 context。
// 操作日志只使用 ctx 已有的 logID，不额外补写或覆盖；需关联时应在入口初始化。
// 出入站完全信任已有的非空 ID，不校验长度或字符，合法性由写入方保证。
// 多次发布需要关联时，应在入口初始化 ID。
// Core Publish/Request 调用期间 data 和 header 必须独占，不得被其他 goroutine 并发读取、修改或用于另一次发布。
// 这两个方法直接使用 data 和 header，不复制；header 为 nil 时内部创建，非 nil 时原地写入追踪 ID，失败也不回滚。
// JetStream 同步和异步发布均深拷贝 Header，仅在副本中写入追踪 ID 和消息 ID，不修改调用方 Header。
// 调用期间 Header 不得并发修改，可被其他发布调用只读共享；同步发布直接使用 data，调用期间 data 须独占。
// 出站追踪仅覆盖 X-Log-Id，其他大小写名称作为普通 Header 原样保留。
// 消费端精确读取 X-Log-Id（区分大小写），在独立日志作用域还原 ID，
// 可通过 logit.LogIDFromContext 读取。
// 服务器地址只能通过 Config.URLs 提供；选项中的 Url 非空或 Servers 长度非零时在拨号前报错。
// URLs 至少包含一个地址，每项必须非空且无首尾空白；不自动裁剪，非法值在拨号前报错。
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
// Core 消费统一通过 RegisterCoreConsumers 注册，Queue 为空时广播，非空时同组竞争消费。
// Subscribers 仅在 Queue 非空时控制当前 Client 的同组订阅数，零值默认 1，负值无效；
// Queue 为空时忽略该数量，固定创建一个广播订阅。
// 单个订阅内串行处理，多个订阅间可并发且不保证处理顺序，handler 须支持并发调用。
// 全空白队列名无效；handler 返回 error，成功时返回 nil。
// 集中注册入口只应在启动阶段调用，不能从消息回调中追加注册。
// 注册返回前可能接收消息，handler 依赖的资源应预先就绪。
// New 不接收外部 ctx；注册入口的 ctx 仅约束准备阶段，消费生命周期由 Client.Close 管理。
// 托管订阅和消费均由 Client.Close 统一排空；消费注册仅返回启动错误。
// 直接关闭、排空原生连接后，不保证封装层的优雅排空及在途回调等待。
// 每条消息获得独立日志作用域的 ctx，由 Client 在关闭完成或超时后取消；
// Close 排空期间 ctx 保持有效，仍允许回调发布和请求；回调内不得调用 Close。
// Publish、Request、PublishJetStream 仍接收调用方 ctx，回调内可按需派生 WithTimeout。
// RegisterJetStreamConsumers 使用 Messages 迭代器和固定 worker；ConsumeConfig.Workers 零值默认 1，负值无效，
// MaxMessages 为零时等于有效 Workers 的两倍；不开放完整拉取选项。
// 拉取期限固定使用 jetstream.DefaultExpires（当前为 30 秒）。注册时校验声明及服务端有效配置：
// MaxRequestExpires 为零或不低于默认期限时允许，正值低于默认期限时在创建迭代器前报错。
// MaxRequestBatch 为零或不低于有效 MaxMessages 时允许，超限时在创建迭代器前报错，不自动缩小预取数量。
// 调用方须在运行期间保持相关限制不变。
// 其余拉取参数由 SDK 按默认规则设置。
// ErrorHandler 在运行错误日志之后执行；心跳丢失后继续拉取，其他迭代器错误终止消费。
// 多 worker 不保证业务完成顺序，确认和重试由组件按 handler 的返回结果管理。
//
// 连接失败可用 errors.Is(err, natsx.ErrConnect) 判断，同时保留原始错误链，
// 可继续使用 errors.Is 和 errors.AsType 判断底层错误；网络超时不转换为 nats.ErrTimeout。
//
// 集中注册只在业务依赖就绪后调用，无需额外 Runtime 或 Start：
//
//	setupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
//	defer cancel()
//	streamCfg := natsx.DefaultStreamConfig("TASKS", 1, "tasks.sms")
//	streamCfg.Retention = jetstream.WorkQueuePolicy
//	if err := client.RegisterStreams(setupCtx, streamCfg); err != nil {
//		return err
//	}
//	handler := func(ctx context.Context, msg jetstream.Msg) error {
//		return sendSMS(ctx, msg.Data())
//	}
//	consumer := natsx.DefaultJetStreamConsumer("TASKS", "send-sms", handler, "tasks.sms")
//	consumer.Consume.Workers = 4
//	return client.RegisterJetStreamConsumers(setupCtx, consumer)
//
// 上例的 client 已初始化且已安排 Close，sendSMS 是业务函数；还需导入 time 与官方 jetstream 包。
// DefaultStreamConfig 返回可直接修改的原生配置，replicas 原样写入副本数，复制传入的 subjects；校验由注册入口和 SDK 承担。
// 默认 LimitsPolicy、FileStorage、MaxBytes=5 GiB、MaxMsgSize=512 KiB、DiscardNew、MaxAge=90 天（约 3 个月）、Duplicates=2 分钟、NoAck=false，
// 其余字段沿用 SDK 默认。注册前可直接覆盖字段，例如 streamCfg.MaxBytes = 2 << 30。
// MaxMsgSize 限制 Header 与正文的总大小，须为组件追加的追踪 ID、消息 ID 等 Header 预留空间。
// LimitsPolicy 下 ACK 不释放容量；默认满额拒绝新消息，空间释放后可继续发布，MaxAge 到期也会删除未 ACK 的消息。
// 需要满额淘汰最老消息时，可在注册前设置 streamCfg.Discard = jetstream.DiscardOld。
// DefaultJetStreamConsumer 返回可修改的消费声明，默认显式确认、AckWait=60s、MaxDeliver=5（含首次投递），
// 显式设置 MaxAckPending=1000，限制同一 Durable 下所有实例合计的未确认消息数。
// MaxRequestBatch 和 MaxRequestExpires 保留零值，不额外指定拉取批量或期限上限。
// BackOff 为空、RetryDelay 为零；可通过 consumer.Config 覆盖 AckWait、MaxDeliver、MaxAckPending 或设置 BackOff。
// handler 后可传入多个过滤主题，复制到 FilterSubjects，要求 NATS Server >= 2.10；不传主题时消费整个 Stream。
// 构造函数不设置 FilterSubject；调用方后续设置该字段时须清空 FilterSubjects，两者不能同时使用。
// Workers 默认设为 1；MaxMessages 保留零值，注册时为有效 Workers 的两倍。
// 默认构造函数不校验参数；手写消费声明仍须显式配置确认超时。
// RegisterJetStreamConsumers 仅支持 Pull Consumer，Config.DeliverSubject 非空时在创建本批任何远端 Consumer 前报错。
// RegisterStreams/RegisterJetStreamConsumers 使用 SDK 创建语义：同配置复用、冲突返回错误，不更新或删除已有资源。
// 已有使用旧默认 AckWait=5s 的 Durable，采用新默认值前须显式更新远端配置。
// RegisterStreams、RegisterJetStreamConsumers 和 RegisterCoreConsumers 分别计数，通过入口检查后每个 Client 各只允许调用一次；
// 须一次传入该类的全部配置，空清单或失败也不可重试，各 Client 互不影响。
// 三个 Register 入口仅在启动阶段串行调用，不与 Close 并发；入口检查初始状态，
// ctx 仅传给资源 RPC 和有 deadline 时的 Core Flush，由 SDK 处理取消和连接错误。
// 不额外检查 ctx.Err()；本地消费启动不受启动 ctx 取消限制，全部操作成功即返回 nil。
// 注册可能在返回前执行 handler；失败返回后须中止启动并 Close，不在同一客户端重试整批注册，
// 本地已执行工作和远端资源不回滚。
// 不同 Client 可共享同一 Durable 分摊消息，服务重启可复用其进度。
// 每个 Consumer 必须显式配置正值 AckWait 或非空 BackOff，至少一项；两者同时配置时 BackOff 优先。
// 无论 RetryDelay 是否为正，都在准备本批远端 Consumer 前检查确认超时配置。
// 新注册入口默认一个 worker，handler 返回 nil 时 ACK，错误或 panic 且尚有投递次数时按 RetryDelay 决定重投方式；
// 失败且达到 MaxDeliver 后记录 exhausted，不 ACK/Term/主动 NAK，不自动删除或转入死信队列；
// MaxDeliver 包含首次投递，例如 3 表示最多投递 3 次。关闭取消后不再发起确认。
// handler 必须在返回前完成本条业务并支持配置的并发度，不得自行 ACK/NAK/Term 或调用 Close。
// RetryDelay 为正时发送指定延迟的 NAK；零值不主动确认，等待服务端按 BackOff 重投，
// 未配置 BackOff 时使用 AckWait；负值无效，非空 BackOff 与正值 RetryDelay 冲突，注册前报错。
// AckWait（或覆盖它的 BackOff）从消息投递时开始计时，应覆盖预取等待和业务处理时间，
// 不自动 InProgress，业务仍须幂等。
// 注册入口的 handler panic 被恢复并按业务失败处理。
//
// Core 通过 RegisterCoreConsumers(setupCtx, CoreConsumer{...}) 集中注册：
// Name 必填且不能全为空白，同批名称按原值判重；不同 Name 可使用相同 Subject 和 Queue，不同 Client 可使用相同 Name。
// Queue 为空时广播，否则竞争消费；Handler 返回错误只记日志，不重投，使用 Msg.Respond 显式应答。
// DefaultCoreConsumer(name, subject, handler) 构建广播声明，Subscribers 显式设为 1。
// DefaultCoreQueueConsumer(name, subject, queue, subscribers, handler) 构建同组声明，subscribers 原样写入，注册时零值默认 1。
// 两者只构建可修改的声明，不建立连接或校验参数；pending 保留零值以沿用 SDK 默认。
// 以下同时注册广播及四个同组订阅；handleBroadcast 和 handleQueue 是业务提供的 CoreHandler，后者须支持并发调用：
//
//	broadcast := natsx.DefaultCoreConsumer("event-listener", "events.created", handleBroadcast)
//	queue := natsx.DefaultCoreQueueConsumer("task-worker", "tasks.created", "task-workers", 4, handleQueue)
//	return client.RegisterCoreConsumers(context.Background(), broadcast, queue)
//
// setupCtx 可使用 context.Background()，全部注册后仅 Flush 一次；有 deadline 时使用 FlushWithContext，
// 无 deadline 时使用 SDK 默认 Flush（当前等待上限 10 秒），此时取消 ctx 不会中断正在执行的 Flush。
// 不另设整体注册期限；Flush 不保证逐个订阅授权成功。
// PendingMessages/PendingBytes 分别限制每个订阅的积压，为零沿用 SDK 默认，正值覆盖；
// 建立订阅后逐个设置，有默认限额窗口，总缓冲容量随订阅数增加。
// 两种注册的启动 ctx 均不传入长期 handler，生命周期、日志作用域和关闭顺序复用 Client。
//
// PublishJetStreamAsync 的参数与同步方法相同，要求非空 messageID，返回原生 PubAckFuture。
// 同一事件重试复用 ID，不同事件使用不同 ID；去重仍受同一 Stream 的服务端窗口限制。
// 异步方法复制 data/Header，不修改原始输入，调用返回后即可复用；不得修改 future.Msg()。
// ctx 只用于提交前取消检查、日志与追踪，不取消已提交消息，也不控制后续确认等待。
// 先检查方法返回的提交 error，再检查 future.Ok()/Err()；异步错误同时记录日志。
// 提交日志及耗时不表示持久化成功，PubAck 也不代表消费者业务完成。
// PublishAsyncTimeout 默认 5 秒，与消费者 AckWait 无关；确认超时不保证消息未写入。
// Close 排空消费后等待异步结果；全部完成不等于全部成功。
// 整体关闭超时时，清理默认发布者并以 jetstream.ErrJetStreamPublisherClosed 结束剩余 future；
// Close 自身仍返回 ErrCloseTimeout，发布者关闭错误不保证消息未写入服务端。
package natsx
