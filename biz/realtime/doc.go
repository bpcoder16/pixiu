// Package realtime 组合 websocketx，提供静态 proto 类型绑定、JSON/PB 双协议、
// 同步/异步响应、本机用户/设备/组寻址和 Memory 在线广播。
// 业务负责认证授权、持久状态、任务队列、幂等及重连恢复。
//
// # 最小接入
//
// 业务请求、响应和事件均由 .proto 定义。下面用标准 protobuf 消息演示类型注册：
//
//	schema := realtime.NewSchema()
//	if err := schema.Request("profile.read", &emptypb.Empty{}, &wrapperspb.StringValue{}); err != nil {
//		return err
//	}
//	if err := schema.Event("profile.changed", &wrapperspb.StringValue{}); err != nil {
//		return err
//	}
//	router := realtime.NewRouter()
//	if err := router.Handle("profile.read", func(ctx context.Context, request *realtime.Request) (realtime.Result, error) {
//		return realtime.Reply(wrapperspb.String(request.Principal().UserID)), nil
//	}); err != nil {
//		return err
//	}
//	bus, err := realtime.NewMemoryBus(realtime.MemoryConfig{})
//	if err != nil {
//		return err
//	}
//	server, err := realtime.NewServer(realtime.Config{
//		Environment: "production",
//		Application: "shop",
//		Portal:      "browser",
//		Bus:         bus,
//		Schema:      schema,
//		Router:      router,
//		WebSocket: realtime.WebSocketConfig{
//			Authenticate: authenticate, // 业务验证凭据后返回不可变 Principal。
//		},
//	})
//	if err != nil {
//		return err
//	}
//	mux := http.NewServeMux()
//	mux.Handle("GET /ws", server)
//	// 由应用运行 server.Run，Status().Ready 为 true 后才可接入。
//	// HTTP 与 Server 各自管理生命周期；Server 已拥有内部 websocketx，无需重复登记。
//
// 构造成功才冻结 Schema/Router，可供多个 Server/Publisher 共享。Schema 只保存类型，
// 每个请求创建独立消息。Middleware 包装只在首次构造时编译，next 至多同步调用一次。
// Config.WebSocket.Transport 的内部回调、接纳门和子协议由 Server 管理，不允许覆盖。
//
// # 协议与请求
//
// 客户端握手选择 SubprotocolJSON 或 SubprotocolProtobuf，分别使用 Text 和 Binary；
// 双方同时支持两者时优先 JSON。同一连接不能切换编码。
// JSON 外层是公共信封，data 是业务消息的 ProtoJSON 值：
//
//	{"kind":"request","route":"profile.read","requestId":"r1","data":{}}
//
// PB 使用 protocol.Envelope，data 存放业务消息的 protobuf bytes。
// response 回显 requestId/route，event 不含 requestId；正文和 error 互斥。
// JSON 外层按精确键读取，拒绝重复协议字段；未知可选字段忽略。
// requestId 最长 128 字节，允许 ASCII 字母/数字/-/_；route 另允许点且保留 sys.*。
// 最大客户端消息 64 KiB、内部投递 128 KiB、解码递归深度 32；Limits 只能收紧。
// 业务 JSON 使用 protojson，包含 int64 字符串、bytes base64、枚举和 oneof 的标准语义。
//
// Handler 返回 Reply(message) 或 Deferred()；零值、nil、错类型及编码失败安全回退 INTERNAL。
// 只有合法 PublicError 可公开内容；普通错误固定脱敏。Handler panic/真实消息超时由
// websocketx 关闭连接，迟到结果不再发送。总并发默认 256，覆盖解码、执行和响应接纳；
// 等待使用原始 MessageTimeout，不新增 goroutine/执行池。请求令牌桶默认关闭，启用时
// RequestsPerSecond/RequestBurst 必须同时为正，超过速率返回 RATE_LIMITED。
//
// Deferred 仅在业务确认任务提交后使用：将 Request.ReplyRef() 和所需业务数据序列化到任务，
// worker 用 Publisher.Reply/ReplyError 回原连接。也可同步 Reply 表示受理，再 Push 事件；
// 业务负责选择，不为同一请求发送第二次 response。Server 不托管远程任务、不维护 pending 表。
// Request.Data 仅在回调期间只读，异步任务自行复制；不得保留 Request/Session 或继承请求期限。
//
// # 身份和寻址
//
// Scope 的 Application/Portal/Tenant 精确匹配，空 Tenant 是明确域。UserID/DeviceID 可选，
// 由认证决定；一用户可对应多连接。Device 按 (Scope, UserID, DeviceID) 精确索引，
// 空 UserID 只命中无用户的同名设备。ConnectionID 每次重连重新生成，身份不带内更换。
// Principal.Value 是业务维护的不可变授权快照，GetLocal/OnConnect 返回只读副本。
//
// OnConnect 在身份登记后、首条请求前运行，可在锁外调用 Join；失败/超时由 websocketx 摘除。
// Join/Leave 由业务授权后调用；组名不含库内业务规则。默认每连接 64 组、每 Server 总计
// 100000 条成员关系。断连清理所有索引；Leave 不撤回已选中/排队消息，立即撤权可 CloseLocal。
// CountLocal/CloseLocal 只针对本机；CloseLocal 发起 1008 关闭，不能据数量判断集群在线状态。
//
// # 推送与广播
//
// Publisher 只需要 Schema 和 Bus，不需要 Router/HTTP/Server。Push 指定事件 route 和消息；
// Reply/ReplyError 指定 ReplyRef，原连接失效后不重定向到同用户的新连接。
// Target 显式选择 Connection/User/Device/Users/Group/All，完整 Scope 必填；Users 去重最多 256。
// 所有发布只走一次 Bus，各节点只做本机分发。默认 topic 为
// rt.<environment>.<application>.<portal>.v1.events，TTL 默认 60 秒，范围 1 秒到 24 小时。
//
// 发布调用内完成编码，返回后可复用输入；成功表示 Bus 接纳，错误可能发生在提交之后，
// 不自动重试、不保证送达或离线重放。业务版本、事件幂等键由业务 proto 定义。
// 内部 PB 正文每条只解码一次，按实际命中连接各编码/Prepare 一次；同编码共用不可变帧。
// JSON 展开超限只拒绝对应连接，不向它发送 PB。各连接仍扣完整逻辑发送预算。
//
// 分发和 Memory 每订阅 pending 默认各 1024 项/16 MiB，包含当前工作项。单连接慢仅关闭
// 受影响连接；共享过载撤销 Ready，低于 75% 水位持续 1 秒后恢复。已知目标丢失只影响目标；
// 订阅 GapCount 表示连续性不确定时关闭当时旧连接 1013，重连后业务重新授权并拉取快照。
// 非法总线输入只记录协议错误。Observe 须快速且并发安全，不等待所属 Server；日志不含正文。
//
// # 生命周期
//
// NewServer 无后台工作；Run 一次，订阅建立期限默认 10 秒。Stop 幂等且非阻塞，可先于 Run。
// Stop 先停新请求/握手/Join 和订阅输入，保留已接纳请求的 context；订阅真实完成后封闭
// 并排空分发，最后 Endpoint 排空请求及网络发送。Run 返回不是完成，Done 才表示真实结束。
// Wait(ctx) 只等待，超时不取消工作；Cancel 通知订阅、分发与连接取消，忽略取消的回调仍占
// 预算并延迟 Done。排空期已知损失汇总到 Wait。Server 只关闭自己的订阅，绝不关闭共享 Bus。
//
// 应用在同一总期限下停止/等待 HTTP 与 Server；失败时 Cancel，再回收共享资源，日志最后关。
// 第一阶段只保证本机已接纳工作，不等待远程异步任务；订阅停收后完成的结果可能错过旧连接，
// 由业务持久状态和重连查询恢复。目前仅实现 Memory；Redis/NATS 和 bootstrap 装配分阶段交付。
//
// 公共与内部 PB 的固定工具版本及再生成命令见 generate.sh。
package realtime

//go:generate sh generate.sh
