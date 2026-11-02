// Package realtime 基于 websocketx 提供连接身份、独立业务状态和 JSON/PB 双向消息。
// 首期通过统一 OnMessage 回调处理请求；场景路由和 Redis/NATS 推送来源尚未实现。
//
// # 构造与消息处理
//
// 业务消息由业务项目的 .proto 定义。下例使用标准 protobuf 消息展示公开 API，
// 需导入 context、net/http、realtime 和 wrapperspb；代码放在返回 error 的函数中：
//
//	server, err := realtime.NewServer(realtime.Config{
//		Authenticate: func(ctx context.Context, r *http.Request) (realtime.ClientInfo, error) {
//			// 示例显式允许匿名；生产业务应校验凭据并返回用户、设备等可信信息。
//			return realtime.ClientInfo{}, nil
//		},
//		OnMessage: func(ctx context.Context, session *realtime.Session, message *realtime.Message) error {
//			var request wrapperspb.StringValue
//			if err := message.Decode(&request); err != nil {
//				return session.ReplyError(ctx, message, "INVALID_ARGUMENT", "请求参数错误")
//			}
//			return session.Reply(ctx, message, wrapperspb.String("hello "+request.Value))
//		},
//	})
//	if err != nil {
//		return err
//	}
//	// Server 同时作为 HTTP Handler 和常驻服务；两者的生命周期分别管理。
//	mux := http.NewServeMux()
//	mux.Handle("GET /ws", server)
//	// 应用运行 server.Run，并由 HTTP 服务监听 mux；Ready 为 true 后接受连接。
//	// 使用 bootstrap 时，显式将 Server 的生命周期方法填入 apprun.Service，与 HTTP 服务分别登记。
//
// Authenticate、OnMessage 必填；NewServer 无后台任务。Run 一次并阻塞到真实收尾，
// Run 前或 Stop 后拒绝握手。HTTP 关闭不自动关闭已升级连接。
// 独立端口可用 bootstrap.NewWebSocketService(name, factory) 一并管理 HTTP 与 realtime，
// factory 返回全新 Server，调用方不得再单独运行或登记内部 Server；各端使用不同名称和配置文件。
// 关闭时 Stop 全部服务，再以同一总期限 Wait；失败时 Cancel，共享资源随后回收，日志最后关闭。
// Stop 不取消已经接纳的业务，Wait 超时只结束等待，Done 只在回调和网络实际结束后关闭。
//
// # 协议
//
// 握手提供 SubprotocolJSON 或 SubprotocolProtobuf，分别使用 Text/Binary；
// 同时提供时优先 JSON。无协议或无交集拒绝升级，同连接上下行编码一致且不可切换。
// JSON 请求示例，data 是 wrapperspb.StringValue 的 ProtoJSON 标量：
//
//	{"kind":"request","requestId":"r1","scene":"home","action":"hello","data":"world"}
//
// PB 使用 protocol.Envelope，data 为业务 proto.Marshal 字节。JSON 信封的 data
// 直接嵌入 ProtoJSON 值，不能将整个 protobuf 正文改为 base64；业务 bytes 字段本身仍
// 遵循 base64、int64/uint64 字段遵循字符串等标准 ProtoJSON 规则。业务 JSON 未知字段报错。
// requestId、scene、action 各为 1–128 字节，允许 ASCII 字母/数字/点/下划线/短横线。
// JSON 信封字段名精确匹配，未知、重复、缺失字段及尾随内容均拒绝，正文容器深度最多 32。
// PB 请求信封拒绝顶层 group 和 error 字段，跳过普通未知字段。错误帧类型关闭 1003，非法信封关闭 1007。
// Message.Decode 对 ProtoJSON 和 PB 正文采用原生 RecursionLimit=32；PB 正文中的未知 group 沿用 protobuf 自身的跳过规则。
// Send/Reply 按完整帧检查 MaxSendBytes，超长返回 websocketx.ErrMessageTooLarge；
// 大小合规后，JSON 正文容器超过 32 层返回 ErrInvalidMessage。接收方在解码时执行自己的递归限制。
// 业务 Decode 错误由 Handler 显式处理；普通 Handler error 只脱敏观察，不自动回复或断连。
// panic/超时由 websocketx 处理。ReplyError 内容由业务授权公开，不应传入内部错误原文。
//
// Message 和其正文只在 OnMessage 回调期间有效，每次 Decode 使用独立可写对象。
// Reply/ReplyError 检查所属 Session，并在入队时检查原请求与调用方的 ctx；回调结束或超时后拒绝迟到回复。
// 每个请求的回复次数由业务保证，库不维护 pending 表、响应去重或远端任务句柄。
//
// # 身份与业务状态
//
// ClientInfo 在认证后固定，Info 返回值副本；其中 Value 的深层对象必须保持不可变，
// 不能携带原始凭据、HTTP 请求、Gin Context 或可变共享状态。身份变化通过重新连接完成。
// 同用户/设备可有多个独立连接，ID 是连接 ID，不是 DeviceID，不自动踢出或覆盖。
//
// NewState 为每个连接返回独立的业务对象，State 返回对象本身，不做深拷贝或自动加锁。
// 工厂仅做快速内存初始化；业务自定义结构负责并发安全，可在 OnClose 中释放资源。
// OnOpen 与 OnMessage 同连接串行，Revalidate 基于固定身份并发执行。
// OnClose 等待网络和在途回调完成，OnOpen 失败也会清理已发布的业务会话；
// 初始化前断开或 NewState panic 时可能不创建业务会话，因此不调用业务 OnClose。
// 清理 I/O 使用 OnClose 的 ctx，不使用已取消的 Session.Context；它独立于断连取消，
// 受 Transport.OnCloseTimeout（默认 5 秒）及 Server.Cancel 约束，Stop/Wait 超时不取消它。
// 回调必须配合取消，Session.Done/Server.Done 等待实际返回；停收期间清理失败计入 ErrDrainFailed。
// 每个业务回调使用自己的 ctx；不可同步等待自身或所属 Server.Done。
//
// # 主动下发与容量
//
// 业务持有当前 Session 时可以直接下发事件，以下代码放在返回 error 的函数中：
//
//	return session.Send(ctx, realtime.Outgoing{
//		Kind:   realtime.Event,
//		Scene:  "home",
//		Action: "changed",
//		Data:   wrapperspb.String("new value"),
//	})
//
// Send 并发安全，调用内完成编码，返回后可复用输入；调用期间不得修改输入。
// 成功表示本地队列接纳，不代表网络写完或客户端收到；不自动重试。
// 队列满、共享预算不足、过期、关闭等错误保留 websocketx 的 errors.Is 语义。
// ExpiresAt 可限制消息入队和实际写出的有效期，ctx 在入队后取消不会撤回消息。
// Transport.ReadLimit/MaxSendBytes 零值各默认 64 KiB，按含信封的完整消息计量；
// 其他心跳、连接数、队列和共享 Budget 默认值沿用 websocketx。
// Transport 的接纳门、认证、子协议与生命周期回调由 Server 管理，误填返回构造错误。
// Observe/Logger 及容量参数可以注入。Server 不关闭借用的日志或业务共享资源。
package realtime

//go:generate sh generate.sh
