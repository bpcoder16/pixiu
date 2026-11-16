package realtime

// 通用下行错误码保持稳定，供 Error.Code 和 ReplyError 复用。
// 业务可使用自定义错误码和公开文案；这些常量不自动映射 Go error。
const (
	// CodeInvalidArgument 表示请求参数非法或不满足业务约束。
	CodeInvalidArgument = "INVALID_ARGUMENT"
	// CodeNotFound 表示请求的 scene/action 没有对应处理器。
	CodeNotFound = "NOT_FOUND"
	// CodeUnavailable 表示服务暂时无法完成请求，不向客户端暴露内部错误。
	CodeUnavailable = "UNAVAILABLE"
	// CodeTopicLimitExceeded 表示当前连接跨 namespace 的订阅总数超限。
	CodeTopicLimitExceeded = "TOPIC_LIMIT_EXCEEDED"
)
