package logit

// 内置日志字段由编码器生成，调用方不得使用这些键。
const (
	levelKey  = "level"
	tsKey     = "ts"
	callerKey = "caller"
	msgKey    = "msg"
)

func rejectReservedField(key string) {
	switch key {
	case levelKey, tsKey, callerKey, msgKey:
		panic("logit: reserved field " + key)
	}
}
