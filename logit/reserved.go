package logit

// 内置日志字段统一在此声明，业务字段不得使用这些键。
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
