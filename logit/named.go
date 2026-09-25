package logit

import "sync"

// 命名 logger 注册表:多个 Logger 实例分流到不同文件/配置(如 request、cron),
// 核心零特殊逻辑——只是"带名字的便捷获取",可用于 cron 等独立日志目标。
var namedLoggers sync.Map // name → Logger

// SetNamed 注册命名 Logger(应用启动期调用)，nil Logger 会 panic。
func SetNamed(name string, l Logger) {
	if isNilInterface(l) {
		panic("logit: nil logger")
	}
	namedLoggers.Store(name, l)
}

// Named 返回命名 Logger,未注册时退回全局默认。
func Named(name string) Logger {
	if v, ok := namedLoggers.Load(name); ok {
		return v.(Logger)
	}
	return Default()
}
