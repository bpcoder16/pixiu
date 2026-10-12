package ginauth

import "sync/atomic"

var defaultMiddleware atomic.Pointer[Middleware]

// SetDefault 在启动阶段安装默认中间件，m 为 nil 时 panic。
// 应在路由构造前调用；已绑定的路由不会随默认实例变化，不支持运行时热切换。
func SetDefault(m *Middleware) {
	if m == nil {
		panic("ginauth: nil default middleware")
	}
	defaultMiddleware.Store(m)
}

// Default 返回已安装的默认中间件，未调用 SetDefault 时 panic。
// New 只创建独立实例，不设置默认值。
func Default() *Middleware {
	m := defaultMiddleware.Load()
	if m == nil {
		panic("ginauth: default middleware not initialized")
	}
	return m
}
