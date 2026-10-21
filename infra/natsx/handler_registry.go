package natsx

import (
	"fmt"

	"github.com/bpcoder16/pixiu/infra/internal/handlerregistry"
)

// JetStreamHandlerRegistry 保存名称到 JetStream Handler 的映射，零值可用。
// 登记与装配快照可并发；首次使用后不得复制。登记不读配置或执行业务。
type JetStreamHandlerRegistry struct {
	handlers handlerregistry.Registry[JetStreamHandler]
}

// MustNewJetStreamHandlerRegistry 以原始函数名批量登记包级具名函数，区分大小写。
// nil 或重名会 panic；方法值、闭包和泛型函数须通过 MustRegister 显式命名。
func MustNewJetStreamHandlerRegistry(handlers ...JetStreamHandler) *JetStreamHandlerRegistry {
	registry := &JetStreamHandlerRegistry{}
	for _, handler := range handlers {
		registry.MustRegister(handler)
	}
	return registry
}

// MustRegister 登记 Handler，可选稳定名称最多一个；非法名称、nil 或重名会 panic。
// 自动名称随函数重命名变化；方法值、闭包和泛型函数必须显式命名。
func (r *JetStreamHandlerRegistry) MustRegister(handler JetStreamHandler, name ...string) {
	if err := r.handlers.Register(handler, "JetStream", name...); err != nil {
		panic(fmt.Errorf("natsx: %w", err))
	}
}

// Handlers 返回独立映射快照；nil 或空注册表返回空清单。
// 修改快照和后续登记互不影响；函数捕获的业务依赖仍由调用方管理。
func (r *JetStreamHandlerRegistry) Handlers() map[string]JetStreamHandler {
	if r == nil {
		return nil
	}
	return r.handlers.Snapshot()
}

// CoreHandlerRegistry 保存名称到 Core Handler 的映射，零值可用。
// 登记与装配快照可并发；首次使用后不得复制。与 JetStream 的名称空间独立。
type CoreHandlerRegistry struct {
	handlers handlerregistry.Registry[CoreHandler]
}

// MustNewCoreHandlerRegistry 以原始函数名批量登记包级具名函数，区分大小写。
// nil 或重名会 panic；方法值、闭包和泛型函数须通过 MustRegister 显式命名。
func MustNewCoreHandlerRegistry(handlers ...CoreHandler) *CoreHandlerRegistry {
	registry := &CoreHandlerRegistry{}
	for _, handler := range handlers {
		registry.MustRegister(handler)
	}
	return registry
}

// MustRegister 登记 Handler，可选稳定名称最多一个；非法名称、nil 或重名会 panic。
// 自动名称随函数重命名变化；方法值、闭包和泛型函数必须显式命名。
func (r *CoreHandlerRegistry) MustRegister(handler CoreHandler, name ...string) {
	if err := r.handlers.Register(handler, "Core", name...); err != nil {
		panic(fmt.Errorf("natsx: %w", err))
	}
}

// Handlers 返回独立映射快照；nil 或空注册表返回空清单。
// 修改快照和后续登记互不影响；函数捕获的业务依赖仍由调用方管理。
func (r *CoreHandlerRegistry) Handlers() map[string]CoreHandler {
	if r == nil {
		return nil
	}
	return r.handlers.Snapshot()
}
