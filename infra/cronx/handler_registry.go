package cronx

import (
	"fmt"

	"github.com/bpcoder16/pixiu/infra/internal/handlerregistry"
)

// NamedHandler 将业务函数绑定到稳定名称，不包含调度或配置文件信息。
type NamedHandler struct {
	Name    string
	Handler Handler
}

// HandlerRegistry 收集由应用持有的任务声明，不读配置或执行业务；零值可用。
// MustRegister 与 Handlers 可并发调用，首次使用后不得复制注册器。
type HandlerRegistry struct {
	handlers handlerregistry.Registry[Handler]
}

// MustNewHandlerRegistry 批量登记包级具名函数，默认名称为原始函数名，区分大小写。
// 非法 Handler 或名称冲突会 panic；闭包、方法值和泛型函数须改用 MustRegister 显式命名。
func MustNewHandlerRegistry(handlers ...Handler) *HandlerRegistry {
	registry := &HandlerRegistry{}
	for _, handler := range handlers {
		registry.MustRegister(handler)
	}
	return registry
}

// MustRegister 追加任务声明；可选 name 最多一个，提供时按原值作为稳定名称。
// 未提供名称时，只为包级具名函数自动取名并保留大小写；名称为空、含首尾空白、重复或 Handler 为 nil 均 panic。
// 非法登记统一以带 cronx: 前缀的 error 值 panic，并保留底层错误链。
// 自动名称会随函数重命名改变；YAML 绑定和锁标识需要兼容时，应显式固定名称。
func (r *HandlerRegistry) MustRegister(handler Handler, name ...string) {
	if err := r.handlers.Register(handler, "Cron", name...); err != nil {
		panic(fmt.Errorf("cronx: %w", err))
	}
}

// Handlers 按登记顺序返回独立清单；后续登记不改变已交付的清单或已构造的调度器。
func (r *HandlerRegistry) Handlers() []NamedHandler {
	entries := r.handlers.Entries()
	if entries == nil {
		return nil
	}
	handlers := make([]NamedHandler, len(entries))
	for i, entry := range entries {
		handlers[i] = NamedHandler{
			Name:    entry.Name,
			Handler: entry.Handler,
		}
	}
	return handlers
}
