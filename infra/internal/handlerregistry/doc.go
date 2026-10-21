// Package handlerregistry 为 infra 内的强类型 Handler 注册表共用登记、命名和快照逻辑。
// 只依赖标准库，不读取配置或管理业务生命周期；公开包负责限制函数签名、包装错误及适配返回类型。
// Registry 零值可用，首次使用后不得复制；登记与读取快照可并发，失败登记不改变已有清单。
//
// 以下示例位于允许导入本包的 infra 包中，另需导入 context：
//
//	type Handler func(context.Context) error
//
//	func Heartbeat(context.Context) error {
//		return nil
//	}
//
//	func declarations() ([]handlerregistry.Entry[Handler], error) {
//		var registry handlerregistry.Registry[Handler]
//		if err := registry.Register(Heartbeat, "Cron"); err != nil {
//			return nil, err
//		}
//		return registry.Entries(), nil
//	}
//
// 包级具名函数自动保留原始大小写；方法值、闭包及泛型函数须显式命名。
// 显式名称不可为空或含首尾空白；nil、非函数、多个名称参数或重名均返回错误。
// 反射仅在登记时校验函数及推导名称，不反射调用业务。
// Entries 保留登记顺序，Snapshot 返回名称映射；空注册表均返回 nil。
// 快照复制容器，后续登记和调用方修改互不影响，函数捕获的依赖仍由调用方管理。
package handlerregistry
