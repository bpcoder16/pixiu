// Package baseconfig 定义 biz 内各类应用入口共用的环境与日志配置，供具体入口嵌入。
// Env 复用 infra/env.Config；LogConfig 与 LogRotateConfig 定义通用日志设置。
// 本包只定义配置，不读取文件、发布环境或创建组件。
//
// 具体入口通过值嵌入复用配置，并用 squash 保持 env、log 位于文件顶层：
//
//	type AppConfig struct {
//		baseconfig.AppConfig `mapstructure:",squash"`
//	}
//	cfg := AppConfig{
//		AppConfig: baseconfig.AppConfig{
//			Log: baseconfig.LogConfig{
//				Format: "json",
//			},
//		},
//	}
//	_ = cfg.Log.Format
//	_ = &cfg.AppConfig
//
// 示例适用于 biz 子包，需导入 github.com/bpcoder16/pixiu/biz/internal/baseconfig。
// 业务项目通过 httpconfig 等公开入口访问嵌入字段，不直接导入本包。
// 加载器应使用完整入口类型解析一次，再调用 env.Init 初始化环境。
// 通用 bootstrap 接收 &cfg.AppConfig，具体入口可继续扩展自身的配置字段。
// HTTP 入口的加载流程和模板见 biz/httpconfig；不在基础类型中预设入口专属配置。
//
// 环境校验由 infra/env 负责，日志校验与默认值由 biz/bootstrap 负责。
// 日志固定使用 rotatefile，Format 必须显式设为 text/json，Caller 默认关闭。
// Dir 零值使用启动目录下的 log，轮转默认每小时、每个分流文件保留 48 份。
// 配置类型与未知字段在加载阶段检查，日志语义在组件初始化阶段检查。
package baseconfig
