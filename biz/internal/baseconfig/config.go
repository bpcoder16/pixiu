package baseconfig

import "github.com/bpcoder16/pixiu/infra/env"

// AppConfig 定义 biz 内各类应用入口共用的环境与通用组件启动配置。
type AppConfig struct {
	Env env.Config `mapstructure:"env"`
	Log LogConfig  `mapstructure:"log"`
}
