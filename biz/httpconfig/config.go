package httpconfig

import "github.com/bpcoder16/pixiu/infra/env"

// AppConfig 定义 HTTP 应用启动配置，第一版仅包含环境信息。
type AppConfig struct {
	Env env.Config `mapstructure:"env"`
}
