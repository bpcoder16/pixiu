package httpconfig

import "github.com/bpcoder16/pixiu/biz/internal/baseconfig"

// AppConfig 嵌入共用启动配置，HTTP 专属配置在此扩展。
type AppConfig struct {
	baseconfig.AppConfig `mapstructure:",squash"`
}
