package appconfig

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/bpcoder16/pixiu/infra/env"
)

// disabledServices 支持以下常驻服务类型，可追加冒号和实例名精确选择。
const (
	ServiceHTTP         = "http"
	ServiceCron         = "cron"
	ServiceNATSConsumer = "nats-consumer"
	ServiceWebSocket    = "websocket"
)

// 与 bootstrap 声明实例时使用的名称规则一致。
var serviceInstanceName = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// AppConfig 定义 biz 内各类应用入口共用的环境与通用组件启动配置。
type AppConfig struct {
	Env              env.Config `mapstructure:"env"`
	Log              LogConfig  `mapstructure:"log"`
	DisabledServices []string   `mapstructure:"disabledServices"`
}

// ValidateDisabledServices 校验禁用列表，避免类型拼写错误时意外启动服务。
func (cfg *AppConfig) ValidateDisabledServices() error {
	for i, rule := range cfg.DisabledServices {
		kind, instance, named := strings.Cut(rule, ":")
		switch kind {
		case ServiceHTTP, ServiceCron, ServiceNATSConsumer, ServiceWebSocket:
		default:
			return fmt.Errorf("disabledServices[%d]: unknown service type in %q", i, rule)
		}
		if named && !serviceInstanceName.MatchString(instance) {
			return fmt.Errorf("disabledServices[%d]: invalid instance name in %q", i, rule)
		}
	}
	return nil
}
