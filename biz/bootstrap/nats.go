package bootstrap

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/natsx"
)

var natsInstances = instanceGroup[natsFileConfig, natsx.Config, *natsx.Client]{
	kind:      "NATS",
	configure: (*natsFileConfig).clientConfig,
	newDefault: func(cfg natsx.Config) (*natsx.Client, error) {
		return natsx.NewDefault(cfg)
	},
	newNamed: func(cfg natsx.Config) (*natsx.Client, error) {
		return natsx.NewNamed(cfg)
	},
	closeAll: natsx.CloseAll,
}

// MustRegisterNATS 声明一个 NATS 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；名称为非空 ASCII 字母、数字、下划线和连字符组合。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许没有默认实例。
// MustBaseInit 从 env.ConfigDirPath() 读取 nats.<name>.yaml，失败时 panic。
// 客户端关闭登记到资源栈；资源回收前由 Start 排空 NATS 消费服务。
func MustRegisterNATS(name string, isDefault bool) {
	natsInstances.mustRegister(name, isDefault)
}

// 实例身份来自声明；文件开放连接参数、日志路由和 namespace，不解码完整官方选项或业务消费配置。
type natsFileConfig struct {
	LoggerName     string            `mapstructure:"loggerName"`
	Namespace      string            `mapstructure:"namespace"`
	URLs           []string          `mapstructure:"urls"`
	Username       string            `mapstructure:"username"`
	Password       string            `mapstructure:"password"`
	ConnectTimeout time.Duration     `mapstructure:"connectTimeout"`
	DrainTimeout   time.Duration     `mapstructure:"drainTimeout"`
	CloseTimeout   time.Duration     `mapstructure:"closeTimeout"`
	JetStream      natsFileJetStream `mapstructure:"jetStream"`
}

type natsFileJetStream struct {
	PublishAsyncTimeout time.Duration `mapstructure:"publishAsyncTimeout"`
}

func (cfg *natsFileConfig) clientConfig(name string) (natsx.Config, error) {
	for i, address := range cfg.URLs {
		// 与 natsx 一样支持省略 scheme，但认证只允许来自显式配置字段。
		if !strings.Contains(address, "://") {
			address = "nats://" + address
		}
		parsed, err := url.Parse(address)
		if err != nil {
			return natsx.Config{}, fmt.Errorf("invalid urls[%d]: %w", i, err)
		}
		if parsed.User != nil {
			return natsx.Config{}, fmt.Errorf("urls[%d] must not contain credentials; use username/password", i)
		}
	}
	return natsx.Config{
		Name:           name,
		LoggerName:     cfg.LoggerName,
		Namespace:      cfg.Namespace,
		URLs:           cfg.URLs,
		Username:       cfg.Username,
		Password:       cfg.Password,
		ConnectTimeout: cfg.ConnectTimeout,
		DrainTimeout:   cfg.DrainTimeout,
		CloseTimeout:   cfg.CloseTimeout,
		JetStream: natsx.JetStreamConfig{
			PublishAsyncTimeout: cfg.JetStream.PublishAsyncTimeout,
		},
	}, nil
}
