package bootstrap

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/aliyun/stsx"
)

var aliyunSTSInstances = instanceGroup[aliyunSTSFileConfig, aliyunSTSInstanceConfig, *stsx.Client]{
	kind:      "AliyunSTSX",
	configure: (*aliyunSTSFileConfig).clientConfig,
	newDefault: func(cfg aliyunSTSInstanceConfig) (*stsx.Client, error) {
		return stsx.NewDefault(cfg.config, stsx.OptLogRequests(cfg.logRequests))
	},
	newNamed: func(cfg aliyunSTSInstanceConfig) (*stsx.Client, error) {
		return stsx.NewNamed(cfg.config, stsx.OptLogRequests(cfg.logRequests))
	},
	closeAll: stsx.CloseAll,
}

// MustRegisterAliyunSTS 声明一个阿里云 STS 实例，不读配置或联网。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 读取 env.ConfigDirPath() 下的 aliyunstsx.<name>.yaml，失败时 panic。
func MustRegisterAliyunSTS(name string, isDefault bool) {
	aliyunSTSInstances.mustRegister(name, isDefault)
}

// 文件只提供固定 AK/SK；实例身份来自声明，零值和语义校验由 stsx 负责。
type aliyunSTSFileConfig struct {
	Region          string        `mapstructure:"region"`
	AccessKeyID     string        `mapstructure:"accessKeyID"`
	AccessKeySecret string        `mapstructure:"accessKeySecret"`
	Endpoint        string        `mapstructure:"endpoint"`
	RequestTimeout  time.Duration `mapstructure:"requestTimeout"`
	SlowThreshold   time.Duration `mapstructure:"slowThreshold"`
	LogRequests     bool          `mapstructure:"logRequests"`
}

type aliyunSTSInstanceConfig struct {
	config      stsx.Config
	logRequests bool
}

func (cfg *aliyunSTSFileConfig) clientConfig(name string) (aliyunSTSInstanceConfig, error) {
	return aliyunSTSInstanceConfig{
		config: stsx.Config{
			Name:            name,
			Region:          cfg.Region,
			AccessKeyID:     cfg.AccessKeyID,
			AccessKeySecret: cfg.AccessKeySecret,
			Endpoint:        cfg.Endpoint,
			RequestTimeout:  cfg.RequestTimeout,
			SlowThreshold:   cfg.SlowThreshold,
		},
		logRequests: cfg.LogRequests,
	}, nil
}
