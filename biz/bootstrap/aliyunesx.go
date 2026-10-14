package bootstrap

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/aliyun/esx"
)

var aliyunESInstances = instanceGroup[aliyunESFileConfig, aliyunESInstanceConfig, *esx.Client]{
	kind:      "AliyunESX",
	configure: (*aliyunESFileConfig).clientConfig,
	newDefault: func(cfg aliyunESInstanceConfig) (*esx.Client, error) {
		return esx.NewDefault(cfg.config, esx.OptLogRequests(cfg.logRequests))
	},
	newNamed: func(cfg aliyunESInstanceConfig) (*esx.Client, error) {
		return esx.NewNamed(cfg.config, esx.OptLogRequests(cfg.logRequests))
	},
	closeAll: esx.CloseAll,
}

// MustRegisterAliyunES 声明一个固定 InstanceID 的阿里云 ES 实例，不读配置或联网。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 读取 env.ConfigDirPath() 下的 aliyunesx.<name>.yaml，失败时 panic。
func MustRegisterAliyunES(name string, isDefault bool) {
	aliyunESInstances.mustRegister(name, isDefault)
}

// 文件只提供固定 AK/SK；实例身份来自声明，零值和语义校验由 esx 负责。
type aliyunESFileConfig struct {
	Region          string        `mapstructure:"region"`
	InstanceID      string        `mapstructure:"instanceID"`
	AccessKeyID     string        `mapstructure:"accessKeyID"`
	AccessKeySecret string        `mapstructure:"accessKeySecret"`
	Endpoint        string        `mapstructure:"endpoint"`
	RequestTimeout  time.Duration `mapstructure:"requestTimeout"`
	SlowThreshold   time.Duration `mapstructure:"slowThreshold"`
	LogRequests     bool          `mapstructure:"logRequests"`
}

type aliyunESInstanceConfig struct {
	config      esx.Config
	logRequests bool
}

func (cfg *aliyunESFileConfig) clientConfig(name string) (aliyunESInstanceConfig, error) {
	return aliyunESInstanceConfig{
		config: esx.Config{
			Name:            name,
			Region:          cfg.Region,
			InstanceID:      cfg.InstanceID,
			AccessKeyID:     cfg.AccessKeyID,
			AccessKeySecret: cfg.AccessKeySecret,
			Endpoint:        cfg.Endpoint,
			RequestTimeout:  cfg.RequestTimeout,
			SlowThreshold:   cfg.SlowThreshold,
		},
		logRequests: cfg.LogRequests,
	}, nil
}
