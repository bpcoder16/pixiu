package bootstrap

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
)

var aliyunOSSInstances = instanceGroup[aliyunOSSFileConfig, aliyunOSSInstanceConfig, *ossx.Client]{
	kind:      "AliyunOSSX",
	configure: (*aliyunOSSFileConfig).clientConfig,
	newDefault: func(cfg aliyunOSSInstanceConfig) (*ossx.Client, error) {
		return ossx.NewDefault(cfg.config, ossx.OptLogRequests(cfg.logRequests))
	},
	newNamed: func(cfg aliyunOSSInstanceConfig) (*ossx.Client, error) {
		return ossx.NewNamed(cfg.config, ossx.OptLogRequests(cfg.logRequests))
	},
	closeAll: ossx.CloseAll,
}

// MustRegisterAliyunOSS 声明一个固定 Bucket 的 OSS 实例，不读配置或联网。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 读取 env.ConfigDirPath() 下的 aliyunossx.<name>.yaml，失败时 panic。
func MustRegisterAliyunOSS(name string, isDefault bool) {
	aliyunOSSInstances.mustRegister(name, isDefault)
}

// 文件只提供固定 AK/SK；实例身份来自声明，零值和语义校验由 ossx 负责。
type aliyunOSSFileConfig struct {
	Region          string        `mapstructure:"region"`
	Bucket          string        `mapstructure:"bucket"`
	AccessKeyID     string        `mapstructure:"accessKeyID"`
	AccessKeySecret string        `mapstructure:"accessKeySecret"`
	Endpoint        string        `mapstructure:"endpoint"`
	RequestTimeout  time.Duration `mapstructure:"requestTimeout"`
	SlowThreshold   time.Duration `mapstructure:"slowThreshold"`
	LogRequests     bool          `mapstructure:"logRequests"`
}

type aliyunOSSInstanceConfig struct {
	config      ossx.Config
	logRequests bool
}

func (cfg *aliyunOSSFileConfig) clientConfig(name string) (aliyunOSSInstanceConfig, error) {
	return aliyunOSSInstanceConfig{
		config: ossx.Config{
			Name:            name,
			Region:          cfg.Region,
			Bucket:          cfg.Bucket,
			AccessKeyID:     cfg.AccessKeyID,
			AccessKeySecret: cfg.AccessKeySecret,
			Endpoint:        cfg.Endpoint,
			RequestTimeout:  cfg.RequestTimeout,
			SlowThreshold:   cfg.SlowThreshold,
		},
		logRequests: cfg.LogRequests,
	}, nil
}
