package bootstrap

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	esv7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
	esv8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	esv9 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v9"
	"github.com/bpcoder16/pixiu/infra/env"
)

var elasticsearchInstances = instanceGroup[elasticsearchFileConfig, elasticsearchInstanceConfig, *elasticsearchx.Client]{
	kind:      "Elasticsearch",
	configure: (*elasticsearchFileConfig).clientConfig,
	newDefault: func(cfg elasticsearchInstanceConfig) (*elasticsearchx.Client, error) {
		return cfg.newClient(true)
	},
	newNamed: func(cfg elasticsearchInstanceConfig) (*elasticsearchx.Client, error) {
		return cfg.newClient(false)
	},
	closeAll: elasticsearchx.CloseAll,
}

// MustRegisterElasticsearch 声明一个 Elasticsearch 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；名称只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// 从 env.ConfigDirPath() 读取 elasticsearch.<name>.yaml，version 必填且支持 7/8/9。
func MustRegisterElasticsearch(name string, isDefault bool) {
	elasticsearchInstances.mustRegister(name, isDefault)
}

// 文件只提供部署参数；名称和默认标记来自声明，版本与选项不进入公共实例组。
type elasticsearchFileConfig struct {
	Version        int                   `mapstructure:"version"`
	Addresses      []string              `mapstructure:"addresses"`
	Username       string                `mapstructure:"username"`
	Password       string                `mapstructure:"password"`
	APIKey         string                `mapstructure:"apiKey"`
	CACertFile     string                `mapstructure:"caCertFile"`
	StartupTimeout time.Duration         `mapstructure:"startupTimeout"`
	DialTimeout    time.Duration         `mapstructure:"dialTimeout"`
	Pool           elasticsearchFilePool `mapstructure:"pool"`
	SlowThreshold  time.Duration         `mapstructure:"slowThreshold"`
	// 区分省略与显式 false；bootstrap 默认开启概要，底层独立客户端默认值不变。
	LogRequests *bool `mapstructure:"logRequests"`
	LogDetails  bool  `mapstructure:"logDetails"`
}

type elasticsearchFilePool struct {
	MaxIdleConns        int           `mapstructure:"maxIdleConns"`
	MaxIdleConnsPerHost int           `mapstructure:"maxIdleConnsPerHost"`
	MaxConnsPerHost     int           `mapstructure:"maxConnsPerHost"`
	IdleConnTimeout     time.Duration `mapstructure:"idleConnTimeout"`
}

type elasticsearchInstanceConfig struct {
	version int
	config  elasticsearchx.Config
	options []elasticsearchx.Option
}

func (cfg *elasticsearchFileConfig) clientConfig(name string) (elasticsearchInstanceConfig, error) {
	if cfg.Version != 7 && cfg.Version != 8 && cfg.Version != 9 {
		return elasticsearchInstanceConfig{}, fmt.Errorf("version must be 7, 8 or 9")
	}
	var cert []byte
	if cfg.CACertFile != "" {
		path := cfg.CACertFile
		if !filepath.IsAbs(path) {
			path = filepath.Join(env.ConfigDirPath(), path)
		}
		var err error
		cert, err = os.ReadFile(path)
		if err != nil {
			return elasticsearchInstanceConfig{}, fmt.Errorf("read CA certificate: %w", err)
		}
		// 防止指定空文件时被底层视为未配置 CA，证书格式仍由底层统一校验。
		if len(cert) == 0 {
			return elasticsearchInstanceConfig{}, fmt.Errorf("empty CA certificate file %q", path)
		}
	}
	return elasticsearchInstanceConfig{
		version: cfg.Version,
		config: elasticsearchx.Config{
			Name:                name,
			Addresses:           cfg.Addresses,
			Username:            cfg.Username,
			Password:            cfg.Password,
			APIKey:              cfg.APIKey,
			CACert:              cert,
			StartupTimeout:      cfg.StartupTimeout,
			DialTimeout:         cfg.DialTimeout,
			MaxIdleConns:        cfg.Pool.MaxIdleConns,
			MaxIdleConnsPerHost: cfg.Pool.MaxIdleConnsPerHost,
			MaxConnsPerHost:     cfg.Pool.MaxConnsPerHost,
			IdleConnTimeout:     cfg.Pool.IdleConnTimeout,
			SlowThreshold:       cfg.SlowThreshold,
		},
		options: []elasticsearchx.Option{
			elasticsearchx.OptLogRequests(cfg.LogRequests == nil || *cfg.LogRequests),
			elasticsearchx.OptLogDetails(cfg.LogDetails),
		},
	}, nil
}

func (cfg elasticsearchInstanceConfig) newClient(asDefault bool) (*elasticsearchx.Client, error) {
	var create func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
	switch cfg.version {
	case 7:
		create = esv7.NewNamed
		if asDefault {
			create = esv7.NewDefault
		}
	case 8:
		create = esv8.NewNamed
		if asDefault {
			create = esv8.NewDefault
		}
	case 9:
		create = esv9.NewNamed
		if asDefault {
			create = esv9.NewDefault
		}
	default:
		return nil, fmt.Errorf("version must be 7, 8 or 9")
	}
	return create(cfg.config, cfg.options...)
}
