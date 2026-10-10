package bootstrap

import (
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

var redisInstances = instanceGroup[redisFileConfig, redisx.Config, *redisx.Client]{
	kind:       "Redis",
	configure:  (*redisFileConfig).clientConfig,
	newDefault: redisx.NewDefault,
	newNamed:   redisx.NewNamed,
	closeAll:   redisx.CloseAll,
}

// MustRegisterRedis 声明一个 Redis 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 从 env.ConfigDirPath() 下读取 redis.<name>.yaml，失败时 panic。
func MustRegisterRedis(name string, isDefault bool) {
	redisInstances.mustRegister(name, isDefault)
}

// 文件只开放单节点 TCP 所需的参数，实例身份由声明注入。
// 不直接解码 redis.Options，避免将驱动的全部字段变为部署配置契约。
type redisFileConfig struct {
	Host          string        `mapstructure:"host"`
	Port          int           `mapstructure:"port"`
	Username      string        `mapstructure:"username"`
	Password      string        `mapstructure:"password"`
	DB            int           `mapstructure:"db"`
	DialTimeout   time.Duration `mapstructure:"dialTimeout"`
	ReadTimeout   time.Duration `mapstructure:"readTimeout"`
	WriteTimeout  time.Duration `mapstructure:"writeTimeout"`
	MaxRetries    int           `mapstructure:"maxRetries"`
	Pool          redisFilePool `mapstructure:"pool"`
	SlowThreshold time.Duration `mapstructure:"slowThreshold"`
	LogCommands   bool          `mapstructure:"logCommands"`
}

type redisFilePool struct {
	Size            int           `mapstructure:"size"`
	MaxActiveConns  int           `mapstructure:"maxActiveConns"`
	MinIdleConns    int           `mapstructure:"minIdleConns"`
	MaxIdleConns    int           `mapstructure:"maxIdleConns"`
	Timeout         time.Duration `mapstructure:"timeout"`
	ConnMaxIdleTime time.Duration `mapstructure:"connMaxIdleTime"`
	ConnMaxLifetime time.Duration `mapstructure:"connMaxLifetime"`
}

func (cfg *redisFileConfig) clientConfig(name string) (redisx.Config, error) {
	if cfg.Host == "" || strings.TrimSpace(cfg.Host) != cfg.Host {
		return redisx.Config{}, fmt.Errorf("host is required and must not have surrounding whitespace")
	}
	if cfg.Port < 0 || cfg.Port > 65535 {
		return redisx.Config{}, fmt.Errorf("invalid port")
	}
	if cfg.DB < 0 {
		return redisx.Config{}, fmt.Errorf("negative db")
	}
	if cfg.MaxRetries < -1 {
		return redisx.Config{}, fmt.Errorf("maxRetries must be -1 or nonnegative")
	}
	for _, setting := range []struct {
		name  string
		value time.Duration
	}{
		{"dialTimeout", cfg.DialTimeout},
		{"readTimeout", cfg.ReadTimeout},
		{"writeTimeout", cfg.WriteTimeout},
		{"slowThreshold", cfg.SlowThreshold},
		{"pool.timeout", cfg.Pool.Timeout},
		{"pool.connMaxIdleTime", cfg.Pool.ConnMaxIdleTime},
		{"pool.connMaxLifetime", cfg.Pool.ConnMaxLifetime},
	} {
		if setting.value < 0 {
			return redisx.Config{}, fmt.Errorf("negative %s", setting.name)
		}
	}
	for _, setting := range []struct {
		name  string
		value int
	}{
		{"pool.size", cfg.Pool.Size},
		{"pool.maxActiveConns", cfg.Pool.MaxActiveConns},
		{"pool.minIdleConns", cfg.Pool.MinIdleConns},
		{"pool.maxIdleConns", cfg.Pool.MaxIdleConns},
	} {
		if setting.value < 0 || setting.value > math.MaxInt32 {
			return redisx.Config{}, fmt.Errorf("invalid %s", setting.name)
		}
	}
	port := cfg.Port
	if port == 0 {
		port = 6379
	}
	return redisx.Config{
		Name: name,
		Options: redis.Options{
			Addr:            net.JoinHostPort(cfg.Host, strconv.Itoa(port)),
			Username:        cfg.Username,
			Password:        cfg.Password,
			DB:              cfg.DB,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			MaxRetries:      cfg.MaxRetries,
			PoolSize:        cfg.Pool.Size,
			MaxActiveConns:  cfg.Pool.MaxActiveConns,
			MinIdleConns:    cfg.Pool.MinIdleConns,
			MaxIdleConns:    cfg.Pool.MaxIdleConns,
			PoolTimeout:     cfg.Pool.Timeout,
			ConnMaxIdleTime: cfg.Pool.ConnMaxIdleTime,
			ConnMaxLifetime: cfg.Pool.ConnMaxLifetime,
		},
		SlowThreshold: cfg.SlowThreshold,
		LogCommands:   cfg.LogCommands,
	}, nil
}
