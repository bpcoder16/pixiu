package bootstrap

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/mysqlx"
)

var mysqlInstances = instanceGroup[mysqlFileConfig, mysqlx.Config, *mysqlx.Client]{
	kind:       "MySQL",
	configure:  (*mysqlFileConfig).clientConfig,
	newDefault: mysqlx.NewDefault,
	newNamed:   mysqlx.NewNamed,
	closeAll:   mysqlx.CloseAll,
}

// MustRegisterMySQL 声明一个 MySQL 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 从 env.ConfigDirPath() 下读取 mysql.<name>.yaml，失败时 panic。
func MustRegisterMySQL(name string, isDefault bool) {
	mysqlInstances.mustRegister(name, isDefault)
}

// 文件不接受 Name 或 IsDefault，避免声明与部署配置产生两份实例身份。
// 主从仅配置地址，共用连接参数；零值默认及语义校验由 mysqlx 负责。
type mysqlFileConfig struct {
	InitTimeout     time.Duration  `mapstructure:"initTimeout"`
	Master          mysqlAddress   `mapstructure:"master"`
	Slaves          []mysqlAddress `mapstructure:"slaves"`
	Database        string         `mapstructure:"database"`
	Username        string         `mapstructure:"username"`
	Password        string         `mapstructure:"password"`
	Charset         string         `mapstructure:"charset"`
	Location        string         `mapstructure:"location"`
	TLSConfig       string         `mapstructure:"tlsConfig"`
	DialTimeout     time.Duration  `mapstructure:"dialTimeout"`
	ReadTimeout     time.Duration  `mapstructure:"readTimeout"`
	WriteTimeout    time.Duration  `mapstructure:"writeTimeout"`
	Pool            mysqlx.Pool    `mapstructure:"pool"`
	SessionTimeZone string         `mapstructure:"sessionTimeZone"`
	SlowThreshold   time.Duration  `mapstructure:"slowThreshold"`
	// 指针区分省略与显式 false；省略时默认开启。
	LogSQL         *bool `mapstructure:"logSQL"`
	InterpolateSQL *bool `mapstructure:"interpolateSQL"`
}

type mysqlAddress struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

func (cfg *mysqlFileConfig) endpoint(address mysqlAddress) mysqlx.Endpoint {
	return mysqlx.Endpoint{
		Host:         address.Host,
		Port:         address.Port,
		Database:     cfg.Database,
		Username:     cfg.Username,
		Password:     cfg.Password,
		Charset:      cfg.Charset,
		Location:     cfg.Location,
		TLSConfig:    cfg.TLSConfig,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		Pool:         cfg.Pool,
	}
}

func (cfg *mysqlFileConfig) clientConfig(name string) (mysqlx.Config, error) {
	slaves := make([]mysqlx.Endpoint, len(cfg.Slaves))
	for i, address := range cfg.Slaves {
		slaves[i] = cfg.endpoint(address)
	}
	return mysqlx.Config{
		Name:            name,
		InitTimeout:     cfg.InitTimeout,
		Master:          cfg.endpoint(cfg.Master),
		Slaves:          slaves,
		SessionTimeZone: cfg.SessionTimeZone,
		SlowThreshold:   cfg.SlowThreshold,
		LogSQL:          cfg.LogSQL == nil || *cfg.LogSQL,
		InterpolateSQL:  cfg.InterpolateSQL == nil || *cfg.InterpolateSQL,
	}, nil
}
