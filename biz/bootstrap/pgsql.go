//go:build !pixiu_bootstrap_no_pgsql

package bootstrap

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/pgsqlx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

var pgsqlInstances = instanceGroup[pgsqlFileConfig, pgsqlx.Config, *pgsqlx.Client]{
	kind:       "PgSQL",
	configure:  (*pgsqlFileConfig).clientConfig,
	newDefault: pgsqlx.NewDefault,
	newNamed:   pgsqlx.NewNamed,
	closeAll:   pgsqlx.CloseAll,
}

// MustRegisterPostgreSQL 声明一个 PostgreSQL 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 从 env.ConfigDirPath() 下读取 pgsql.<name>.yaml，失败时 panic。
func MustRegisterPostgreSQL(name string, isDefault bool) {
	pgsqlInstances.mustRegister(name, isDefault)
}

func initPostgreSQL(resources *lifecycle.Stack) error {
	return pgsqlInstances.initialize(resources)
}

// 实例身份来自代码声明；主从共用连接参数，默认值及语义校验由 pgsqlx 负责。
type pgsqlFileConfig struct {
	InitTimeout           time.Duration  `mapstructure:"initTimeout"`
	Master                pgsqlAddress   `mapstructure:"master"`
	Slaves                []pgsqlAddress `mapstructure:"slaves"`
	Database              string         `mapstructure:"database"`
	Username              string         `mapstructure:"username"`
	Password              string         `mapstructure:"password"`
	Charset               string         `mapstructure:"charset"`
	Location              string         `mapstructure:"location"`
	SSLMode               string         `mapstructure:"sslMode"`
	SSLRootCert           string         `mapstructure:"sslRootCert"`
	SSLCert               string         `mapstructure:"sslCert"`
	SSLKey                string         `mapstructure:"sslKey"`
	DialTimeout           time.Duration  `mapstructure:"dialTimeout"`
	Pool                  pgsqlx.Pool    `mapstructure:"pool"`
	SessionTimeZone       string         `mapstructure:"sessionTimeZone"`
	SlowThreshold         time.Duration  `mapstructure:"slowThreshold"`
	DisableStatementCache bool           `mapstructure:"disableStatementCache"`
	// 指针区分省略与显式 false；省略时默认开启，与 MySQL 装配一致。
	LogSQL         *bool `mapstructure:"logSQL"`
	InterpolateSQL *bool `mapstructure:"interpolateSQL"`
}

type pgsqlAddress struct {
	Host string `mapstructure:"host"`
	Port int    `mapstructure:"port"`
}

func (cfg *pgsqlFileConfig) endpoint(address pgsqlAddress) pgsqlx.Endpoint {
	return pgsqlx.Endpoint{
		Host:        address.Host,
		Port:        address.Port,
		Database:    cfg.Database,
		Username:    cfg.Username,
		Password:    cfg.Password,
		Charset:     cfg.Charset,
		Location:    cfg.Location,
		SSLMode:     cfg.SSLMode,
		SSLRootCert: cfg.SSLRootCert,
		SSLCert:     cfg.SSLCert,
		SSLKey:      cfg.SSLKey,
		DialTimeout: cfg.DialTimeout,
		Pool:        cfg.Pool,
	}
}

func (cfg *pgsqlFileConfig) clientConfig(name string) (pgsqlx.Config, error) {
	slaves := make([]pgsqlx.Endpoint, len(cfg.Slaves))
	for i, address := range cfg.Slaves {
		slaves[i] = cfg.endpoint(address)
	}
	return pgsqlx.Config{
		Name:                  name,
		InitTimeout:           cfg.InitTimeout,
		Master:                cfg.endpoint(cfg.Master),
		Slaves:                slaves,
		SessionTimeZone:       cfg.SessionTimeZone,
		SlowThreshold:         cfg.SlowThreshold,
		LogSQL:                cfg.LogSQL == nil || *cfg.LogSQL,
		InterpolateSQL:        cfg.InterpolateSQL == nil || *cfg.InterpolateSQL,
		DisableStatementCache: cfg.DisableStatementCache,
	}, nil
}
