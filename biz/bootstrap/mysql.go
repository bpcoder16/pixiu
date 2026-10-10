package bootstrap

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

type mysqlRegistration struct {
	name      string
	isDefault bool
}

var mysqlRegistrations []mysqlRegistration

// MustRegisterMySQL 声明一个 MySQL 实例，不读配置或建立连接。
// 仅在 MustBaseInit 前串行调用；name 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；允许不设置默认实例。
// MustBaseInit 从 env.ConfigDirPath() 下读取 mysql.<name>.yaml，失败时 panic。
func MustRegisterMySQL(name string, isDefault bool) {
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started; cannot register MySQL %q", name))
	}
	if name == "" {
		panic(fmt.Errorf("bootstrap: invalid MySQL name %q", name))
	}
	for _, ch := range name {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			panic(fmt.Errorf("bootstrap: invalid MySQL name %q: only ASCII letters, digits, '_' and '-' are allowed", name))
		}
	}
	for _, registered := range mysqlRegistrations {
		if registered.name == name {
			panic(fmt.Errorf("bootstrap: MySQL %q is already registered", name))
		}
		if isDefault && registered.isDefault {
			panic(fmt.Errorf("bootstrap: default MySQL %q is already registered; cannot register %q as default", registered.name, name))
		}
	}
	mysqlRegistrations = append(mysqlRegistrations, mysqlRegistration{
		name:      name,
		isDefault: isDefault,
	})
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

func initMySQL(resources *lifecycle.Stack) error {
	if len(mysqlRegistrations) == 0 {
		return nil
	}
	// 全部文件先完成严格解析，避免后续文件缺失或拼写错误时已创建连接。
	configs := make([]mysqlx.Config, len(mysqlRegistrations))
	for i, registration := range mysqlRegistrations {
		path := filepath.Join(env.ConfigDirPath(), "mysql."+registration.name+".yaml")
		cfg, err := configx.Parse[mysqlFileConfig](path)
		if err != nil {
			return fmt.Errorf("load instance %q from %q: %w", registration.name, path, err)
		}
		slaves := make([]mysqlx.Endpoint, len(cfg.Slaves))
		for j, address := range cfg.Slaves {
			slaves[j] = cfg.endpoint(address)
		}
		configs[i] = mysqlx.Config{
			Name:            registration.name,
			InitTimeout:     cfg.InitTimeout,
			Master:          cfg.endpoint(cfg.Master),
			Slaves:          slaves,
			SessionTimeZone: cfg.SessionTimeZone,
			SlowThreshold:   cfg.SlowThreshold,
			LogSQL:          cfg.LogSQL == nil || *cfg.LogSQL,
			InterpolateSQL:  cfg.InterpolateSQL == nil || *cfg.InterpolateSQL,
		}
	}
	// 命名客户端由模块统一管理；先登记一次，确保部分初始化失败也能收尾。
	if err := resources.Register(mysqlx.CloseAll); err != nil {
		return fmt.Errorf("register CloseAll: %w", err)
	}
	for i, cfg := range configs {
		var err error
		if mysqlRegistrations[i].isDefault {
			_, err = mysqlx.NewDefault(cfg)
		} else {
			_, err = mysqlx.NewNamed(cfg)
		}
		if err != nil {
			path := filepath.Join(env.ConfigDirPath(), "mysql."+cfg.Name+".yaml")
			return fmt.Errorf("initialize instance %q from %q: %w", cfg.Name, path, err)
		}
	}
	return nil
}
