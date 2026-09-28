package mysqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	mysqldriver "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Pool 配置单个端点的连接池。零值分别采用 100、min(10, MaxOpenConns)、
// 3 分钟和 1 分钟；负值无效，空闲数不能超过打开数。
type Pool = gormcore.PoolConfig

// Endpoint 表示一个主库或从库的连接配置。
type Endpoint struct {
	// Host 是必填的主机名或 IP 地址，不包含端口。
	Host string
	// Port 是 TCP 端口，零值使用 3306；有效范围为 1 到 65535。
	Port int
	// Database 是必填的数据库名。
	Database string
	// Username 是必填的数据库用户名。
	Username string
	// Password 是连接密码，可以为空；调用方不要将其写入日志。
	Password string
	// Charset 是连接字符集，空值使用 utf8mb4。
	Charset string
	// Location 是解析时间值时使用的 Go 时区名称，空值使用 Asia/Shanghai；不修改 MySQL 会话时区。
	Location string
	// TLSConfig 是 MySQL 驱动识别的 TLS 配置名，空值不配置 TLS。
	TLSConfig string
	// DialTimeout 是建立连接的超时，零值不设置驱动级超时。
	DialTimeout time.Duration
	// ReadTimeout 是读取连接数据的超时，零值不设置驱动级超时。
	ReadTimeout time.Duration
	// WriteTimeout 是写入连接数据的超时，零值不设置驱动级超时。
	WriteTimeout time.Duration
	// Pool 是此端点的连接池配置，零值使用 Pool 的默认值。
	Pool Pool
}

// Config 配置一个逻辑 MySQL 数据库及其日志行为。
type Config struct {
	// Name 是必填的逻辑库名，用作日志中的下游标识。
	Name string
	// Master 是必填的主库连接配置，MasterDB 使用它。
	Master Endpoint
	// Slaves 是可选的从库配置；SlaveDB 轮询选择，未配置时回退主库。
	Slaves []Endpoint
	// SessionTimeZone 设置所有新连接的 MySQL 会话时区；空值沿用 MySQL 默认值。
	SessionTimeZone string
	// SlowThreshold 是慢查询阈值，零值默认 200 毫秒；无其他错误且耗时严格超过阈值时记录 Warn。
	SlowThreshold time.Duration
	// LogSQL 控制是否为正常查询记录包含 SQL 的 Info 日志；慢查询和错误始终包含 SQL。
	LogSQL bool
	// InterpolateSQL 控制日志 SQL 是否用参数值替换占位符；开启后可能记录业务数据。
	InterpolateSQL bool
}

// Client 持有一个主库及零个或多个从库的 GORM 连接池。
type Client struct {
	master    *gorm.DB
	slaves    []*gorm.DB
	pools     []*sql.DB
	nextSlave atomic.Uint64
	close     sync.Once
	closeErr  error
}

type preparedEndpoint struct {
	name   string
	pool   Pool
	driver *mysqldriver.Config
}

// New 创建全部端点并用 ctx 验活；失败时关闭已创建的连接池。
func New(ctx context.Context, cfg Config) (client *Client, err error) {
	if ctx == nil {
		return nil, errors.New("mysqlx: nil context")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("mysqlx: empty database name")
	}
	if cfg.SlowThreshold < 0 {
		return nil, errors.New("mysqlx: negative slow threshold")
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}

	master, err := prepare(cfg.Master, "master", cfg.SessionTimeZone)
	if err != nil {
		return nil, err
	}
	slaves := make([]preparedEndpoint, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaves[i], err = prepare(slave, "slave-"+strconv.Itoa(i+1), cfg.SessionTimeZone)
		if err != nil {
			return nil, err
		}
	}

	c := &Client{slaves: make([]*gorm.DB, 0, len(slaves))}
	defer func() {
		if err != nil {
			_ = c.Close()
		}
	}()
	c.master, err = c.open(ctx, cfg, master, "master")
	if err != nil {
		return nil, err
	}
	for _, slave := range slaves {
		db, openErr := c.open(ctx, cfg, slave, "slave")
		if openErr != nil {
			return nil, openErr
		}
		c.slaves = append(c.slaves, db)
	}
	return c, nil
}

func prepare(endpoint Endpoint, name, sessionTimeZone string) (preparedEndpoint, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.Username == "" {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q requires host, database and username", name)
	}
	if endpoint.Port < 0 || endpoint.Port > 65535 {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q has invalid port", name)
	}
	port := endpoint.Port
	if port == 0 {
		port = 3306
	}
	if endpoint.DialTimeout < 0 || endpoint.ReadTimeout < 0 || endpoint.WriteTimeout < 0 {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q has negative timeout", name)
	}
	pool, err := normalizePool(endpoint.Pool)
	if err != nil {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q: %w", name, err)
	}
	locationName := endpoint.Location
	if locationName == "" {
		locationName = "Asia/Shanghai"
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q has invalid location: %w", name, err)
	}
	charset := endpoint.Charset
	if charset == "" {
		charset = "utf8mb4"
	}
	driver := mysqldriver.NewConfig()
	driver.User = endpoint.Username
	driver.Passwd = endpoint.Password
	driver.Net = "tcp"
	driver.Addr = net.JoinHostPort(endpoint.Host, strconv.Itoa(port))
	driver.DBName = endpoint.Database
	driver.ParseTime = true
	driver.Loc = location
	driver.TLSConfig = endpoint.TLSConfig
	driver.Timeout = endpoint.DialTimeout
	driver.ReadTimeout = endpoint.ReadTimeout
	driver.WriteTimeout = endpoint.WriteTimeout
	if sessionTimeZone != "" {
		value, err := sessionTimeZoneSQLValue(sessionTimeZone)
		if err != nil {
			return preparedEndpoint{}, err
		}
		// 驱动在每条物理连接建立后执行 SET；值必须是安全的 SQL 字符串字面量。
		driver.Params = map[string]string{"time_zone": value}
	}
	if err := driver.Apply(mysqldriver.Charset(charset, "")); err != nil {
		return preparedEndpoint{}, fmt.Errorf("mysqlx: endpoint %q has invalid charset", name)
	}
	return preparedEndpoint{name: name, pool: pool, driver: driver}, nil
}

func sessionTimeZoneSQLValue(zone string) (string, error) {
	for _, r := range zone {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' ||
			r == '_' || r == '/' || r == '.' || r == '+' || r == '-' || r == ':' {
			continue
		}
		return "", errors.New("mysqlx: invalid session time zone")
	}
	return "'" + zone + "'", nil
}

func normalizePool(pool Pool) (Pool, error) {
	normalized, err := gormcore.NormalizePool(pool, gormcore.PoolConfig{
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxLifetime: 3 * time.Minute,
		ConnMaxIdleTime: time.Minute,
	})
	if err != nil {
		return Pool{}, err
	}
	return normalized, nil
}

func (c *Client) open(ctx context.Context, cfg Config, prepared preparedEndpoint, endpointType string) (*gorm.DB, error) {
	connector, err := mysqldriver.NewConnector(prepared.driver)
	if err != nil {
		return nil, fmt.Errorf("mysqlx: endpoint %q has invalid driver settings", prepared.name)
	}
	sqlDB := sql.OpenDB(connector)
	if err := gormcore.ConfigureAndPing(ctx, sqlDB, prepared.pool); err != nil {
		return nil, fmt.Errorf("mysqlx: ping endpoint %q: %w", prepared.name, err)
	}

	// GORM 的版本探测使用 Background，不受 New 的 ctx 截止时间约束。
	db, err := gormcore.Open(sqlDB, gormmysql.New(gormmysql.Config{
		Conn:                      sqlDB,
		DSNConfig:                 prepared.driver,
		SkipInitializeWithVersion: false,
	}), newTraceLogger(cfg, endpointType, prepared.name))
	if err != nil {
		return nil, fmt.Errorf("mysqlx: initialize endpoint %q: %w", prepared.name, err)
	}
	c.pools = append(c.pools, sqlDB)
	return db, nil
}

// MasterDB 返回带 ctx 的主库会话。
func (c *Client) MasterDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("mysqlx: nil context")
	}
	return c.master.WithContext(ctx)
}

// SlaveDB 返回带 ctx 的从库会话；未配置从库时使用主库。
func (c *Client) SlaveDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("mysqlx: nil context")
	}
	if len(c.slaves) == 0 {
		return c.master.WithContext(ctx)
	}
	index := (c.nextSlave.Add(1) - 1) % uint64(len(c.slaves))
	return c.slaves[index].WithContext(ctx)
}

// Close 关闭所有连接池；重复调用返回首次关闭结果。
func (c *Client) Close() error {
	c.close.Do(func() {
		var errs []error
		for _, pool := range c.pools {
			if err := pool.Close(); err != nil {
				errs = append(errs, err)
			}
		}
		c.closeErr = errors.Join(errs...)
	})
	return c.closeErr
}
