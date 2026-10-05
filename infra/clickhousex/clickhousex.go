package clickhousex

import (
	"context"
	"crypto/tls"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

// Pool 配置单个端点的连接池。零值分别采用 100、min(10, MaxOpenConns)、
// 3 分钟和 1 分钟；负值无效，空闲数不能超过打开数。
type Pool = gormcore.PoolConfig

// Endpoint 表示一个 ClickHouse 主库或从库的连接配置。
type Endpoint struct {
	// Host 是必填的主机名或 IP 地址，不包含端口。
	Host string
	// Port 是服务端端口，零值时原生协议使用 9000，HTTP 使用 8123；TLS 端口应显式设置。
	Port int
	// Database 是必填的数据库名。
	Database string
	// Username 是必填的数据库用户名。
	Username string
	// Password 是连接密码，可以为空；调用方不要将其写入日志。
	Password string
	// Protocol 零值为原生 TCP，也可设置为 clickhouse.HTTP。
	Protocol clickhouse.Protocol
	// TLS 非 nil 时启用 TLS，调用方应配置证书验证所需的参数。
	TLS *tls.Config
	// DialTimeout 是建立连接的超时，零值沿用驱动默认值。
	DialTimeout time.Duration
	// ReadTimeout 是读取数据的超时，零值沿用驱动默认值。
	ReadTimeout time.Duration
	// Settings 透传至官方驱动；创建客户端后不得修改。
	Settings clickhouse.Settings
	// Compression 指定驱动压缩选项；创建客户端后不得修改。
	Compression *clickhouse.Compression
	// Pool 是此端点的连接池配置，零值使用 Pool 的默认值。
	Pool Pool
}

// Config 配置一个逻辑 ClickHouse 数据库及其日志行为。
type Config struct {
	// Name 是必填的逻辑库名，用作日志中的下游标识。
	Name string
	// Master 是必填的主库连接配置，MasterDB 使用它。
	Master Endpoint
	// Slaves 是可选的从库配置；SlaveDB 轮询选择，未配置时回退主库。
	Slaves []Endpoint
	// SlowThreshold 零值为 200 毫秒，严格超过时记录 Warn。
	SlowThreshold time.Duration
	// LogSQL 控制正常查询的 Info SQL 日志；错误和慢查询始终记录 SQL。
	LogSQL bool
	// InterpolateSQL 开启后会在诊断 SQL 中展开参数，可能记录业务数据。
	InterpolateSQL bool
}

// Client 持有一个主库和零个或多个从库的 GORM 连接池。
type Client struct {
	cluster gormcore.Cluster
}

type preparedEndpoint struct {
	name    string
	options clickhouse.Options
	pool    Pool
}

// New 创建并验活全部端点；初始化沿用驱动超时，失败时关闭已创建的连接池。
// 客户端由调用方通过 Close 显式关闭，查询时再传入操作 context。
func New(cfg Config) (client *Client, err error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("clickhousex: empty database name")
	}
	if cfg.SlowThreshold < 0 {
		return nil, errors.New("clickhousex: negative slow threshold")
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}

	master, err := prepare(cfg.Master, "master")
	if err != nil {
		return nil, err
	}
	slaves := make([]preparedEndpoint, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaves[i], err = prepare(slave, "slave-"+strconv.Itoa(i+1))
		if err != nil {
			return nil, err
		}
	}

	c := &Client{}
	if err := gormcore.BuildCluster(context.Background(), &c.cluster, master, slaves, func(ctx context.Context, endpoint preparedEndpoint, role string) (*gorm.DB, *sql.DB, error) {
		return open(ctx, cfg, endpoint, role)
	}); err != nil {
		return nil, err
	}
	return c, nil
}

func prepare(endpoint Endpoint, name string) (preparedEndpoint, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.Username == "" {
		return preparedEndpoint{}, fmt.Errorf("clickhousex: endpoint %q requires host, database and username", name)
	}
	if endpoint.Protocol != clickhouse.Native && endpoint.Protocol != clickhouse.HTTP {
		return preparedEndpoint{}, fmt.Errorf("clickhousex: endpoint %q has invalid protocol", name)
	}
	if endpoint.Port < 0 || endpoint.Port > 65535 {
		return preparedEndpoint{}, fmt.Errorf("clickhousex: endpoint %q has invalid port", name)
	}
	if endpoint.DialTimeout < 0 || endpoint.ReadTimeout < 0 {
		return preparedEndpoint{}, fmt.Errorf("clickhousex: endpoint %q has negative timeout", name)
	}
	pool, err := normalizePool(endpoint.Pool)
	if err != nil {
		return preparedEndpoint{}, fmt.Errorf("clickhousex: endpoint %q: %w", name, err)
	}
	port := endpoint.Port
	if port == 0 {
		port = 9000
		if endpoint.Protocol == clickhouse.HTTP {
			port = 8123
		}
	}
	options := clickhouse.Options{
		Protocol: endpoint.Protocol,
		Addr:     []string{net.JoinHostPort(endpoint.Host, strconv.Itoa(port))},
		Auth: clickhouse.Auth{
			Database: endpoint.Database,
			Username: endpoint.Username,
			Password: endpoint.Password,
		},
		TLS:         endpoint.TLS,
		DialTimeout: endpoint.DialTimeout,
		ReadTimeout: endpoint.ReadTimeout,
		Settings:    endpoint.Settings,
		Compression: endpoint.Compression,
	}
	return preparedEndpoint{
		name:    name,
		options: options,
		pool:    pool,
	}, nil
}

func normalizePool(pool Pool) (Pool, error) {
	return gormcore.NormalizePool(pool, Pool{
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxLifetime: 3 * time.Minute,
		ConnMaxIdleTime: time.Minute,
	})
}

func open(ctx context.Context, cfg Config, prepared preparedEndpoint, endpointType string) (*gorm.DB, *sql.DB, error) {
	sqlDB := clickhouse.OpenDB(&prepared.options)
	if err := gormcore.ConfigureAndPing(ctx, sqlDB, prepared.pool); err != nil {
		return nil, nil, fmt.Errorf("clickhousex: ping endpoint %q: %w", prepared.name, err)
	}

	// 跳过方言版本探测，初始化只执行驱动连接与验活。
	// 保留默认事务：方言通过 Prepare/Exec 追加批次，驱动在 Commit 时才发送。
	db, err := gormcore.Open(sqlDB, gormclickhouse.New(gormclickhouse.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), newTraceLogger(cfg, endpointType, prepared.name))
	if err != nil {
		return nil, nil, fmt.Errorf("clickhousex: initialize endpoint %q: %w", prepared.name, err)
	}
	return db, sqlDB, nil
}

// MasterDB 返回绑定 ctx 的主库会话。
func (c *Client) MasterDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("clickhousex: nil context")
	}
	return c.cluster.Master(ctx)
}

// SlaveDB 返回绑定 ctx 的从库会话；未配置从库时使用主库。
func (c *Client) SlaveDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("clickhousex: nil context")
	}
	return c.cluster.Slave(ctx)
}

// Close 关闭所有连接池；重复调用返回首次关闭结果。
func (c *Client) Close() error {
	return c.cluster.Close()
}
