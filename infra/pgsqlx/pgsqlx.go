package pgsqlx

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Pool 配置单个端点的连接池。零值分别采用 100、min(10, MaxOpenConns)、
// 3 分钟和 1 分钟；负值无效，空闲数不能超过打开数。
type Pool = gormcore.PoolConfig

// Endpoint 表示一个主库或从库的 TCP 连接配置。
type Endpoint struct {
	// Host 是必填的主机名或 IP 地址，不包含端口。
	Host string
	// Port 是 TCP 端口，零值使用 5432；有效范围为 1 到 65535。
	Port int
	// Database 是必填的数据库名。
	Database string
	// Username 是必填的数据库用户名。
	Username string
	// Password 是连接密码，可以为空；调用方不要将其写入日志。
	Password string
	// Charset 是客户端编码；空值或 UTF8/UTF-8 均使用 UTF8，其他编码无效。
	Charset string
	// Location 解释无时区 timestamp 的 Go 时区；空值使用 Asia/Shanghai。
	Location string
	// SSLMode 是 pgx 支持的 TLS 模式，空值使用 prefer。
	SSLMode string
	// SSLRootCert、SSLCert、SSLKey 是 pgx 使用的证书文件路径。
	SSLRootCert string
	SSLCert     string
	SSLKey      string
	// DialTimeout 是建立连接的超时，零值不设置驱动级超时。
	DialTimeout time.Duration
	// Pool 是此端点的连接池配置，零值使用 Pool 的默认值。
	Pool Pool
}

// Config 配置一个逻辑 PostgreSQL 数据库及其日志行为。
type Config struct {
	// Name 是必填的逻辑库名，用作日志中的下游标识。
	Name string
	// Master 是必填的主库连接配置，MasterDB 使用它。
	Master Endpoint
	// Slaves 是可选的从库配置；SlaveDB 轮询选择，未配置时回退主库。
	Slaves []Endpoint
	// SessionTimeZone 设置新连接的 PostgreSQL 会话时区；空值沿用服务端默认值。
	SessionTimeZone string
	// SlowThreshold 是慢查询阈值，零值默认 200 毫秒；无其他错误且耗时严格超过阈值时记录 Warn。
	SlowThreshold time.Duration
	// LogSQL 控制正常查询是否记录带 SQL 的 Info 日志；慢查询和错误始终包含 SQL。
	LogSQL bool
	// InterpolateSQL 控制日志 SQL 是否用参数值替换占位符；开启后可能记录业务数据。
	InterpolateSQL bool
	// DisableStatementCache 为不支持预备语句缓存的代理选择 pgx exec 模式。
	DisableStatementCache bool
}

// Client 持有一个主库及零个或多个从库的 GORM 连接池。
type Client struct {
	cluster gormcore.Cluster
}

type preparedEndpoint struct {
	name     string
	pool     Pool
	driver   *pgx.ConnConfig
	location *time.Location
}

// New 创建并验活全部端点；初始化沿用驱动超时，失败时关闭已创建的连接池。
// 客户端由调用方通过 Close 显式关闭，查询时再传入操作 context。
func New(cfg Config) (client *Client, err error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("pgsqlx: empty database name")
	}
	if cfg.SlowThreshold < 0 {
		return nil, errors.New("pgsqlx: negative slow threshold")
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}

	master, err := prepare(cfg.Master, "master", cfg.SessionTimeZone, cfg.DisableStatementCache)
	if err != nil {
		return nil, err
	}
	slaves := make([]preparedEndpoint, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaves[i], err = prepare(slave, "slave-"+strconv.Itoa(i+1), cfg.SessionTimeZone, cfg.DisableStatementCache)
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

func prepare(endpoint Endpoint, name, sessionTimeZone string, disableStatementCache bool) (preparedEndpoint, error) {
	if endpoint.Host == "" || endpoint.Database == "" || endpoint.Username == "" {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q requires host, database and username", name)
	}
	if endpoint.Port < 0 || endpoint.Port > 65535 {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q has invalid port", name)
	}
	port := endpoint.Port
	if port == 0 {
		port = 5432
	}
	if endpoint.DialTimeout < 0 {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q has negative dial timeout", name)
	}
	pool, err := normalizePool(endpoint.Pool)
	if err != nil {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q: %w", name, err)
	}
	locationName := endpoint.Location
	if locationName == "" {
		locationName = "Asia/Shanghai"
	}
	location, err := time.LoadLocation(locationName)
	if err != nil {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q has invalid location: %w", name, err)
	}

	if endpoint.Charset != "" && !strings.EqualFold(endpoint.Charset, "UTF8") && !strings.EqualFold(endpoint.Charset, "UTF-8") {
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q has unsupported charset", name)
	}

	sslMode := endpoint.SSLMode
	if sslMode == "" {
		sslMode = "prefer"
	}
	params := url.Values{"sslmode": {sslMode}}
	if endpoint.SSLRootCert != "" {
		params.Set("sslrootcert", endpoint.SSLRootCert)
	}
	if endpoint.SSLCert != "" {
		params.Set("sslcert", endpoint.SSLCert)
	}
	if endpoint.SSLKey != "" {
		params.Set("sslkey", endpoint.SSLKey)
	}
	uri := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(endpoint.Username, endpoint.Password),
		Host:     net.JoinHostPort(endpoint.Host, strconv.Itoa(port)),
		Path:     "/" + endpoint.Database,
		RawPath:  "/" + url.PathEscape(endpoint.Database),
		RawQuery: params.Encode(),
	}
	driver, err := pgx.ParseConfig(uri.String())
	if err != nil {
		// 解析错误外层包含连接 URI，只输出不含该 URI 的底层原因。
		if parseErr, ok := errors.AsType[*pgconn.ParseConfigError](err); ok && parseErr.Unwrap() != nil {
			return preparedEndpoint{}, initializationError("configure", name, parseErr.Unwrap())
		}
		return preparedEndpoint{}, fmt.Errorf("pgsqlx: endpoint %q has invalid connection settings", name)
	}
	if endpoint.DialTimeout != 0 {
		driver.ConnectTimeout = endpoint.DialTimeout
	}
	if driver.RuntimeParams == nil {
		driver.RuntimeParams = make(map[string]string)
	}
	// pgx 使用 Go UTF8 字符串；显式指定客户端编码，避免继承不兼容的服务端或环境默认值。
	driver.RuntimeParams["client_encoding"] = "UTF8"
	if sessionTimeZone != "" {
		driver.RuntimeParams["timezone"] = sessionTimeZone
	}
	if disableStatementCache {
		driver.DefaultQueryExecMode = pgx.QueryExecModeExec
	}
	return preparedEndpoint{
		name:     name,
		pool:     pool,
		driver:   driver,
		location: location,
	}, nil
}

// registerTimestampLocation 仅改变无时区 timestamp 的解码；每条物理连接有独立的类型表。
func registerTimestampLocation(typeMap *pgtype.Map, location *time.Location) {
	typeMap.RegisterType(&pgtype.Type{
		Name: "timestamp",
		OID:  pgtype.TimestampOID,
		Codec: &pgtype.TimestampCodec{
			ScanLocation: location,
		},
	})
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
	sqlDB := stdlib.OpenDB(*prepared.driver, stdlib.OptionAfterConnect(func(_ context.Context, conn *pgx.Conn) error {
		registerTimestampLocation(conn.TypeMap(), prepared.location)
		return nil
	}))
	if err := gormcore.ConfigureAndPing(ctx, sqlDB, prepared.pool); err != nil {
		return nil, nil, initializationError("ping", prepared.name, err)
	}
	db, err := gormcore.Open(sqlDB, gormpostgres.New(gormpostgres.Config{
		Conn: sqlDB,
	}), newTraceLogger(cfg, endpointType, prepared.name))
	if err != nil {
		return nil, nil, initializationError("initialize", prepared.name, err)
	}
	return db, sqlDB, nil
}

// 不保留含凭据的驱动配置；context 和 SQLSTATE 维持原有语义，网络与 TLS 错误保留具体原因。
func initializationError(stage, endpoint string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("pgsqlx: %s endpoint %q: %w", stage, endpoint, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("pgsqlx: %s endpoint %q: %w", stage, endpoint, context.DeadlineExceeded)
	}
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		return fmt.Errorf("pgsqlx: %s endpoint %q: SQLSTATE %s", stage, endpoint, pgErr.SQLState())
	}
	if connectErr, ok := errors.AsType[*pgconn.ConnectError](err); ok {
		err = connectErr.Unwrap()
	}
	return fmt.Errorf("pgsqlx: %s endpoint %q: %s", stage, endpoint, err)
}

// MasterDB 返回带 ctx 的主库会话。
func (c *Client) MasterDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("pgsqlx: nil context")
	}
	return c.cluster.Master(ctx)
}

// SlaveDB 返回带 ctx 的从库会话；未配置从库时使用主库。
func (c *Client) SlaveDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("pgsqlx: nil context")
	}
	return c.cluster.Slave(ctx)
}

// Close 关闭所有连接池；重复调用返回首次关闭结果。
func (c *Client) Close() error {
	return c.cluster.Close()
}
