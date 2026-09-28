package sqlitex

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	gormsqlite "gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// JournalMode 是 SQLite 支持的日志模式；空值沿用 DSN 或驱动默认值。
type JournalMode string

const (
	JournalModeDelete   JournalMode = "DELETE"
	JournalModeTruncate JournalMode = "TRUNCATE"
	JournalModePersist  JournalMode = "PERSIST"
	JournalModeMemory   JournalMode = "MEMORY"
	JournalModeWAL      JournalMode = "WAL"
	JournalModeOff      JournalMode = "OFF"
)

// SynchronousMode 是 SQLite 支持的同步级别；空值沿用 DSN 或驱动默认值。
type SynchronousMode string

const (
	SynchronousOff    SynchronousMode = "OFF"
	SynchronousNormal SynchronousMode = "NORMAL"
	SynchronousFull   SynchronousMode = "FULL"
	SynchronousExtra  SynchronousMode = "EXTRA"
)

// Pool 配置 SQLite 连接池。打开数和空闲数零值默认 1；连接寿命与空闲时间
// 零值不主动回收。负值无效，空闲数不能超过打开数。
type Pool = gormcore.PoolConfig

// Config 配置一个逻辑 SQLite 数据库及其日志行为。
type Config struct {
	// Name 是必填的逻辑库名，用作日志中的下游标识。
	Name string
	// DSN 是必填的 SQLite 文件路径或驱动连接字符串；显式配置项优先于同名 DSN 参数。
	DSN string
	// JournalMode 设置 SQLite 日志模式；空值沿用 DSN 或驱动默认值。
	JournalMode JournalMode
	// BusyTimeout 设置锁等待时间，精度为毫秒；零值沿用 DSN 或驱动默认值。
	BusyTimeout time.Duration
	// Synchronous 设置同步级别；空值沿用 DSN 或驱动默认值。
	Synchronous SynchronousMode
	// ForeignKeys 设置是否启用外键；nil 沿用 DSN 或驱动默认值。
	ForeignKeys *bool
	// Pool 配置连接池；私有内存库只能使用一个打开连接，内存库不能设置回收时间。
	Pool Pool
	// SlowThreshold 是慢查询阈值，零值默认 200 毫秒。
	SlowThreshold time.Duration
	// LogSQL 控制是否为正常查询记录包含 SQL 的 Info 日志；慢查询和错误始终包含 SQL。
	LogSQL bool
	// InterpolateSQL 控制日志 SQL 是否用参数值替换占位符；开启后可能记录业务数据。
	InterpolateSQL bool
}

// Client 持有一个可并发使用的 GORM SQLite 连接池。
type Client struct {
	db       *gorm.DB
	pool     *sql.DB
	close    sync.Once
	closeErr error
}

// New 创建连接池并用 ctx 验活；失败时关闭已创建的连接池。
func New(ctx context.Context, cfg Config) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("sqlitex: nil context")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("sqlitex: empty database name")
	}
	if cfg.DSN == "" {
		return nil, errors.New("sqlitex: empty DSN")
	}
	if cfg.SlowThreshold < 0 {
		return nil, errors.New("sqlitex: negative slow threshold")
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}
	poolCfg, err := normalizePool(cfg.Pool)
	if err != nil {
		return nil, err
	}
	isMemory, isPrivate := classifyMemoryDSN(cfg.DSN)
	if isMemory && (poolCfg.ConnMaxLifetime > 0 || poolCfg.ConnMaxIdleTime > 0) {
		return nil, errors.New("sqlitex: in-memory database cannot recycle connections")
	}
	if poolCfg.MaxOpenConns > 1 && isPrivate {
		return nil, errors.New("sqlitex: private in-memory database requires one open connection")
	}
	dsn, err := configureDSN(cfg, isMemory)
	if err != nil {
		return nil, err
	}

	pool, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlitex: open database: %w", err)
	}
	if err := gormcore.ConfigureAndPing(ctx, pool, poolCfg); err != nil {
		return nil, fmt.Errorf("sqlitex: ping database %q: %w", cfg.Name, err)
	}

	// 方言的版本探测使用 Background；连接池仍由 New 持有并负责失败清理。
	db, err := gormcore.Open(pool, gormsqlite.New(gormsqlite.Config{
		Conn: pool,
	}), newTraceLogger(cfg))
	if err != nil {
		return nil, fmt.Errorf("sqlitex: initialize database %q: %w", cfg.Name, err)
	}
	return &Client{db: db, pool: pool}, nil
}

func configureDSN(cfg Config, isMemory bool) (string, error) {
	switch cfg.JournalMode {
	case "", JournalModeDelete, JournalModeTruncate, JournalModePersist, JournalModeMemory, JournalModeWAL, JournalModeOff:
	default:
		return "", errors.New("sqlitex: invalid journal mode")
	}
	if isMemory && cfg.JournalMode == JournalModeWAL {
		return "", errors.New("sqlitex: WAL requires a file database")
	}
	switch cfg.Synchronous {
	case "", SynchronousOff, SynchronousNormal, SynchronousFull, SynchronousExtra:
	default:
		return "", errors.New("sqlitex: invalid synchronous mode")
	}
	if cfg.BusyTimeout < 0 || cfg.BusyTimeout%time.Millisecond != 0 || cfg.BusyTimeout/time.Millisecond > 1<<31-1 {
		return "", errors.New("sqlitex: invalid busy timeout")
	}
	if cfg.JournalMode == "" && cfg.Synchronous == "" && cfg.BusyTimeout == 0 && cfg.ForeignKeys == nil {
		return cfg.DSN, nil
	}

	name, rawQuery, _ := strings.Cut(cfg.DSN, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", fmt.Errorf("sqlitex: invalid DSN parameters: %w", err)
	}
	// 驱动同时支持长短参数名；显式字段覆盖 DSN 中的两种写法。
	if cfg.JournalMode != "" {
		query.Del("_journal")
		query.Set("_journal_mode", string(cfg.JournalMode))
	}
	if cfg.Synchronous != "" {
		query.Del("_sync")
		query.Set("_synchronous", string(cfg.Synchronous))
	}
	if cfg.BusyTimeout > 0 {
		query.Del("_timeout")
		query.Set("_busy_timeout", strconv.FormatInt(cfg.BusyTimeout.Milliseconds(), 10))
	}
	if cfg.ForeignKeys != nil {
		query.Del("_fk")
		query.Set("_foreign_keys", strconv.FormatBool(*cfg.ForeignKeys))
	}
	return name + "?" + query.Encode(), nil
}

func normalizePool(pool Pool) (Pool, error) {
	normalized, err := gormcore.NormalizePool(pool, gormcore.PoolConfig{
		MaxOpenConns: 1,
		MaxIdleConns: 1,
	})
	if err != nil {
		return Pool{}, fmt.Errorf("sqlitex: %w", err)
	}
	return normalized, nil
}

// 私有内存库的每条物理连接各自持有一份数据；所有内存库都会在最后一条连接关闭后消失。
func classifyMemoryDSN(dsn string) (memory, private bool) {
	name, rawQuery, _ := strings.Cut(dsn, "?")
	if name == ":memory:" {
		return true, true
	}
	if name != "file::memory:" && !strings.HasPrefix(name, "file:") {
		return false, false
	}
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return false, false
	}
	if name == "file::memory:" || query.Get("mode") == "memory" {
		return true, query.Get("cache") != "shared"
	}
	return false, false
}

// DB 返回绑定 ctx 的 GORM 会话。
func (c *Client) DB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("sqlitex: nil context")
	}
	return c.db.WithContext(ctx)
}

// Close 关闭连接池；重复调用返回首次关闭结果。
func (c *Client) Close() error {
	c.close.Do(func() {
		c.closeErr = c.pool.Close()
	})
	return c.closeErr
}
