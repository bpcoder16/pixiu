package gorm

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"time"

	gormlib "gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// PoolConfig 是数据库模块共用的连接池配置；零值默认值由使用它的模块决定。
type PoolConfig struct {
	// MaxOpenConns 限制同时打开的连接数；零值使用模块默认值。
	MaxOpenConns int
	// MaxIdleConns 限制保留的空闲连接数；零值使用模块默认值。
	MaxIdleConns int
	// ConnMaxLifetime 限制连接可被复用的最长时间；零值使用模块默认值。
	ConnMaxLifetime time.Duration
	// ConnMaxIdleTime 限制连接的空闲时间；零值使用模块默认值。
	ConnMaxIdleTime time.Duration
}

// NormalizePool 校验参数并应用模块默认值；默认空闲数不超过生效后的打开数。
func NormalizePool(pool, defaults PoolConfig) (PoolConfig, error) {
	if pool.MaxOpenConns < 0 || pool.MaxIdleConns < 0 || pool.ConnMaxLifetime < 0 || pool.ConnMaxIdleTime < 0 {
		return PoolConfig{}, errors.New("negative pool setting")
	}
	if pool.MaxOpenConns == 0 {
		pool.MaxOpenConns = defaults.MaxOpenConns
	}
	if pool.MaxIdleConns == 0 {
		pool.MaxIdleConns = min(defaults.MaxIdleConns, pool.MaxOpenConns)
	}
	if pool.MaxIdleConns > pool.MaxOpenConns {
		return PoolConfig{}, errors.New("max idle connections exceed max open connections")
	}
	if pool.ConnMaxLifetime == 0 {
		pool.ConnMaxLifetime = defaults.ConnMaxLifetime
	}
	if pool.ConnMaxIdleTime == 0 {
		pool.ConnMaxIdleTime = defaults.ConnMaxIdleTime
	}
	return pool, nil
}

// ConfigureAndPing 设置池参数并验活；验活失败时关闭连接池。
// context 错误优先；网络超时且初始化期限已到时统一返回 context.DeadlineExceeded。
func ConfigureAndPing(ctx context.Context, pool *sql.DB, cfg PoolConfig) error {
	pool.SetMaxOpenConns(cfg.MaxOpenConns)
	pool.SetMaxIdleConns(cfg.MaxIdleConns)
	pool.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	pool.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	if err := pool.PingContext(ctx); err != nil {
		// 部分驱动将 Ping 取消转换为 ErrBadConn，优先保留初始化超时的原因。
		if ctxErr := ctx.Err(); ctxErr != nil {
			err = ctxErr
		} else if timeoutErr, ok := errors.AsType[net.Error](err); ok && timeoutErr.Timeout() {
			// socket deadline 可能先于 context 计时器生效，只归一化期限已到的网络超时。
			if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
				err = context.DeadlineExceeded
			}
		}
		// 先确定验活错误，再清理，避免关闭耗时使独立网络超时被误判为初始化超时。
		_ = pool.Close()
		return err
	}
	return nil
}

// Open 用已验活的连接池初始化 GORM；可选配置不能覆盖共享的 Logger 和禁用自动 Ping 约定。
// 方言初始化失败时关闭连接池。
func Open(pool *sql.DB, dialector gormlib.Dialector, diagnostic logger.Interface, configure ...func(*gormlib.Config)) (*gormlib.DB, error) {
	config := &gormlib.Config{}
	for _, apply := range configure {
		apply(config)
	}
	config.DisableAutomaticPing = true
	config.Logger = diagnostic
	db, err := gormlib.Open(dialector, config)
	if err != nil {
		_ = pool.Close()
	}
	return db, err
}
