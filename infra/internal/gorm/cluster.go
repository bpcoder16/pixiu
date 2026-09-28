package gorm

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"

	gormlib "gorm.io/gorm"
)

// Cluster 持有 GORM 主从会话及其底层连接池。初始化阶段登记，查询阶段只读。
type Cluster struct {
	master    *gormlib.DB
	slaves    []*gormlib.DB
	pools     []*sql.DB
	nextSlave atomic.Uint64
	close     sync.Once
	closeErr  error
}

// setMaster 登记主库；仅在启动阶段调用一次。
func (c *Cluster) setMaster(db *gormlib.DB, pool *sql.DB) {
	c.master = db
	if pool != nil {
		c.pools = append(c.pools, pool)
	}
}

// addSlave 按配置顺序登记从库；仅在启动阶段调用。
func (c *Cluster) addSlave(db *gormlib.DB, pool *sql.DB) {
	c.slaves = append(c.slaves, db)
	if pool != nil {
		c.pools = append(c.pools, pool)
	}
}

// BuildCluster 依次打开主库与从库；回调失败时关闭已登记的连接池。
// 回调失败时若返回了连接池，由此函数关闭；否则回调须自行关闭尚未交出的连接池。
// 构建失败后的 Cluster 不可重用。
func BuildCluster[T any](
	ctx context.Context,
	cluster *Cluster,
	master T,
	slaves []T,
	open func(context.Context, T, string) (*gormlib.DB, *sql.DB, error),
) (err error) {
	defer func() {
		if err != nil {
			_ = cluster.Close()
		}
	}()

	masterDB, masterPool, err := open(ctx, master, "master")
	if err != nil {
		if masterPool != nil {
			_ = masterPool.Close()
		}
		return err
	}
	cluster.setMaster(masterDB, masterPool)
	for _, slave := range slaves {
		db, pool, openErr := open(ctx, slave, "slave")
		if openErr != nil {
			if pool != nil {
				_ = pool.Close()
			}
			return openErr
		}
		cluster.addSlave(db, pool)
	}
	return nil
}

// Master 返回绑定请求 context 的主库 GORM 会话；调用方保证 ctx 非 nil。
func (c *Cluster) Master(ctx context.Context) *gormlib.DB {
	return c.master.WithContext(ctx)
}

// Slave 返回绑定请求 context 的从库会话；无从库时回退主库。
func (c *Cluster) Slave(ctx context.Context) *gormlib.DB {
	if len(c.slaves) == 0 {
		return c.master.WithContext(ctx)
	}
	index := (c.nextSlave.Add(1) - 1) % uint64(len(c.slaves))
	return c.slaves[index].WithContext(ctx)
}

// Close 关闭所有连接池；重复调用返回首次关闭结果。
func (c *Cluster) Close() error {
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
