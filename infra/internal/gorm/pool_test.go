package gorm

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	_ "github.com/mattn/go-sqlite3"
	gormsqlite "gorm.io/driver/sqlite"
	gormlib "gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 模拟网络期限已到，但 context 取消计时器尚未调度的窗口。
type pendingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c pendingDeadlineContext) Deadline() (time.Time, bool) {
	return c.deadline, true
}

type pingTestConnector struct {
	conn *pingTestConn
}

func (c pingTestConnector) Connect(context.Context) (driver.Conn, error) {
	return c.conn, nil
}

func (c pingTestConnector) Driver() driver.Driver { return c }

func (c pingTestConnector) Open(string) (driver.Conn, error) { return c.conn, nil }

type pingTestConn struct {
	ping    func(context.Context) error
	onClose func()
	closed  atomic.Bool
}

func (c *pingTestConn) Ping(ctx context.Context) error { return c.ping(ctx) }

func (c *pingTestConn) Close() error {
	c.closed.Store(true)
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func (*pingTestConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("测试连接不支持 Prepare")
}

func (*pingTestConn) Begin() (driver.Tx, error) {
	return nil, errors.New("测试连接不支持事务")
}

func TestConfigureAndPingErrorClassification(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		due := pendingDeadlineContext{
			Context:  context.Background(),
			deadline: now,
		}
		expired := pendingDeadlineContext{
			Context:  context.Background(),
			deadline: now.Add(-time.Second),
		}
		future := pendingDeadlineContext{
			Context:  context.Background(),
			deadline: now.Add(time.Hour),
		}
		timeoutErr := fmt.Errorf("ping: %w", &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: os.ErrDeadlineExceeded,
		})
		databaseErr := errors.New("数据库拒绝验活")
		networkErr := &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: errors.New("连接被关闭"),
		}
		for _, tc := range []struct {
			name   string
			ctx    context.Context
			ping   error
			cancel bool
			want   error
		}{
			{
				name: "恰好到期且取消尚未调度",
				ctx:  due,
				ping: timeoutErr,
				want: context.DeadlineExceeded,
			},
			{
				name: "已到期且取消尚未调度",
				ctx:  expired,
				ping: timeoutErr,
				want: context.DeadlineExceeded,
			},
			{
				name: "初始化期限未到的网络超时",
				ctx:  future,
				ping: timeoutErr,
				want: timeoutErr,
			},
			{
				name: "无初始化期限的网络超时",
				ctx:  context.Background(),
				ping: timeoutErr,
				want: timeoutErr,
			},
			{
				name: "已到期但属于数据库错误",
				ctx:  expired,
				ping: databaseErr,
				want: databaseErr,
			},
			{
				name: "已到期但属于其他网络错误",
				ctx:  expired,
				ping: networkErr,
				want: networkErr,
			},
			{
				name:   "主动取消优先于超时归一化",
				ctx:    expired,
				ping:   timeoutErr,
				cancel: true,
				want:   context.Canceled,
			},
			{
				name: "不将成功验活改成超时",
				ctx:  expired,
			},
		} {
			func() {
				ctx, cancel := context.WithCancel(tc.ctx)
				defer cancel()
				conn := &pingTestConn{
					ping: func(context.Context) error {
						if tc.cancel {
							cancel()
						}
						return tc.ping
					},
				}
				pool := sql.OpenDB(pingTestConnector{conn: conn})
				defer pool.Close()
				err := ConfigureAndPing(ctx, pool, PoolConfig{
					MaxOpenConns: 1,
					MaxIdleConns: 1,
				})
				if !errors.Is(err, tc.want) {
					t.Errorf("%s: 验活错误分类不符: got=%v want=%v", tc.name, err, tc.want)
				}
				if conn.closed.Load() != (tc.want != nil) {
					t.Errorf("%s: 验活后连接关闭状态不符: closed=%v err=%v", tc.name, conn.closed.Load(), err)
				}
				if tc.want != nil && pool.Stats().OpenConnections != 0 {
					t.Errorf("%s: 验活失败后连接池仍持有连接", tc.name)
				}
			}()
		}
	})
}

func TestConfigureAndPingClassifiesBeforeCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		timeoutErr := &net.OpError{
			Op:  "read",
			Net: "tcp",
			Err: os.ErrDeadlineExceeded,
		}
		conn := &pingTestConn{
			ping: func(context.Context) error { return timeoutErr },
			onClose: func() {
				// 清理跨过初始化期限，也不能覆盖此前发生的独立网络超时。
				time.Sleep(2 * time.Second)
			},
		}
		pool := sql.OpenDB(pingTestConnector{conn: conn})
		defer pool.Close()
		err := ConfigureAndPing(ctx, pool, PoolConfig{
			MaxOpenConns: 1,
			MaxIdleConns: 1,
		})
		if !errors.Is(err, timeoutErr) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("清理耗时改变了验活错误分类: %v", err)
		}
		if !conn.closed.Load() {
			t.Fatal("验活失败后连接未关闭")
		}
	})
}

func TestOpenKeepsDefaultsAndAllowsExtraConfig(t *testing.T) {
	diagnostic := logger.Default.LogMode(logger.Silent)
	newPool := func() *sql.DB {
		pool, err := sql.Open("sqlite3", ":memory:")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = pool.Close() })
		return pool
	}

	legacyPool := newPool()
	legacy, err := Open(legacyPool, gormsqlite.New(gormsqlite.Config{Conn: legacyPool}), diagnostic)
	if err != nil {
		t.Fatal(err)
	}
	if !legacy.Config.DisableAutomaticPing || legacy.Config.SkipDefaultTransaction || legacy.Config.Logger != diagnostic {
		t.Fatalf("旧调用配置不兼容: %+v", legacy.Config)
	}

	configuredPool := newPool()
	configured, err := Open(configuredPool, gormsqlite.New(gormsqlite.Config{Conn: configuredPool}), diagnostic, func(config *gormlib.Config) {
		config.SkipDefaultTransaction = true
		config.DisableAutomaticPing = false
		config.Logger = logger.Default
	})
	if err != nil {
		t.Fatal(err)
	}
	if !configured.Config.SkipDefaultTransaction || !configured.Config.DisableAutomaticPing || configured.Config.Logger != diagnostic {
		t.Fatalf("扩展配置未保留共享约束: %+v", configured.Config)
	}
}
