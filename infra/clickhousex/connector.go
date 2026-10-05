package clickhousex

import (
	"context"
	"crypto/tls"
	"database/sql/driver"
	"net"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// 原生驱动的拨号和握手未完整遵循 context；只在 Connect 期间绑定取消，避免影响已建立的连接。
type nativeConnector struct {
	driver.Connector
	options clickhouse.Options
}

func (c nativeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	options := c.options
	var rawConn net.Conn
	var stop func() bool
	options.DialContext = func(dialCtx context.Context, addr string) (net.Conn, error) {
		timeout := options.DialTimeout
		if timeout == 0 {
			timeout = 30 * time.Second // 与 ClickHouse 驱动的默认建连超时一致。
		}
		dialer := &net.Dialer{Timeout: timeout}
		var err error
		if options.TLS != nil {
			rawConn, err = (&tls.Dialer{NetDialer: dialer, Config: options.TLS}).DialContext(dialCtx, "tcp", addr)
		} else {
			rawConn, err = dialer.DialContext(dialCtx, "tcp", addr)
		}
		if err == nil {
			conn := rawConn
			// 驱动握手会重设 socket deadline；取消时直接关闭连接才能保证初始化预算。
			stop = context.AfterFunc(dialCtx, func() { _ = conn.Close() })
		}
		return rawConn, err
	}
	conn, err := clickhouse.Connector(&options).Connect(ctx)
	if stop != nil {
		stop()
		stop = nil
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		if conn != nil {
			_ = conn.Close()
		} else if rawConn != nil {
			_ = rawConn.Close()
		}
		return nil, err
	}
	return conn, nil
}
