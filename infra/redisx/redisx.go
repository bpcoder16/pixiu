package redisx

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config 配置一个单机 Redis 目标及其命令日志。
type Config struct {
	Name    string
	Options redis.Options
	// SlowThreshold 是慢命令阈值；零值默认 200 毫秒，负数无效。
	SlowThreshold time.Duration
	// LogCommands 控制是否记录正常命令的 Debug 日志；所有已输出的业务命令日志
	// 均附加完整请求参数，可能包含 key、值和凭据。
	LogCommands bool
}

// Client 持有一个可并发使用的 Redis 客户端。
type Client struct {
	redis    *redis.Client
	close    sync.Once
	closeErr error
}

type startupPingKey struct{}

// New 创建客户端并用 ctx 验活；失败时关闭已创建的客户端。
func New(ctx context.Context, cfg Config) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("redisx: nil context")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("redisx: empty name")
	}
	if strings.TrimSpace(cfg.Options.Addr) == "" {
		return nil, errors.New("redisx: empty address")
	}
	if cfg.SlowThreshold < 0 {
		return nil, errors.New("redisx: negative slow threshold")
	}
	if err := validatePool(cfg.Options); err != nil {
		return nil, err
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}

	options := cfg.Options
	options.ContextTimeoutEnabled = true
	rdb := redis.NewClient(&options)
	rdb.AddHook(newLoggerHook(cfg.Name, cfg.SlowThreshold, cfg.LogCommands))
	c := &Client{redis: rdb}
	// 初始化 Ping 的失败交给 New 调用方统一记录，避免同一启动错误重复入日志。
	pingCtx := context.WithValue(ctx, startupPingKey{}, true)
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return nil, fmt.Errorf("redisx: ping %q: %w", cfg.Name, errors.Join(err, c.Close()))
	}
	return c, nil
}

func validatePool(options redis.Options) error {
	for _, setting := range []struct {
		name  string
		value int
	}{
		{"PoolSize", options.PoolSize},
		{"MinIdleConns", options.MinIdleConns},
		{"MaxIdleConns", options.MaxIdleConns},
		{"MaxActiveConns", options.MaxActiveConns},
	} {
		if setting.value < 0 || setting.value > math.MaxInt32 {
			return fmt.Errorf("redisx: invalid %s", setting.name)
		}
	}
	return nil
}

// Client 返回底层客户端；调用方不能自行关闭它或在运行期修改配置与 Hook。
func (c *Client) Client() *redis.Client { return c.redis }

// Close 关闭连接池；重复调用返回首次关闭结果。
func (c *Client) Close() error {
	c.close.Do(func() { c.closeErr = c.redis.Close() })
	return c.closeErr
}
