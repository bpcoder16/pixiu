package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/biz/lockx/internal/lockctx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

// Config 配置锁的固定租期；TTL 必须为正，不自动续租。
type Config struct {
	TTL time.Duration
}

// Locker 基于单机 Redis 提供固定租期的非阻塞锁，可并发复用。
// 客户端生命周期由调用方管理；零值不可用，必须通过 New 构造。
type Locker struct {
	client *redis.Client
	ttl    time.Duration
}

type lock struct {
	client   *redis.Client
	key      string
	token    string
	deadline time.Time
}

var (
	_ lockx.TryLocker = (*Locker)(nil)
	_ lockx.Lock      = (*lock)(nil)
)

// 比较与删除在同一脚本内完成，避免旧持有者删除过期后新建的锁。
const unlockScript = "if redis.call('get', KEYS[1]) == ARGV[1] then\n" +
	"    return redis.call('del', KEYS[1])\n" +
	"end\n" +
	"return 0"

// New 复用已初始化的 redisx 客户端，不创建或关闭连接池。
// client 创建时必须设置 redis.Options.MaxRetries=-1，防止响应丢失后的
// 自动重试将已经获取的锁误报为普通竞争失败。
func New(client *redisx.Client, cfg Config) (*Locker, error) {
	if client == nil || client.Client() == nil {
		return nil, errors.New("redislock: uninitialized client")
	}
	if cfg.TTL <= 0 {
		return nil, errors.New("redislock: non-positive ttl")
	}
	rdb := client.Client()
	// go-redis 将配置的 -1 归一化为 0，将配置的 0 归一化为默认重试次数。
	if rdb.Options().MaxRetries != 0 {
		return nil, errors.New("redislock: create redisx client with MaxRetries=-1")
	}
	return &Locker{
		client: rdb,
		ttl:    cfg.TTL,
	}, nil
}

// TryLock 进行一次带 TTL 的原子获取，不排队、不重试、不自动续租。
// 竞争失败返回 nil、false、nil；结果不确定或本地租期预算耗尽时返回错误。
// key 原样使用，配置的 TTL 向上取整到毫秒；租期不从返回时重新起算。
func (l *Locker) TryLock(ctx context.Context, key string) (lockx.Lock, bool, error) {
	start := time.Now()
	if l == nil || l.client == nil {
		return nil, false, errors.New("redislock: uninitialized locker")
	}
	if ctx == nil {
		return nil, false, errors.New("redislock: nil context")
	}
	if strings.TrimSpace(key) == "" {
		return nil, false, errors.New("redislock: empty key")
	}
	deadline := start.Add(l.ttl)
	attemptCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	if err := lockctx.Err(attemptCtx); err != nil {
		return nil, false, fmt.Errorf("redislock: try lock: %w", err)
	}
	// 用商和余数进位，不先做 Duration 加法；最大正 Duration 也不会溢出。
	milliseconds := int64(l.ttl / time.Millisecond)
	if l.ttl%time.Millisecond != 0 {
		milliseconds++
	}
	token := rand.Text()
	result, err := l.client.Do(attemptCtx, "SET", key, token, "NX", "PX", milliseconds).Text()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, false, fmt.Errorf("redislock: try lock: %w", errors.Join(err, lockctx.Err(attemptCtx)))
	}
	// 包括网络及日志耗时；晚到的成功不能交付为仍可使用的锁。
	if ctxErr := lockctx.Err(attemptCtx); ctxErr != nil {
		return nil, false, fmt.Errorf("redislock: try lock: %w", ctxErr)
	}
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if result != "OK" {
		return nil, false, errors.New("redislock: unexpected SET result")
	}
	return &lock{
		client:   l.client,
		key:      key,
		token:    token,
		deadline: deadline,
	}, true, nil
}

func (l *lock) Deadline() (time.Time, bool) {
	return l.deadline, true
}

func (l *lock) Unlock(ctx context.Context) error {
	if ctx == nil {
		return errors.New("redislock: nil context")
	}
	if err := lockctx.Err(ctx); err != nil {
		return fmt.Errorf("redislock: unlock: %w", err)
	}
	result, err := l.client.Eval(ctx, unlockScript, []string{l.key}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("redislock: unlock: %w", errors.Join(err, lockctx.Err(ctx)))
	}
	switch result {
	case 1:
		return nil
	case 0:
		return lockx.ErrNotHeld
	default:
		return errors.New("redislock: unexpected unlock result")
	}
}
