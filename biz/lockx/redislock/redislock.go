package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	randmath "math/rand/v2"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/biz/lockx/internal/lockctx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

// Config 配置阻塞锁（Lock）与非阻塞锁（TryLock），不自动续租。
type Config struct {
	// TTL 是阻塞锁（Lock）和非阻塞锁（TryLock）共用的持有租期。
	// 零值默认 1 分钟，负值无效。
	// Redis 无条件按租期失效；长任务须显式配置足够的时间。
	TTL time.Duration
	// WaitTimeout 仅用于阻塞锁（Lock），限制获取阶段的总等待时间。
	// 零值默认 5 秒，负值无效；非阻塞锁（TryLock）不使用此字段。
	// 包含连接池、网络与日志耗时，不进入成功后的工作预算。
	WaitTimeout time.Duration
	// RetryInterval 仅用于阻塞锁（Lock），控制竞争失败后的等待间隔。
	// 零值默认 50ms，负值无效；非阻塞锁（TryLock）不使用此字段。
	// 实际等待加入小幅随机抖动。
	RetryInterval time.Duration
}

// Locker 基于单机 Redis 提供固定租期的阻塞与非阻塞锁，可并发复用。
// 客户端生命周期由调用方管理；零值不可用，必须通过 New 构造。
type Locker struct {
	client      *redis.Client
	ttl         time.Duration
	waitTimeout time.Duration
	interval    time.Duration
}

type lock struct {
	client   *redis.Client
	key      string
	token    string
	deadline time.Time
}

var (
	_ lockx.Locker    = (*Locker)(nil)
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
	if cfg.TTL < 0 {
		return nil, errors.New("redislock: negative ttl")
	}
	if cfg.TTL == 0 {
		cfg.TTL = time.Minute
	}
	if cfg.WaitTimeout < 0 {
		return nil, errors.New("redislock: negative wait timeout")
	}
	if cfg.WaitTimeout == 0 {
		cfg.WaitTimeout = 5 * time.Second
	}
	if cfg.RetryInterval < 0 {
		return nil, errors.New("redislock: negative retry interval")
	}
	if cfg.RetryInterval == 0 {
		cfg.RetryInterval = 50 * time.Millisecond
	}
	rdb := client.Client()
	// go-redis 将配置的 -1 归一化为 0，将配置的 0 归一化为默认重试次数。
	if rdb.Options().MaxRetries != 0 {
		return nil, errors.New("redislock: create redisx client with MaxRetries=-1")
	}
	return &Locker{
		client:      rdb,
		ttl:         cfg.TTL,
		waitTimeout: cfg.WaitTimeout,
		interval:    cfg.RetryInterval,
	}, nil
}

// Lock 等待获取，直至成功、ctx 取消或 WaitTimeout 到达；不保证公平顺序。
// 仅明确竞争失败时重试，下游错误与不确定结果立即返回。
// 先前竞争等待不消耗最终成功尝试的 TTL；等待期间不占用 Redis 连接。
func (l *Locker) Lock(ctx context.Context, key string) (lockx.Lock, error) {
	if l == nil || l.client == nil {
		return nil, errors.New("redislock: uninitialized locker")
	}
	if ctx == nil {
		return nil, errors.New("redislock: nil context")
	}
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("redislock: empty key")
	}
	waitCtx, cancel := context.WithTimeout(ctx, l.waitTimeout)
	defer cancel()
	// 等待下限和随机偏移范围只依赖固定配置，每次获取计算一次即可。
	spread := l.interval / 5
	minDelay := l.interval - spread
	jitterRange := int64(spread) + 1
	for {
		h, acquired, err := l.TryLock(waitCtx, key)
		if err != nil {
			return nil, fmt.Errorf("redislock: lock: %w", err)
		}
		if acquired {
			return h, nil
		}
		// 只在确认竞争后等待；80%~100% 抖动避免同步抢锁，计算不溢出。
		delay := minDelay + time.Duration(randmath.Int64N(jitterRange))
		timer := time.NewTimer(delay)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return nil, fmt.Errorf("redislock: wait: %w", lockctx.Err(waitCtx))
		case <-timer.C:
		}
	}
}

// TryLock 进行一次带 TTL 的原子获取，不排队、不重试、不自动续租。
// 竞争失败返回 nil、false、nil；结果不确定或本地租期预算耗尽时返回错误。
// key 原样使用，配置的 TTL 向上取整到毫秒；租期不从返回时重新起算。
// 已确认成功但因取消或超时无法交付时，使用独立的 1 秒预算尽力释放。
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
		if err == nil && result == "OK" {
			// 上层拿不到句柄，必须在此按本次身份清理，并脱离已失效的获取预算。
			unlockCtx, unlockCancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			defer unlockCancel()
			held := lock{
				client: l.client,
				key:    key,
				token:  token,
			}
			ctxErr = errors.Join(ctxErr, held.Unlock(unlockCtx))
		}
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

// Unlock 原子校验本次身份并幂等释放；正常下游响应下可重复或并发调用。
// 返回 nil 只表示本次身份已无占用需要释放，不代表业务仍处于有效租期内。
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
		// key 已过期、已释放或身份已改变时，本次释放已完成；不能删除新锁。
		return nil
	default:
		return errors.New("redislock: unexpected unlock result")
	}
}
