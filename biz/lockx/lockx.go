package lockx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx/internal/lockctx"
)

// Locker 定义分布式阻塞锁接口。
// 锁被占用时等待其他持有者释放；等待上限由实现配置与 ctx 共同约束。
type Locker interface {
	Lock(ctx context.Context, key string) (Lock, error)
}

// TryLocker 定义分布式非阻塞锁接口。
// 只尝试一次，锁被占用时返回竞争失败，不等待其他持有者释放。
// 非阻塞仅针对锁竞争，连接池与网络操作仍可能等待。
type TryLocker interface {
	// TryLock 成功返回句柄、true、nil；确认竞争失败返回 nil、false、nil。
	// 取消、下游错误和结果不确定返回 nil、false、error。
	TryLock(ctx context.Context, key string) (Lock, bool, error)
}

// Lock 是一次独立获取的锁句柄，持有身份由实现保存。
type Lock interface {
	// Deadline 返回保守的本地工作期限，不包含获取前的竞争等待。
	// 会话锁没有固定租期，返回零值、false；有限租期包含成功尝试的耗时。
	Deadline() (time.Time, bool)
	// Unlock 原子校验本次身份；过期或不再持有返回 ErrNotHeld。
	// 允许并发调用，正常下游情况下重复释放返回 ErrNotHeld。
	Unlock(ctx context.Context) error
}

// ErrNotHeld 表示本次锁已过期、已释放或已由其他执行者持有。
var ErrNotHeld = errors.New("lockx: lock not held")

// Do 等待获取锁，同步执行 fn 并释放；获取失败不执行 fn。
// 工作期限取 ctx 与句柄期限的较早者，取消不能强制终止 fn。
// 释放脱离请求取消并独立限时 1 秒；正常路径合并错误，panic 时继续原始 panic。
func Do(ctx context.Context, locker Locker, key string, fn func(context.Context) error) error {
	if locker == nil {
		return errors.New("lockx: nil locker")
	}
	_, err := run(ctx, key, fn, func() (Lock, bool, error) {
		lock, err := locker.Lock(ctx, key)
		return lock, err == nil, err
	})
	return err
}

// TryDo 尝试获取锁，成功后同步执行 fn 并释放；竞争失败返回 false、nil。
// executed 表示是否进入过 fn，不表示业务是否提交成功；其余语义与 Do 相同。
func TryDo(ctx context.Context, locker TryLocker, key string, fn func(context.Context) error) (bool, error) {
	if locker == nil {
		return false, errors.New("lockx: nil locker")
	}
	return run(ctx, key, fn, func() (Lock, bool, error) {
		return locker.TryLock(ctx, key)
	})
}

func run(
	ctx context.Context,
	key string,
	fn func(context.Context) error,
	acquire func() (Lock, bool, error),
) (executed bool, err error) {
	if ctx == nil {
		return false, errors.New("lockx: nil context")
	}
	if fn == nil {
		return false, errors.New("lockx: nil function")
	}
	if strings.TrimSpace(key) == "" {
		return false, errors.New("lockx: empty key")
	}
	if err := lockctx.Err(ctx); err != nil {
		return false, err
	}
	lock, acquired, err := acquire()
	if err != nil {
		return false, fmt.Errorf("lockx: acquire: %w", err)
	}
	if !acquired {
		return false, lockctx.Err(ctx)
	}
	if lock == nil {
		return false, errors.New("lockx: acquired without handle")
	}
	// 先登记释放；晚到的成功也需要清理，panic 时同样执行。
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		defer cancel()
		if unlockErr := lock.Unlock(unlockCtx); unlockErr != nil {
			err = errors.Join(err, fmt.Errorf("lockx: unlock: %w", unlockErr))
		}
	}()
	workCtx := ctx
	if deadline, ok := lock.Deadline(); ok {
		var cancel context.CancelFunc
		workCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	if err := lockctx.Err(workCtx); err != nil {
		return false, err
	}
	executed = true
	err = fn(workCtx)
	// 在释放前检查期限，避免将释放耗时误算成业务超时。
	return executed, errors.Join(err, lockctx.Err(workCtx))
}
