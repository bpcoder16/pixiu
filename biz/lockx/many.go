package lockx

import (
	"context"
	"errors"
	"strings"
)

// TryManyLocker 提供整批非阻塞获取，是 Backend 必须实现的能力。
// 只有全部 key 获取成功才交付一个管理整批占用的 Lock；竞争返回 nil、false、nil。
// 错误或不确定结果不能交付句柄；后端负责清理已确认的占用，异常遗留由租期回收。
type TryManyLocker interface {
	TryLockMany(ctx context.Context, keys []string) (Lock, bool, error)
}

// TryDoMany 只尝试一次整批获取，全部成功后同步执行 fn；竞争失败不执行回调。
// keys 须非空且不能包含空白 key；按原值复制去重，不修剪或改变大小写。
// executed 只表示进入过回调；租期、取消、错误合并与 panic 清理沿用 TryDo。
func TryDoMany(ctx context.Context, locker TryManyLocker, keys []string, fn func(context.Context) error) (bool, error) {
	if locker == nil {
		return false, errors.New("lockx: nil locker")
	}
	if len(keys) == 0 {
		return false, errors.New("lockx: empty keys")
	}
	unique := make([]string, 0, len(keys))
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			return false, errors.New("lockx: empty key")
		}
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			unique = append(unique, key)
		}
	}
	return run(ctx, fn, func() (Lock, bool, error) {
		return locker.TryLockMany(ctx, unique)
	})
}

// TryDoMany 通过绑定的 Backend 整批获取，沿用包级 TryDoMany 的执行与清理语义。
func (e *Executor) TryDoMany(ctx context.Context, keys []string, fn func(context.Context) error) (bool, error) {
	if e == nil || e.backend == nil {
		return false, errors.New("lockx: uninitialized executor")
	}
	return TryDoMany(ctx, e.backend, keys, fn)
}
