package lockx_test

import (
	"context"

	"github.com/bpcoder16/pixiu/biz/lockx"
)

func ExampleTryDo() {
	// 业务接收统一接口，初始化位置决定 Redis 或其他实现。
	tryConfirm := func(
		ctx context.Context,
		locker lockx.TryLocker,
		orderID string,
		confirm func(context.Context) error,
	) (bool, error) {
		return lockx.TryDo(ctx, locker, "orders:lock:confirm:"+orderID, confirm)
	}
	_ = tryConfirm
}

func ExampleDo() {
	confirm := func(ctx context.Context, locker lockx.Locker, fn func(context.Context) error) error {
		return lockx.Do(ctx, locker, "orders:confirm:123", fn)
	}
	_ = confirm
}

func ExampleNewNamed() {
	// 后端由启动流程创建；两个后端可以共享 Redis 并持有不同的租期配置。
	configure := func(normal, longTask lockx.Backend) error {
		if _, err := lockx.NewDefault("default", normal); err != nil {
			return err
		}
		_, err := lockx.NewNamed("long-task", longTask)
		return err
	}
	run := func(ctx context.Context, key string, work func(context.Context) error) error {
		return lockx.Named("long-task").Do(ctx, key, work)
	}
	_ = configure
	_ = run
}

// 批量竞争只尝试一次；业务 key 由应用按资源构造。
func ExampleExecutor_TryDoMany() {
	publish := func(ctx context.Context, keys []string, work func(context.Context) error) (bool, error) {
		return lockx.Default().TryDoMany(ctx, keys, work)
	}
	_ = publish
}
