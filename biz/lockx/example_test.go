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
