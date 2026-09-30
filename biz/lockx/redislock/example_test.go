package redislock_test

import (
	"context"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/biz/lockx/redislock"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

func ExampleNew() {
	ctx := context.Background()
	client, err := redisx.New(ctx, redisx.Config{
		Name: "locks",
		Options: redis.Options{
			Addr:         "127.0.0.1:6379",
			MaxRetries:   -1,
			DialTimeout:  time.Second,
			ReadTimeout:  time.Second,
			WriteTimeout: time.Second,
		},
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	// 退出时先结束业务并释放锁，再关闭 Redis，最后关闭日志。
	defer client.Close()
	locker, err := redislock.New(client, redislock.Config{TTL: 10 * time.Second})
	if err != nil {
		fmt.Println(err)
		return
	}
	executed, err := lockx.TryDo(ctx, locker, "orders:lock:confirm:123",
		func(ctx context.Context) error {
			// 在此执行遵守 ctx 期限的业务逻辑。
			return nil
		},
	)
	fmt.Println(executed, err)
}
