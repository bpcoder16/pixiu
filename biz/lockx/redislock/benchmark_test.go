package redislock

import (
	"context"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/redis/go-redis/v9"
)

// BenchmarkSingleKeyLock 用 Hook 返回确定响应，隔离网络与 Redis 耗时，比较 Go 流程和分配。
// 成功场景包含获取和释放，竞争场景仅包含一次获取；真实互斥由集成测试覆盖。
func BenchmarkSingleKeyLock(b *testing.B) {
	for _, method := range []string{"TryLock", "TryLockMany", "TryLockManyDuplicate"} {
		for _, acquired := range []bool{true, false} {
			outcome := "Success"
			if !acquired {
				outcome = "Contention"
			}
			b.Run(method+"/"+outcome, func(b *testing.B) {
				rdb := redis.NewClient(&redis.Options{
					Addr:       "unused",
					MaxRetries: -1,
				})
				b.Cleanup(func() {
					_ = rdb.Close()
				})
				rdb.AddHook(cleanupProcessHook(func(_ context.Context, cmd redis.Cmder, _ redis.ProcessHook) error {
					result := cmd.(*redis.Cmd)
					if cmd.Name() == "set" {
						if acquired {
							result.SetVal("OK")
							return nil
						}
						result.SetErr(redis.Nil)
						return redis.Nil
					}
					if cmd.Args()[1] == acquireManyScript && !acquired {
						result.SetVal(int64(0))
					} else {
						result.SetVal(int64(1))
					}
					return nil
				}))
				locker := &Locker{
					client: rdb,
					ttl:    time.Minute,
				}
				ctx := context.Background()
				keys := []string{"key"}
				if method == "TryLockManyDuplicate" {
					keys = append(keys, "key")
				}
				attempt := func() (lockx.Lock, bool, error) {
					if method == "TryLock" {
						return locker.TryLock(ctx, keys[0])
					}
					return locker.TryLockMany(ctx, keys)
				}
				b.ReportAllocs()
				for b.Loop() {
					held, ok, err := attempt()
					if err != nil || ok != acquired {
						b.Fatalf("获取结果错误: %v %v", ok, err)
					}
					if ok {
						if err := held.Unlock(ctx); err != nil {
							b.Fatal(err)
						}
					}
				}
			})
		}
	}
}
