package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

func TestRedisIntegration(t *testing.T) {
	addr := os.Getenv("PIXIU_REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("设置 PIXIU_REDIS_TEST_ADDR 后运行真实 Redis 集成测试")
	}
	newClient := func() *redisx.Client {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		client, err := redisx.New(ctx, redisx.Config{
			Name: t.Name(),
			Options: redis.Options{
				Addr:         addr,
				MaxRetries:   -1,
				DialTimeout:  time.Second,
				ReadTimeout:  time.Second,
				WriteTimeout: time.Second,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	first, second := newClient(), newClient()
	lockers := []*Locker{mustNew(t, first), mustNew(t, second)}
	prefix := "pixiu:lockx:test:" + rand.Text() + ":"
	ctx := context.Background()
	// 仅清理本次随机前缀下明确创建的 key，不清空测试实例。
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = first.Client().Del(cleanupCtx, prefix+"race", prefix+"other", prefix+"expiry", prefix+"work").Err()
	})

	t.Run("同 key 竞争与并发释放", func(t *testing.T) {
		const contenders = 16
		start := make(chan struct{})
		winners := make(chan lockx.Lock, contenders)
		var workers sync.WaitGroup
		for i := range contenders {
			workers.Go(func() {
				<-start
				lock, acquired, err := lockers[i%len(lockers)].TryLock(ctx, prefix+"race")
				if err != nil {
					t.Errorf("竞争请求失败: %v", err)
				} else if acquired {
					winners <- lock
				}
			})
		}
		close(start)
		workers.Wait()
		if len(winners) != 1 {
			t.Fatalf("同一 key 应只有一个持有者: %d", len(winners))
		}
		winner := <-winners
		for _, locker := range lockers {
			if _, acquired, err := locker.TryLock(ctx, prefix+"race"); acquired || err != nil {
				t.Fatalf("持有期间不能重入: acquired=%v err=%v", acquired, err)
			}
		}
		other, acquired, err := lockers[1].TryLock(ctx, prefix+"other")
		if err != nil || !acquired {
			t.Fatalf("不同 key 不应互相阻塞: %v", err)
		}
		if err := other.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		var released atomic.Int32
		for range contenders {
			workers.Go(func() {
				err := winner.Unlock(ctx)
				if err == nil {
					released.Add(1)
				} else if !errors.Is(err, lockx.ErrNotHeld) {
					t.Errorf("释放结果异常: %v", err)
				}
			})
		}
		workers.Wait()
		if released.Load() != 1 {
			t.Fatalf("应只有一次释放成功: %d", released.Load())
		}
		next, acquired, err := lockers[1].TryLock(ctx, prefix+"race")
		if err != nil || !acquired {
			t.Fatalf("释放后应可重新获取: %v", err)
		}
		if err := next.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("过期后旧句柄不能删除新锁", func(t *testing.T) {
		key := prefix + "expiry"
		old, acquired, err := mustNew(t, first, 200*time.Millisecond).TryLock(ctx, key)
		if err != nil || !acquired {
			t.Fatalf("获取旧锁失败: %v", err)
		}
		deadline := time.Now().Add(3 * time.Second)
		for {
			exists, err := first.Client().Exists(ctx, key).Result()
			if err != nil {
				t.Fatal(err)
			}
			if exists == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("锁没有自动过期")
			}
			time.Sleep(10 * time.Millisecond)
		}
		current, acquired, err := lockers[1].TryLock(ctx, key)
		if err != nil || !acquired {
			t.Fatalf("过期后获取新锁失败: %v", err)
		}
		if err := old.Unlock(ctx); !errors.Is(err, lockx.ErrNotHeld) {
			t.Fatalf("旧持有者应收到 ErrNotHeld: %v", err)
		}
		if _, acquired, err := lockers[0].TryLock(ctx, key); acquired || err != nil {
			t.Fatalf("新锁被旧持有者删除: acquired=%v err=%v", acquired, err)
		}
		if err := current.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("业务错误后仍释放", func(t *testing.T) {
		key := prefix + "work"
		workErr := errors.New("业务失败")
		executed, err := lockx.TryDo(ctx, lockers[0], key, func(ctx context.Context) error {
			if _, acquired, err := lockers[1].TryLock(ctx, key); acquired || err != nil {
				t.Fatalf("业务执行时未持锁: acquired=%v err=%v", acquired, err)
			}
			return workErr
		})
		if !executed || !errors.Is(err, workErr) {
			t.Fatalf("执行结果改变: executed=%v err=%v", executed, err)
		}
		next, acquired, err := lockers[1].TryLock(ctx, key)
		if err != nil || !acquired {
			t.Fatalf("业务错误后未释放: %v", err)
		}
		if err := next.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
	})
}
