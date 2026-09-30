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
		_ = first.Client().Del(cleanupCtx, prefix+"race", prefix+"other", prefix+"expiry", prefix+"work", prefix+"blocking", prefix+"serial").Err()
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
				} else {
					t.Errorf("释放结果异常: %v", err)
				}
			})
		}
		workers.Wait()
		if released.Load() != contenders {
			t.Fatalf("并发释放应全部幂等完成: %d", released.Load())
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
		if err := old.Unlock(ctx); err != nil {
			t.Fatalf("key 已过期且不存在时应正常释放: %v", err)
		}
		current, acquired, err := lockers[1].TryLock(ctx, key)
		if err != nil || !acquired {
			t.Fatalf("过期后获取新锁失败: %v", err)
		}
		if err := old.Unlock(ctx); err != nil {
			t.Fatalf("过期后的旧身份释放应幂等完成: %v", err)
		}
		if _, acquired, err := lockers[0].TryLock(ctx, key); acquired || err != nil {
			t.Fatalf("新锁被旧持有者删除: acquired=%v err=%v", acquired, err)
		}
		if err := current.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("阻塞等待与非阻塞获取共用同一锁", func(t *testing.T) {
		key := prefix + "blocking"
		held, acquired, err := lockers[0].TryLock(ctx, key)
		if err != nil || !acquired {
			t.Fatalf("获取占用失败: %v", err)
		}
		defer held.Unlock(ctx)
		waiter, err := New(second, Config{
			WaitTimeout:   time.Second,
			RetryInterval: 5 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		limited, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
		defer cancel()
		if h, err := waiter.Lock(limited, key); h != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("持有期间等待应超时: handle=%v err=%v", h, err)
		}
		waiting, stop := context.WithCancel(ctx)
		defer stop()
		started := make(chan struct{})
		failure := make(chan error, 1)
		go func() {
			close(started)
			h, err := waiter.Lock(waiting, key)
			if h != nil {
				_ = h.Unlock(ctx)
			}
			failure <- err
		}()
		<-started
		stop()
		if err := <-failure; !errors.Is(err, context.Canceled) {
			t.Fatalf("等待取消未生效: %v", err)
		}
		result := make(chan lockx.Lock, 1)
		go func() {
			h, err := waiter.Lock(ctx, key)
			if err != nil {
				failure <- err
				return
			}
			result <- h
		}()
		select {
		case h := <-result:
			_ = h.Unlock(ctx)
			t.Fatal("未释放时不应获取成功")
		case err := <-failure:
			t.Fatal(err)
		case <-time.After(40 * time.Millisecond):
		}
		if err := held.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case h := <-result:
			defer h.Unlock(ctx)
			if _, acquired, err := lockers[0].TryLock(ctx, key); acquired || err != nil {
				t.Fatalf("非阻塞请求应观察到阻塞持有者: acquired=%v err=%v", acquired, err)
			}
			deadline, finite := h.Deadline()
			if !finite || time.Until(deadline) < 59*time.Second {
				t.Fatalf("竞争等待消耗了默认租期: %v", deadline)
			}
		case err := <-failure:
			t.Fatal(err)
		case <-time.After(2 * time.Second):
			t.Fatal("释放后等待者未获取")
		}
	})

	t.Run("阻塞执行同 key 临界区互斥", func(t *testing.T) {
		const contenders = 8
		key := prefix + "serial"
		var active, completed atomic.Int32
		var workers sync.WaitGroup
		start := make(chan struct{})
		for i := range contenders {
			workers.Go(func() {
				<-start
				err := lockx.Do(ctx, lockers[i%len(lockers)], key, func(context.Context) error {
					if active.Add(1) != 1 {
						t.Error("临界区同时进入了多个持有者")
					}
					defer active.Add(-1)
					time.Sleep(10 * time.Millisecond)
					completed.Add(1)
					return nil
				})
				if err != nil {
					t.Errorf("阻塞执行失败: %v", err)
				}
			})
		}
		close(start)
		workers.Wait()
		if completed.Load() != contenders {
			t.Fatalf("部分临界区未执行: %d", completed.Load())
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
