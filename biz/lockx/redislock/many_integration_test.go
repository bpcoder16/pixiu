package redislock

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/redis/go-redis/v9"
)

func TestManyRedisIntegration(t *testing.T) {
	addr := os.Getenv("PIXIU_REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("设置 PIXIU_REDIS_TEST_ADDR 后运行真实 Redis 批量锁测试")
	}
	newClient := func() *redisx.Client {
		client, err := redisx.New(redisx.Config{
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
	left, right := mustNew(t, first), mustNew(t, second)
	ctx := context.Background()
	// 每个子测试仅清理自己生成的随机 key，不清空实例或更改全局配置。
	keysFor := func(t *testing.T, count int) []string {
		prefix := "pixiu:lockx:many:" + rand.Text() + ":"
		keys := make([]string, count)
		for i := range keys {
			keys[i] = fmt.Sprintf("%s%d", prefix, i)
		}
		t.Cleanup(func() {
			cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := first.Client().Del(cleanup, keys...).Err(); err != nil {
				t.Error(err)
			}
		})
		return keys
	}
	exists := func(t *testing.T, want int64, keys ...string) {
		t.Helper()
		got, err := first.Client().Exists(ctx, keys...).Result()
		if err != nil || got != want {
			t.Fatalf("锁占用错误: got=%d want=%d err=%v", got, want, err)
		}
	}

	t.Run("一个唯一key与单锁及多锁互斥", func(t *testing.T) {
		for _, count := range []int{1, 3} {
			keys := keysFor(t, 2)
			input := make([]string, count)
			for i := range input {
				input[i] = keys[0]
			}
			held, ok, err := left.TryLockMany(ctx, input)
			if err != nil || !ok {
				t.Fatalf("一个唯一 key 获取失败: %v", err)
			}
			input[0] = "caller-mutated"
			if single, ok, err := right.TryLock(ctx, keys[0]); single != nil || ok || err != nil {
				t.Fatalf("单锁必须竞争失败: %v %v", ok, err)
			}
			if batch, ok, err := right.TryLockMany(ctx, keys); batch != nil || ok || err != nil {
				t.Fatalf("多锁必须整批竞争失败: %v %v", ok, err)
			}
			exists(t, 1, keys...)
			if err := held.Unlock(ctx); err != nil {
				t.Fatal(err)
			}
			exists(t, 0, keys...)
			batch, ok, err := right.TryLockMany(ctx, keys)
			if err != nil || !ok {
				t.Fatalf("释放后批量获取失败: %v", err)
			}
			if old, ok, err := left.TryLockMany(ctx, []string{keys[0]}); old != nil || ok || err != nil {
				t.Fatalf("一个唯一 key 必须与多锁互斥: %v %v", ok, err)
			}
			if err := batch.Unlock(ctx); err != nil {
				t.Fatal(err)
			}
		}
	})

	t.Run("任意位置冲突整批失败且与单锁互斥", func(t *testing.T) {
		for position := range 3 {
			keys := keysFor(t, 3)
			held, ok, err := left.TryLock(ctx, keys[position])
			if err != nil || !ok {
				t.Fatalf("预持有失败: %v", err)
			}
			executed, err := lockx.TryDoMany(ctx, right, keys, func(context.Context) error {
				t.Fatal("部分冲突不能执行业务")
				return nil
			})
			if executed || err != nil {
				t.Fatalf("竞争结果错误: %v %v", executed, err)
			}
			exists(t, 1, keys...)
			if err := held.Unlock(ctx); err != nil {
				t.Fatal(err)
			}
			input := []string{keys[0], keys[1], keys[0], keys[2]}
			batch, ok, err := right.TryLockMany(ctx, input)
			if err != nil || !ok {
				t.Fatalf("释放后批量获取失败: %v", err)
			}
			input[0] = "caller-mutated"
			for _, key := range keys {
				if single, acquired, err := left.TryLock(ctx, key); single != nil || acquired || err != nil {
					t.Fatalf("单 key 必须与批量占用互斥: %v", err)
				}
			}
			if err := batch.Unlock(ctx); err != nil {
				t.Fatal(err)
			}
			exists(t, 0, keys...)
		}
	})

	t.Run("重叠批次并发只有一个成功", func(t *testing.T) {
		const count = 16
		keys := keysFor(t, count+1)
		start := make(chan struct{})
		winners := make(chan lockx.Lock, count)
		var workers sync.WaitGroup
		for i := range count {
			workers.Go(func() {
				<-start
				pair := []string{keys[i+1], keys[0]}
				locker := left
				if i%2 == 1 {
					locker = right
					pair[0], pair[1] = pair[1], pair[0]
				}
				held, acquired, err := locker.TryLockMany(ctx, pair)
				if err != nil {
					t.Error(err)
				} else if acquired {
					winners <- held
				}
			})
		}
		close(start)
		workers.Wait()
		if len(winners) != 1 {
			t.Fatalf("重叠批次应只有一个持有者: %d", len(winners))
		}
		exists(t, 2, keys...)
		winner := <-winners
		for range count {
			workers.Go(func() {
				if err := winner.Unlock(ctx); err != nil {
					t.Error(err)
				}
			})
		}
		workers.Wait()
		exists(t, 0, keys...)
	})

	t.Run("不重叠批次可以同时执行业务", func(t *testing.T) {
		keys := keysFor(t, 4)
		entered, release := make(chan struct{}, 2), make(chan struct{})
		var workers sync.WaitGroup
		defer workers.Wait()
		defer close(release)
		for i, locker := range []*Locker{left, right} {
			workers.Go(func() {
				executed, err := lockx.TryDoMany(ctx, locker, keys[i*2:i*2+2], func(context.Context) error {
					entered <- struct{}{}
					<-release
					return nil
				})
				if err != nil || !executed {
					t.Errorf("不重叠批次应能执行: %v %v", executed, err)
				}
			})
		}
		for range 2 {
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("不同批次被阻塞")
			}
		}
		exists(t, 4, keys...)
	})

	t.Run("业务失败取消和panic均释放整批", func(t *testing.T) {
		e, err := lockx.New(left)
		if err != nil {
			t.Fatal(err)
		}
		for _, mode := range []string{"error", "cancel", "panic"} {
			keys := keysFor(t, 2)
			workCtx, cancel := context.WithCancel(ctx)
			workErr := errors.New("业务失败")
			func() {
				defer func() {
					value := recover()
					if mode == "panic" && value != "original" || mode != "panic" && value != nil {
						t.Errorf("panic 语义错误: %v", value)
					}
				}()
				executed, err := e.TryDoMany(workCtx, keys, func(context.Context) error {
					switch mode {
					case "cancel":
						cancel()
						return workCtx.Err()
					case "panic":
						panic("original")
					default:
						return workErr
					}
				})
				want := workErr
				if mode == "cancel" {
					want = context.Canceled
				}
				if !executed || !errors.Is(err, want) {
					t.Errorf("业务错误未保留: %v %v", executed, err)
				}
			}()
			cancel()
			exists(t, 0, keys...)
		}
	})

	t.Run("TTL回收和旧句柄不能删除新占用", func(t *testing.T) {
		keys := keysFor(t, 3)
		old, ok, err := mustNew(t, first, 500*time.Millisecond).TryLockMany(ctx, keys[:2])
		if err != nil || !ok {
			t.Fatalf("获取旧批次失败: %v", err)
		}
		until := time.Now().Add(3 * time.Second)
		for {
			count, err := first.Client().Exists(ctx, keys[:2]...).Result()
			if err != nil {
				t.Fatal(err)
			}
			if count == 0 {
				break
			}
			if time.Now().After(until) {
				t.Fatal("整批租期未回收")
			}
			time.Sleep(10 * time.Millisecond)
		}
		current, ok, err := right.TryLockMany(ctx, []string{keys[0], keys[2]})
		if err != nil || !ok {
			t.Fatalf("获取新批次失败: %v", err)
		}
		if err := old.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		exists(t, 2, keys...)
		if err := first.Client().Del(ctx, keys[0]).Err(); err != nil {
			t.Fatal(err)
		}
		single, ok, err := left.TryLock(ctx, keys[0])
		if err != nil || !ok {
			t.Fatalf("替换单 key 失败: %v", err)
		}
		if err := current.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		exists(t, 1, keys...)
		if err := single.Unlock(ctx); err != nil {
			t.Fatal(err)
		}
		exists(t, 0, keys...)
	})

	t.Run("某个key类型异常仍清理其余占用", func(t *testing.T) {
		keys := keysFor(t, 2)
		held, ok, err := left.TryLockMany(ctx, keys)
		if err != nil || !ok {
			t.Fatalf("获取失败: %v", err)
		}
		if err := first.Client().Del(ctx, keys[0]).Err(); err != nil {
			t.Fatal(err)
		}
		if err := first.Client().RPush(ctx, keys[0], "other-owner").Err(); err != nil {
			t.Fatal(err)
		}
		if err := held.Unlock(ctx); err == nil {
			t.Fatal("应保留 Redis 类型错误")
		}
		exists(t, 1, keys[0])
		exists(t, 0, keys[1])
	})
}
