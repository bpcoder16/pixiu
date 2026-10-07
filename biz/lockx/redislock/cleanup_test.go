package redislock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/redis/go-redis/v9"
)

func TestCanceledSuccessfulAcquireStillUnlocks(t *testing.T) {
	for _, mode := range []string{"Do", "TryDo"} {
		for _, reply := range []string{":1\r\n", ":0\r\n", "-ERR cleanup failed\r\n"} {
			t.Run(mode+"/"+strings.TrimSpace(reply), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				commands := make(chan []string, 4)
				client := newWireClient(t, -1, func(args []string) string {
					commands <- args
					if strings.EqualFold(args[0], "SET") {
						// 已执行的 SET 返回成功，但调用方在读取响应前取消。
						cancel()
						return "+OK\r\n"
					}
					return reply
				})
				locker := mustNew(t, client)
				work := func(context.Context) error {
					t.Fatal("请求已取消，不能执行业务")
					return nil
				}
				var err error
				if mode == "Do" {
					err = lockx.Do(ctx, locker, " key:原值 ", work)
				} else {
					var executed bool
					executed, err = lockx.TryDo(ctx, locker, " key:原值 ", work)
					if executed {
						t.Fatal("取消后的成功不能交付给业务")
					}
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("丢失请求取消错误: %v", err)
				}
				if len(commands) != 2 {
					t.Fatalf("确认成功后应执行一次清理: commands=%d err=%v", len(commands), err)
				}
				set, unlock := <-commands, <-commands
				if len(unlock) != 5 || !strings.EqualFold(unlock[0], "EVAL") ||
					unlock[1] != unlockScript || unlock[2] != "1" || unlock[3] != set[1] || unlock[4] != set[2] {
					t.Fatalf("清理必须复用本次 key、令牌及原子释放脚本: %v", unlock)
				}
				var cleanupErr redis.Error
				if errors.As(err, &cleanupErr) != strings.HasPrefix(reply, "-ERR") {
					t.Fatalf("清理错误链不正确: %v", err)
				}
			})
		}
	}
}

func TestExpiredSuccessfulAcquireUsesIndependentCleanupBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		type requestKey struct{}
		ctx := context.WithValue(context.Background(), requestKey{}, "request")
		client := newWireClient(t, -1, func(args []string) string {
			if strings.EqualFold(args[0], "SET") {
				return "+OK\r\n"
			}
			return ":0\r\n"
		})
		unlocks := 0
		client.Client().AddHook(cleanupProcessHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
			if strings.EqualFold(cmd.Name(), "eval") {
				unlocks++
				deadline, ok := ctx.Deadline()
				if ctx.Err() != nil || ctx.Value(requestKey{}) != "request" || !ok || time.Until(deadline) != time.Second {
					t.Fatalf("清理必须保留请求值并使用独立 1 秒预算: %v", ctx)
				}
				// 模拟释放未能在独立预算内完成。
				<-ctx.Done()
				return ctx.Err()
			}
			err := next(ctx, cmd)
			if strings.EqualFold(cmd.Name(), "set") {
				// 模拟成功响应后的 Hook 耗时，确保进入已确认成功的超时分支。
				<-ctx.Done()
			}
			return err
		}))
		start := time.Now()
		h, acquired, err := mustNew(t, client, 2*time.Second).TryLock(ctx, "key")
		if h != nil || acquired || !errors.Is(err, context.DeadlineExceeded) || unlocks != 1 || time.Since(start) != 3*time.Second {
			t.Fatalf("超时后的清理结果错误: handle=%v acquired=%v err=%v unlocks=%d elapsed=%v", h, acquired, err, unlocks, time.Since(start))
		}
	})
}

type cleanupProcessHook func(context.Context, redis.Cmder, redis.ProcessHook) error

func (cleanupProcessHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h cleanupProcessHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		return h(ctx, cmd, next)
	}
}

func (cleanupProcessHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
