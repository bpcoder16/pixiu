package redislock

import (
	"context"
	"errors"
	"io"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/redis/go-redis/v9"
)

func TestManyWireProtocolAndOwnership(t *testing.T) {
	commands := make(chan []string, 8)
	client := newWireClient(t, -1, func(args []string) string {
		commands <- args
		return ":1\r\n"
	})
	var previous string
	for _, tc := range []struct {
		ttl  time.Duration
		want string
	}{
		{1500*time.Millisecond + time.Nanosecond, "1501"},
		{time.Duration(math.MaxInt64), "9223372036855"},
	} {
		keys := []string{" b ", "a", " b "}
		held, acquired, err := mustNew(t, client, tc.ttl).TryLockMany(context.Background(), keys)
		if err != nil || !acquired || held == nil {
			t.Fatalf("获取失败: %v", err)
		}
		keys[0] = "changed"
		acquire := <-commands
		if len(acquire) != 7 || acquire[0] != "eval" || acquire[2] != "2" || !reflect.DeepEqual(acquire[3:5], []string{" b ", "a"}) || acquire[6] != tc.want || len(acquire[5]) < 26 || acquire[5] == previous {
			t.Fatalf("批量命令参数错误: %v", acquire)
		}
		previous = acquire[5]
		deadline, ok := held.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > tc.ttl {
			t.Fatal("租期必须包含获取耗时")
		}
		if err := held.Unlock(nil); err == nil {
			t.Fatal("应拒绝 nil context")
		}
		if err := held.Unlock(context.Background()); err != nil {
			t.Fatal(err)
		}
		release := <-commands
		if len(release) != 6 || release[2] != "2" || !reflect.DeepEqual(release[3:5], acquire[3:5]) || release[5] != acquire[5] {
			t.Fatalf("释放必须保留原始 key 和本次身份: %v", release)
		}
	}
	if len(commands) != 0 {
		t.Fatal("不能重复获取或多发命令")
	}
}

func TestManyContentionAndErrors(t *testing.T) {
	for _, reply := range []string{":0\r\n", "-ERR unavailable\r\n", "", ":2\r\n", "$-1\r\n"} {
		t.Run(strings.TrimSpace(reply), func(t *testing.T) {
			var calls atomic.Int32
			client := newWireClient(t, -1, func([]string) string {
				calls.Add(1)
				return reply
			})
			executed, err := lockx.TryDoMany(context.Background(), mustNew(t, client), []string{"a", "b"}, func(context.Context) error {
				t.Fatal("失败批次不能执行业务")
				return nil
			})
			if executed || calls.Load() != 1 || (err != nil) != (reply != ":0\r\n") {
				t.Fatalf("竞争与下游错误混淆或发生重试: %v %v %d", executed, err, calls.Load())
			}
			if reply == "" && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				t.Fatalf("丢失不确定结果的底层错误: %v", err)
			}
		})
	}
}

func TestManyInvalidInputSendsNoCommands(t *testing.T) {
	client := newWireClient(t, -1, nil)
	locker := mustNew(t, client)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		ctx  context.Context
		keys []string
	}{
		{nil, []string{"a"}},
		{context.Background(), nil},
		{context.Background(), []string{"a", ""}},
		{context.Background(), []string{"a", " \t"}},
		{canceled, []string{"a"}},
		{expiredContext{context.Background()}, []string{"a"}},
	} {
		if held, acquired, err := locker.TryLockMany(tc.ctx, tc.keys); held != nil || acquired || err == nil {
			t.Fatalf("无效请求被放行: %v %v %v", held, acquired, err)
		}
	}
	for _, locker := range []*Locker{nil, {}} {
		if _, _, err := locker.TryLockMany(context.Background(), []string{"a"}); err == nil {
			t.Fatal("未构造实例不能使用")
		}
	}
}

func TestManyLateSuccessCleanup(t *testing.T) {
	for _, reply := range []string{":2\r\n", "-ERR cleanup failed\r\n"} {
		t.Run(strings.TrimSpace(reply), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			commands := make(chan []string, 4)
			client := newWireClient(t, -1, func(args []string) string {
				commands <- args
				if args[1] == acquireManyScript {
					cancel()
					return ":1\r\n"
				}
				return reply
			})
			held, acquired, err := mustNew(t, client).TryLockMany(ctx, []string{"a", "b"})
			if held != nil || acquired || !errors.Is(err, context.Canceled) || len(commands) != 2 {
				t.Fatalf("已确认成功后取消必须清理整批: %v %v %v", held, acquired, err)
			}
			acquire, release := <-commands, <-commands
			if !reflect.DeepEqual(acquire[2:6], release[2:6]) {
				t.Fatalf("清理身份不一致: %v %v", acquire, release)
			}
			var cleanupErr redis.Error
			if errors.As(err, &cleanupErr) != strings.HasPrefix(reply, "-ERR") {
				t.Fatalf("清理错误链错误: %v", err)
			}
		})
	}
	synctest.Test(t, func(t *testing.T) {
		type requestKey struct{}
		client := newWireClient(t, -1, func([]string) string { return ":1\r\n" })
		unlocks := 0
		client.Client().AddHook(cleanupProcessHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
			if cmd.Args()[1] == unlockManyScript {
				unlocks++
				deadline, ok := ctx.Deadline()
				if ctx.Err() != nil || ctx.Value(requestKey{}) != "request" || !ok || time.Until(deadline) != time.Second {
					t.Error("清理必须有独立预算并保留请求值")
				}
				<-ctx.Done()
				return ctx.Err()
			}
			err := next(ctx, cmd)
			if cmd.Args()[1] == acquireManyScript {
				<-ctx.Done()
			}
			return err
		}))
		ctx := context.WithValue(context.Background(), requestKey{}, "request")
		start := time.Now()
		held, acquired, err := mustNew(t, client, 2*time.Second).TryLockMany(ctx, []string{"a", "b"})
		if held != nil || acquired || !errors.Is(err, context.DeadlineExceeded) || unlocks != 1 || time.Since(start) != 3*time.Second {
			t.Fatalf("租期与清理预算错误: %v %v %v %d", held, acquired, err, unlocks)
		}
	})
}
