package redislock

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/lockx"
)

func TestLockWaitsWithoutConsumingSuccessfulLease(t *testing.T) {
	const ttl = time.Second
	var calls atomic.Int32
	var firstAttempt, successfulAttempt time.Time
	client := newWireClient(t, -1, func(args []string) string {
		if strings.EqualFold(args[0], "EVAL") {
			return ":0\r\n"
		}
		if calls.Add(1) == 1 {
			firstAttempt = time.Now()
			return "$-1\r\n"
		}
		successfulAttempt = time.Now()
		time.Sleep(20 * time.Millisecond)
		return "+OK\r\n"
	})
	locker, err := New(client, Config{
		TTL:           ttl,
		WaitTimeout:   time.Second,
		RetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	var blocking lockx.Locker = locker
	h, err := blocking.Lock(context.Background(), "key")
	if err != nil || h == nil || calls.Load() != 2 {
		t.Fatalf("应在竞争后获取: handle=%v err=%v calls=%d", h, err, calls.Load())
	}
	deadline, finite := h.Deadline()
	if !finite || !deadline.After(firstAttempt.Add(ttl)) || deadline.After(successfulAttempt.Add(ttl)) || time.Until(deadline) > ttl-20*time.Millisecond {
		t.Fatalf("竞争等待或成功请求耗时处理错误: deadline=%v first=%v success=%v", deadline, firstAttempt, successfulAttempt)
	}
	if err := h.Unlock(context.Background()); err != nil {
		t.Fatalf("已过期或已释放的响应应正常返回: %v", err)
	}
}

func TestLockOnlyRetriesConfirmedContention(t *testing.T) {
	for _, response := range []string{"-ERR unavailable\r\n", "", "+unexpected\r\n"} {
		t.Run(response, func(t *testing.T) {
			var calls atomic.Int32
			client := newWireClient(t, -1, func([]string) string {
				calls.Add(1)
				return response
			})
			h, err := mustNew(t, client).Lock(context.Background(), "key")
			if h != nil || err == nil || calls.Load() != 1 {
				t.Fatalf("不确定结果不能重试: handle=%v err=%v calls=%d", h, err, calls.Load())
			}
		})
	}
}

func TestLockWaitingCanBeInterrupted(t *testing.T) {
	for _, mode := range []string{"配置等待超时", "更短请求期限", "请求取消"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			waitTimeout := 30 * time.Millisecond
			want := context.DeadlineExceeded
			if mode == "更短请求期限" {
				var stop context.CancelFunc
				ctx, stop = context.WithTimeout(ctx, 30*time.Millisecond)
				defer stop()
				waitTimeout = time.Second
			} else if mode == "请求取消" {
				waitTimeout = time.Second
				want = context.Canceled
			}
			var calls atomic.Int32
			client := newWireClient(t, -1, func([]string) string {
				calls.Add(1)
				if mode == "请求取消" {
					// 响应后取消，验证等待可及时退出而不会继续发命令。
					time.AfterFunc(10*time.Millisecond, cancel)
				}
				return "$-1\r\n"
			})
			locker, err := New(client, Config{
				TTL:           time.Minute,
				WaitTimeout:   waitTimeout,
				RetryInterval: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			h, err := locker.Lock(ctx, "key")
			if h != nil || !errors.Is(err, want) || calls.Load() != 1 || time.Since(start) >= 500*time.Millisecond {
				t.Fatalf("等待未及时结束: handle=%v err=%v calls=%d elapsed=%v", h, err, calls.Load(), time.Since(start))
			}
		})
	}
}

func TestLockValidatesBeforeCommands(t *testing.T) {
	client := newWireClient(t, -1, nil)
	locker := mustNew(t, client)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tt := range []struct {
		ctx context.Context
		key string
	}{
		{nil, "key"},
		{context.Background(), " \t "},
		{canceled, "key"},
		{expiredContext{context.Background()}, "key"},
	} {
		if h, err := locker.Lock(tt.ctx, tt.key); h != nil || err == nil {
			t.Fatalf("无效请求未拒绝: handle=%v err=%v", h, err)
		}
	}
	for _, l := range []*Locker{nil, {}} {
		if h, err := l.Lock(context.Background(), "key"); h != nil || err == nil {
			t.Fatalf("未构造的实例应拒绝: handle=%v err=%v", h, err)
		}
	}
}

func TestNewRejectsNegativeWaitSettings(t *testing.T) {
	client := newWireClient(t, -1, nil)
	for _, cfg := range []Config{
		{TTL: time.Second, WaitTimeout: -time.Second},
		{TTL: time.Second, RetryInterval: -time.Second},
	} {
		if _, err := New(client, cfg); err == nil {
			t.Fatal("负等待配置应被拒绝")
		}
	}
}

func TestDefaultTTLIsOneMinute(t *testing.T) {
	commands := make(chan []string, 2)
	client := newWireClient(t, -1, func(args []string) string {
		commands <- args
		return "+OK\r\n"
	})
	locker, err := New(client, Config{})
	if err != nil {
		t.Fatal(err)
	}
	h, acquired, err := locker.TryLock(context.Background(), "key")
	if err != nil || !acquired || h == nil {
		t.Fatalf("默认配置应可获取: handle=%v acquired=%v err=%v", h, acquired, err)
	}
	args := <-commands
	if len(args) != 6 || args[5] != "60000" {
		t.Fatalf("默认 TTL 应为 1 分钟: %v", args)
	}
	deadline, finite := h.Deadline()
	if !finite || time.Until(deadline) <= 59*time.Second || time.Until(deadline) > time.Minute {
		t.Fatalf("默认句柄期限错误: %v", deadline)
	}
}

func TestDoAndTryDoPreserveWorkResultWithIdempotentUnlock(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		for _, expireDuringWork := range []bool{false, true} {
			t.Run(fmt.Sprintf("blocking=%v/expired=%v", blocking, expireDuringWork), func(t *testing.T) {
				client := newWireClient(t, -1, func(args []string) string {
					if strings.EqualFold(args[0], "EVAL") {
						// 模拟释放时 key 已过期或已不存在。
						return ":0\r\n"
					}
					return "+OK\r\n"
				})
				locker := mustNew(t, client, 100*time.Millisecond)
				calls := 0
				work := func(ctx context.Context) error {
					calls++
					if expireDuringWork {
						<-ctx.Done()
					}
					return nil
				}
				var err error
				if blocking {
					err = lockx.Do(context.Background(), locker, "key", work)
				} else {
					var executed bool
					executed, err = lockx.TryDo(context.Background(), locker, "key", work)
					if !executed {
						t.Fatal("未执行回调")
					}
				}
				if calls != 1 || (err != nil) != expireDuringWork || errors.Is(err, context.DeadlineExceeded) != expireDuringWork || errors.Is(err, lockx.ErrNotHeld) {
					t.Fatalf("幂等释放改变业务结果: calls=%d err=%v", calls, err)
				}
			})
		}
	}
}
