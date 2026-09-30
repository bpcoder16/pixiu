package lockx

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"
)

type tryLockerFunc func(context.Context, string) (Lock, bool, error)

func (f tryLockerFunc) TryLock(ctx context.Context, key string) (Lock, bool, error) {
	return f(ctx, key)
}

type unlockFunc func(context.Context) error

func (f unlockFunc) Unlock(ctx context.Context) error { return f(ctx) }
func (f unlockFunc) Deadline() (time.Time, bool)      { return time.Time{}, false }

type leaseHandle struct {
	Lock
	deadline time.Time
}

func (l leaseHandle) Deadline() (time.Time, bool) { return l.deadline, true }

type lockerFunc func(context.Context, string) (Lock, error)

func (f lockerFunc) Lock(ctx context.Context, key string) (Lock, error) { return f(ctx, key) }

func TestTryDoOutcomes(t *testing.T) {
	acquireErr := errors.New("下游失败")
	workErr := errors.New("业务失败")
	for _, tt := range []struct {
		name       string
		acquired   bool
		acquireErr error
		workErr    error
		unlockErr  error
	}{
		{name: "竞争失败"},
		{name: "获取错误", acquireErr: acquireErr},
		{name: "正常执行", acquired: true},
		{name: "业务失败", acquired: true, workErr: workErr},
		{name: "释放失败", acquired: true, unlockErr: ErrNotHeld},
		{name: "合并错误", acquired: true, workErr: workErr, unlockErr: ErrNotHeld},
	} {
		t.Run(tt.name, func(t *testing.T) {
			acquires, calls, unlocks := 0, 0, 0
			locker := tryLockerFunc(func(_ context.Context, key string) (Lock, bool, error) {
				acquires++
				if key != "orders:123" {
					t.Fatalf("获取参数改变: %q", key)
				}
				if !tt.acquired {
					return nil, false, tt.acquireErr
				}
				return unlockFunc(func(context.Context) error {
					unlocks++
					return tt.unlockErr
				}), true, nil
			})
			executed, err := TryDo(context.Background(), locker, "orders:123", func(context.Context) error {
				calls++
				return tt.workErr
			})
			wantCalls := 0
			if tt.acquired {
				wantCalls = 1
			}
			if executed != tt.acquired || acquires != 1 || calls != wantCalls || unlocks != wantCalls {
				t.Fatalf("执行次数异常: executed=%v acquires=%d calls=%d unlocks=%d", executed, acquires, calls, unlocks)
			}
			wantErr := tt.acquireErr != nil || tt.workErr != nil || tt.unlockErr != nil
			if (err != nil) != wantErr {
				t.Fatalf("错误状态不符: %v", err)
			}
			for _, cause := range []error{tt.acquireErr, tt.workErr, tt.unlockErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("错误链缺少 %v: %v", cause, err)
				}
			}
		})
	}
}

func TestTryDoCanceledWorkStillUnlocks(t *testing.T) {
	type valueKey struct{}
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), valueKey{}, "request"))
	defer cancel()
	unlocked := false
	locker := tryLockerFunc(func(context.Context, string) (Lock, bool, error) {
		return unlockFunc(func(ctx context.Context) error {
			unlocked = true
			if ctx.Err() != nil || ctx.Value(valueKey{}) != "request" {
				t.Fatalf("释放应保留值并脱离取消: %v", ctx)
			}
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > time.Second {
				t.Fatalf("释放缺少独立的 1 秒期限: %v", deadline)
			}
			return nil
		}), true, nil
	})
	executed, err := TryDo(ctx, locker, "key", func(ctx context.Context) error {
		cancel()
		return ctx.Err()
	})
	if !executed || !unlocked || !errors.Is(err, context.Canceled) {
		t.Fatalf("取消后释放异常: executed=%v unlocked=%v err=%v", executed, unlocked, err)
	}
}

func TestTryDoPanicStillUnlocks(t *testing.T) {
	panicValue := &struct{}{}
	unlocked := false
	locker := tryLockerFunc(func(context.Context, string) (Lock, bool, error) {
		return unlockFunc(func(context.Context) error {
			unlocked = true
			return errors.New("释放失败不应覆盖 panic")
		}), true, nil
	})
	defer func() {
		if value := recover(); value != panicValue || !unlocked {
			t.Fatalf("原始 panic 或释放行为丢失: panic=%v unlocked=%v", value, unlocked)
		}
	}()
	_, _ = TryDo(context.Background(), locker, "key", func(context.Context) error {
		panic(panicValue)
	})
}

func TestTryDoLeaseBudget(t *testing.T) {
	for _, tt := range []struct {
		name         string
		acquireDelay time.Duration
		workDelay    time.Duration
		parentLimit  time.Duration
		wantExecuted bool
		wantExpired  bool
	}{
		{name: "扣除获取耗时", acquireDelay: 2 * time.Second, workDelay: time.Second, wantExecuted: true},
		{name: "获取耗尽租期", acquireDelay: 11 * time.Second, wantExpired: true},
		{name: "工作超过租期", workDelay: 11 * time.Second, wantExecuted: true, wantExpired: true},
		{name: "遵守更短请求期限", workDelay: 6 * time.Second, parentLimit: 5 * time.Second, wantExecuted: true, wantExpired: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx := context.Background()
				if tt.parentLimit > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tt.parentLimit)
					defer cancel()
				}
				const ttl = 10 * time.Second
				start := time.Now()
				unlocks := 0
				locker := tryLockerFunc(func(context.Context, string) (Lock, bool, error) {
					time.Sleep(tt.acquireDelay)
					// 故意返回晚到的成功，验证编排层仍检查预算并释放。
					return leaseHandle{Lock: unlockFunc(func(ctx context.Context) error {
						unlocks++
						if ctx.Err() != nil {
							t.Fatalf("释放复用了已过期的工作 context: %v", ctx.Err())
						}
						return nil
					}), deadline: start.Add(ttl)}, true, nil
				})
				executed, err := TryDo(ctx, locker, "key", func(ctx context.Context) error {
					deadline, ok := ctx.Deadline()
					want := start.Add(ttl)
					if tt.parentLimit > 0 {
						want = start.Add(tt.parentLimit)
					}
					if !ok || !deadline.Equal(want) {
						t.Fatalf("工作期限重置了租期: got=%v want=%v", deadline, want)
					}
					time.Sleep(tt.workDelay)
					return nil
				})
				if executed != tt.wantExecuted || unlocks != 1 || errors.Is(err, context.DeadlineExceeded) != tt.wantExpired {
					t.Fatalf("租期处理异常: executed=%v unlocks=%d err=%v", executed, unlocks, err)
				}
			})
		})
	}
}

func TestTryDoReleaseTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		locker := tryLockerFunc(func(context.Context, string) (Lock, bool, error) {
			return unlockFunc(func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}), true, nil
		})
		executed, err := TryDo(context.Background(), locker, "key", func(context.Context) error {
			return nil
		})
		if !executed || !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Fatalf("释放未按独立期限结束: executed=%v err=%v elapsed=%v", executed, err, time.Since(start))
		}
	})
}

// 模拟期限已到、取消计时器尚未调度的 context。
type expiredContext struct{ context.Context }

func (expiredContext) Deadline() (time.Time, bool) { return time.Now().Add(-time.Second), true }

func TestTryDoRejectsInvalidInputsBeforeAcquire(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tt := range []struct {
		name      string
		ctx       context.Context
		key       string
		nilLocker bool
		nilFunc   bool
		wantErr   error
	}{
		{name: "空 context", key: "key"},
		{name: "空 key", ctx: context.Background()},
		{name: "空白 key", ctx: context.Background(), key: " \t "},
		{name: "空实例", ctx: context.Background(), key: "key", nilLocker: true},
		{name: "空回调", ctx: context.Background(), key: "key", nilFunc: true},
		{name: "请求已取消", ctx: ctx, key: "key", wantErr: context.Canceled},
		{name: "期限已到", ctx: expiredContext{context.Background()}, key: "key", wantErr: context.DeadlineExceeded},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var locker TryLocker = tryLockerFunc(func(context.Context, string) (Lock, bool, error) {
				t.Fatal("无效输入不能访问下游")
				return nil, false, nil
			})
			fn := func(context.Context) error {
				t.Fatal("无效输入不能调用回调")
				return nil
			}
			if tt.nilLocker {
				locker = nil
			}
			if tt.nilFunc {
				fn = nil
			}
			executed, err := TryDo(tt.ctx, locker, tt.key, fn)
			if executed || err == nil || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("未拒绝无效输入: executed=%v err=%v", executed, err)
			}
		})
	}
}

func TestDoWaitBudgetDoesNotLimitWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		released := false
		locker := lockerFunc(func(ctx context.Context, key string) (Lock, error) {
			if key != "key" || ctx != context.Background() {
				t.Fatal("获取参数被改变")
			}
			time.Sleep(5 * time.Second)
			return unlockFunc(func(context.Context) error {
				released = true
				return nil
			}), nil
		})
		err := Do(context.Background(), locker, "key", func(ctx context.Context) error {
			if _, ok := ctx.Deadline(); ok {
				t.Fatal("会话锁不应设置虚构租期")
			}
			time.Sleep(10 * time.Second)
			return nil
		})
		if err != nil || !released || time.Since(start) != 15*time.Second {
			t.Fatalf("会话工作异常: %v", err)
		}
	})
}

func TestDoUsesSuccessfulAttemptDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		locker := lockerFunc(func(context.Context, string) (Lock, error) {
			time.Sleep(time.Minute)
			deadline := time.Now().Add(10 * time.Second)
			time.Sleep(2 * time.Second)
			return leaseHandle{Lock: unlockFunc(func(context.Context) error { return nil }), deadline: deadline}, nil
		})
		err := Do(context.Background(), locker, "key", func(ctx context.Context) error {
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) != 8*time.Second {
				t.Fatal("之前的等待消耗了成功尝试的租期")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestDoAcquisitionError(t *testing.T) {
	cause := errors.New("获取失败")
	err := Do(context.Background(), lockerFunc(func(context.Context, string) (Lock, error) {
		return nil, cause
	}), "key", func(context.Context) error {
		t.Fatal("获取失败不能执行业务")
		return nil
	})
	if !errors.Is(err, cause) {
		t.Fatalf("丢失错误链: %v", err)
	}
}
