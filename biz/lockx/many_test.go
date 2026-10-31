package lockx

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

type tryManyLockerFunc func(context.Context, []string) (Lock, bool, error)

func (f tryManyLockerFunc) TryLockMany(ctx context.Context, keys []string) (Lock, bool, error) {
	return f(ctx, keys)
}

func TestTryDoManyOutcomes(t *testing.T) {
	acquireErr, workErr, unlockErr := errors.New("获取失败"), errors.New("业务失败"), errors.New("释放失败")
	for _, tc := range []struct {
		name                           string
		acquired                       bool
		acquireErr, workErr, unlockErr error
	}{
		{name: "竞争失败"},
		{name: "获取失败", acquireErr: acquireErr},
		{name: "成功", acquired: true},
		{name: "合并业务与释放错误", acquired: true, workErr: workErr, unlockErr: unlockErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls, unlocks, acquires := 0, 0, 0
			keys := []string{" b ", "a", " b "}
			backend := tryManyLockerFunc(func(_ context.Context, got []string) (Lock, bool, error) {
				acquires++
				if !reflect.DeepEqual(got, []string{" b ", "a"}) {
					t.Fatalf("应复制去重并保留 key 原值: %q", got)
				}
				got[0] = "changed"
				if !tc.acquired {
					return nil, false, tc.acquireErr
				}
				return unlockFunc(func(context.Context) error {
					unlocks++
					return tc.unlockErr
				}), true, nil
			})
			e, err := New(executorBackend{TryManyLocker: backend})
			if err != nil {
				t.Fatal(err)
			}
			executed, err := e.TryDoMany(context.Background(), keys, func(context.Context) error {
				calls++
				return tc.workErr
			})
			want := 0
			if tc.acquired {
				want = 1
			}
			if executed != tc.acquired || acquires != 1 || calls != want || unlocks != want || keys[0] != " b " {
				t.Fatalf("整批执行次数或输入切片错误: %v %d %d %d %q", executed, acquires, calls, unlocks, keys)
			}
			if (err != nil) != (tc.acquireErr != nil || tc.workErr != nil || tc.unlockErr != nil) {
				t.Fatalf("错误状态: %v", err)
			}
			for _, cause := range []error{tc.acquireErr, tc.workErr, tc.unlockErr} {
				if cause != nil && !errors.Is(err, cause) {
					t.Fatalf("丢失错误链: %v", err)
				}
			}
		})
	}
}

func TestTryDoManyValidation(t *testing.T) {
	backend := tryManyLockerFunc(func(context.Context, []string) (Lock, bool, error) {
		t.Fatal("无效参数不能获取锁")
		return nil, false, nil
	})
	work := func(context.Context) error {
		t.Fatal("不能执行业务")
		return nil
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		ctx  context.Context
		keys []string
		fn   func(context.Context) error
	}{
		{nil, []string{"a"}, work},
		{context.Background(), nil, work},
		{context.Background(), []string{"a", " \t"}, work},
		{context.Background(), []string{"a"}, nil},
		{canceled, []string{"a"}, work},
	} {
		if executed, err := TryDoMany(tc.ctx, backend, tc.keys, tc.fn); executed || err == nil {
			t.Fatalf("无效请求被放行: %v %v", executed, err)
		}
	}
	if _, err := TryDoMany(context.Background(), nil, []string{"a"}, work); err == nil {
		t.Fatal("nil 后端")
	}
	for _, e := range []*Executor{nil, {}} {
		if _, err := e.TryDoMany(context.Background(), []string{"a"}, work); err == nil {
			t.Fatal("未初始化执行器")
		}
	}
}

func TestTryDoManyLeaseAndCleanup(t *testing.T) {
	for _, mode := range []string{"cancel", "panic", "expired"} {
		t.Run(mode, func(t *testing.T) {
			type requestKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), requestKey{}, "value"))
			defer cancel()
			deadline := time.Now().Add(time.Minute)
			if mode == "expired" {
				deadline = time.Now().Add(-time.Second)
			}
			unlocked := false
			backend := tryManyLockerFunc(func(context.Context, []string) (Lock, bool, error) {
				return leaseHandle{Lock: unlockFunc(func(ctx context.Context) error {
					unlocked = true
					until, ok := ctx.Deadline()
					if ctx.Err() != nil || ctx.Value(requestKey{}) != "value" || !ok || time.Until(until) <= 0 || time.Until(until) > time.Second {
						t.Error("释放必须使用独立期限并保留值")
					}
					return nil
				}), deadline: deadline}, true, nil
			})
			defer func() {
				value := recover()
				if mode == "panic" && value != "original" || mode != "panic" && value != nil || !unlocked {
					t.Errorf("清理或 panic 错误: %v %v", value, unlocked)
				}
			}()
			executed, err := TryDoMany(ctx, backend, []string{"a", "b"}, func(work context.Context) error {
				if mode == "expired" {
					t.Fatal("过期句柄不能执行业务")
				}
				if got, ok := work.Deadline(); !ok || !got.Equal(deadline) {
					t.Fatal("必须透传租期")
				}
				if mode == "panic" {
					panic("original")
				}
				cancel()
				return work.Err()
			})
			if mode == "expired" {
				if executed || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("过期结果错误: %v %v", executed, err)
				}
			} else if !executed || !errors.Is(err, context.Canceled) {
				t.Fatalf("取消结果错误: %v %v", executed, err)
			}
		})
	}
}
