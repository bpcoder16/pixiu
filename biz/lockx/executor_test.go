package lockx

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
)

type executorBackend struct {
	Locker
	TryLocker
}

func isolatedExecutor(t *testing.T, run func()) {
	t.Helper()
	const marker = "PIXIU_LOCKX_EXECUTOR_TEST"
	if os.Getenv(marker) == t.Name() {
		run()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("执行器独立进程测试失败: %v\n%s", err, output)
	}
}

func requireExecutorPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("未登记的执行器应 panic")
		}
	}()
	fn()
}

func TestExecutorRegistration(t *testing.T) {
	isolatedExecutor(t, func() {
		backend := executorBackend{}
		requireExecutorPanic(t, func() { Default() })
		requireExecutorPanic(t, func() { Named("missing") })
		if _, err := New(backend); err != nil {
			t.Fatal(err)
		}
		requireExecutorPanic(t, func() { Default() })
		for _, name := range []string{"", " \t"} {
			if executor, err := NewNamed(name, backend); executor != nil || err == nil {
				t.Fatalf("应拒绝空名称: executor=%v err=%v", executor, err)
			}
		}
		for _, backend := range []Backend{nil, (*executorBackend)(nil)} {
			if executor, err := NewDefault("default", backend); executor != nil || err == nil {
				t.Fatalf("应拒绝 nil 后端: executor=%v err=%v", executor, err)
			}
		}
		named, err := NewNamed("slow", backend)
		if err != nil {
			t.Fatal(err)
		}
		requireExecutorPanic(t, func() { Default() })
		if _, err := NewDefault("slow", backend); err == nil {
			t.Fatal("已有命名实例不能被覆盖为默认实例")
		}
		def, err := NewDefault("default", backend)
		if err != nil {
			t.Fatal(err)
		}
		if Default() != def || Named("default") != def || Named("slow") != named || named == def {
			t.Fatal("默认应指向对应命名实例，其他命名实例应独立")
		}
		if _, err := NewNamed("default", backend); err == nil {
			t.Fatal("重复名称应失败")
		}
		if _, err := NewDefault("other", backend); err == nil {
			t.Fatal("第二个默认实例应失败")
		}
		if _, err := NewNamed("other", backend); err != nil {
			t.Fatalf("失败不得占用名称: %v", err)
		}
		if Default() != def || Named("slow") != named {
			t.Fatal("登记失败不应替换已发布的实例")
		}
	})
}

func TestExecutorConcurrentReuse(t *testing.T) {
	isolatedExecutor(t, func() {
		type requestKey struct{}
		var acquired, released atomic.Int32
		acquire := func(ctx context.Context, key string) (Lock, error) {
			if ctx.Value(requestKey{}) != key {
				return nil, errors.New("执行器混用了请求 context 或 key")
			}
			acquired.Add(1)
			return unlockFunc(func(context.Context) error {
				released.Add(1)
				return nil
			}), nil
		}
		backend := executorBackend{
			Locker: lockerFunc(acquire),
			TryLocker: tryLockerFunc(func(ctx context.Context, key string) (Lock, bool, error) {
				handle, err := acquire(ctx, key)
				return handle, err == nil, err
			}),
		}
		if _, err := NewDefault("shared", backend); err != nil {
			t.Fatal(err)
		}
		var workers sync.WaitGroup
		for i := range 32 {
			workers.Go(func() {
				key := string(rune('a' + i))
				ctx := context.WithValue(context.Background(), requestKey{}, key)
				work := func(ctx context.Context) error {
					if ctx.Value(requestKey{}) != key {
						return errors.New("回调丢失请求 context")
					}
					return nil
				}
				if i%2 == 0 {
					if err := Default().Do(ctx, key, work); err != nil {
						t.Error(err)
					}
				} else if executed, err := Named("shared").TryDo(ctx, key, work); !executed || err != nil {
					t.Errorf("TryDo: executed=%v err=%v", executed, err)
				}
			})
		}
		workers.Wait()
		if acquired.Load() != 32 || released.Load() != 32 {
			t.Fatalf("每次获取必须独立释放: acquired=%d released=%d", acquired.Load(), released.Load())
		}
	})
}
