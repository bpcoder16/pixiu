package outbox_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"gorm.io/gorm"
)

// 默认实例不提供重置入口，各用例在独立进程内验证启动和使用流程。
func isolatedDefaults(t *testing.T) bool {
	t.Helper()
	const marker = "PIXIU_OUTBOX_DEFAULT_TEST"
	if os.Getenv(marker) == t.Name() {
		return true
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("默认实例独立进程测试失败: %v\n%s", err, output)
	}
	return false
}

func requireDefaultPanic(t *testing.T, want string, call func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Errorf("未初始化应明确 panic: got=%v want=%s", got, want)
		}
	}()
	call()
}

func TestDefaultRegistration(t *testing.T) {
	if !isolatedDefaults(t) {
		return
	}
	ctx := context.Background()
	for _, call := range []func(){
		func() { outbox.DefaultStore() },
		func() { _, _ = outbox.EnqueueTx(nil, outbox.Task{}) },
		func() { _, _ = outbox.Get(ctx, 1) },
	} {
		requireDefaultPanic(t, "outbox: default store is not registered", call)
	}
	for _, call := range []func(){
		func() { outbox.DefaultWorker() },
		func() { _ = outbox.Send(ctx, 1) },
		func() { _ = outbox.RetryPending(ctx, 1) },
	} {
		requireDefaultPanic(t, "outbox: default worker is not registered", call)
	}
	provider := func(context.Context) *gorm.DB { panic("注册不应访问数据库") }
	cfg := outbox.StoreConfig{
		Backend:  outbox.MySQL,
		MasterDB: provider,
	}
	handlers := map[outbox.HandlerKey]outbox.Handler{
		{Kind: "test.publish", Version: 1}: func(context.Context, outbox.Delivery) error { return nil },
	}
	store, err := outbox.NewStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outbox.NewWorker(store, handlers); err != nil {
		t.Fatal(err)
	}
	requireDefaultPanic(t, "outbox: default store is not registered", func() { outbox.DefaultStore() })
	requireDefaultPanic(t, "outbox: default worker is not registered", func() { outbox.DefaultWorker() })
	if worker, err := outbox.NewDefaultWorker(handlers); worker != nil || err == nil {
		t.Fatalf("未登记 Store 时不能创建默认 Worker: %p %v", worker, err)
	}
	for _, invalid := range []outbox.StoreConfig{
		{Backend: outbox.MySQL},
		{
			Backend:  "sqlite",
			MasterDB: provider,
		},
	} {
		if got, err := outbox.NewDefaultStore(invalid); got != nil || !errors.Is(err, outbox.ErrInvalidArgument) {
			t.Fatalf("无效 Store 配置应失败且允许重试: %p %v", got, err)
		}
	}
	requireDefaultPanic(t, "outbox: default store is not registered", func() { outbox.DefaultStore() })
	def, err := outbox.NewDefaultStore(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []map[outbox.HandlerKey]outbox.Handler{
		nil,
		{{Kind: "test.publish", Version: 0}: handlers[outbox.HandlerKey{Kind: "test.publish", Version: 1}]},
		{{Kind: "test.publish", Version: 1}: nil},
	} {
		if got, err := outbox.NewDefaultWorker(invalid); got != nil || !errors.Is(err, outbox.ErrInvalidArgument) {
			t.Fatalf("无效 Handler 应失败且允许重试: %p %v", got, err)
		}
	}
	requireDefaultPanic(t, "outbox: default worker is not registered", func() { outbox.DefaultWorker() })
	worker, err := outbox.NewDefaultWorker(handlers)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := outbox.NewDefaultStore(cfg); got != nil || err == nil {
		t.Fatalf("重复注册 Store 应失败: %p %v", got, err)
	}
	if got, err := outbox.NewDefaultWorker(handlers); got != nil || err == nil {
		t.Fatalf("重复注册 Worker 应失败: %p %v", got, err)
	}
	if outbox.DefaultStore() != def || def == store || outbox.DefaultWorker() != worker {
		t.Fatal("默认实例应保持稳定且独立于显式实例")
	}
}

func TestDefaultConcurrentRegistrationAndUse(t *testing.T) {
	if !isolatedDefaults(t) {
		return
	}
	type requestKey struct{}
	cfg := outbox.StoreConfig{
		Backend: outbox.MySQL,
		MasterDB: func(ctx context.Context) *gorm.DB {
			// 用已有错误的合法会话验证 Store 绑定与请求隔离，无需外部数据库。
			return &gorm.DB{
				Config: &gorm.Config{},
				Error:  ctx.Value(requestKey{}).(error),
			}
		},
	}
	var stores, workers atomic.Int32
	var group sync.WaitGroup
	for range 16 {
		group.Go(func() {
			if store, err := outbox.NewDefaultStore(cfg); err == nil {
				stores.Add(1)
				if store != outbox.DefaultStore() {
					t.Error("默认 Store 与成功登记的实例不同")
				}
			} else if store != nil {
				t.Error("失败登记不应返回 Store")
			}
		})
	}
	group.Wait()
	for range 16 {
		group.Go(func() {
			worker, err := outbox.NewDefaultWorker(map[outbox.HandlerKey]outbox.Handler{
				{Kind: "test.publish", Version: 1}: func(context.Context, outbox.Delivery) error { return nil },
			})
			if err == nil {
				workers.Add(1)
				if worker != outbox.DefaultWorker() {
					t.Error("默认 Worker 与成功登记的实例不同")
				}
			} else if worker != nil {
				t.Error("失败登记不应返回 Worker")
			}
		})
	}
	group.Wait()
	if stores.Load() != 1 || workers.Load() != 1 {
		t.Fatalf("只能各登记一个默认实例: stores=%d workers=%d", stores.Load(), workers.Load())
	}
	for range 16 {
		group.Go(func() {
			want := errors.New("本次请求的数据库错误")
			ctx := context.WithValue(context.Background(), requestKey{}, want)
			if _, err := outbox.Get(ctx, 1); !errors.Is(err, want) {
				t.Errorf("查询未使用默认 Store 与本次 context: %v", err)
			}
			if err := outbox.Send(ctx, 1); !errors.Is(err, want) {
				t.Errorf("发送未绑定默认 Store 与本次 context: %v", err)
			}
			if err := outbox.RetryPending(ctx, 1); !errors.Is(err, want) {
				t.Errorf("补偿未绑定默认 Store 与本次 context: %v", err)
			}
		})
	}
	group.Wait()
}
