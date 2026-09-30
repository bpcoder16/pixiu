package named_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

func TestDefaultIdentityAndClose(t *testing.T) {
	registry := named.New[*testClient]("example")
	other := &testClient{}
	if _, err := registry.Create("default", func() (*testClient, error) {
		return other, nil
	}); err != nil {
		t.Fatal(err)
	}
	assertPanic(t, "example: default client is not registered", func() {
		registry.MustDefault()
	})

	closeErr := errors.New("default close failed")
	client := &testClient{err: closeErr}
	created, err := registry.CreateDefault("orders", func() (*testClient, error) {
		return client, nil
	})
	if err != nil || created != client {
		t.Fatalf("创建默认客户端 = %p, %v", created, err)
	}
	for _, name := range []string{"orders", "rejected"} {
		if got, err := registry.CreateDefault(name, func() (*testClient, error) {
			t.Fatal("重复默认初始化执行了构造")
			return nil, nil
		}); got != nil || err == nil || err.Error() != "example: default client is already registered" {
			t.Fatalf("重复默认初始化 = %p, %v", got, err)
		}
	}
	assertPanic(t, `example: client "rejected" is not registered`, func() {
		registry.MustGet("rejected")
	})
	if _, err := registry.Create("orders", func() (*testClient, error) {
		t.Fatal("默认名称重复登记执行了构造")
		return nil, nil
	}); err == nil || err.Error() != `example: client "orders" is already registered` {
		t.Fatalf("默认名称重复登记错误 = %v", err)
	}

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if registry.MustDefault() != client || registry.MustGet("orders") != client {
					t.Error("默认查询与命名查询未返回同一实例")
					return
				}
			}
		}()
	}
	wg.Wait()
	results := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = registry.CloseAll()
		}()
	}
	wg.Wait()
	if !errors.Is(results[0], closeErr) || results[0] != results[1] || registry.CloseAll() != results[0] {
		t.Fatalf("关闭结果未复用或丢失错误: %v", results)
	}
	for _, c := range []*testClient{client, other} {
		if got := c.closed.Load(); got != 1 {
			t.Fatalf("客户端关闭 %d 次, want 1", got)
		}
	}
	assertPanic(t, "example: named clients closed", func() {
		registry.MustDefault()
	})
	assertPanic(t, "example: named clients closed", func() {
		registry.MustGet("orders")
	})
	if got, err := registry.CreateDefault("later", func() (*testClient, error) {
		t.Fatal("关闭后默认初始化执行了构造")
		return nil, nil
	}); got != nil || err == nil || err.Error() != "example: named clients closed" {
		t.Fatalf("关闭后默认初始化 = %p, %v", got, err)
	}
}

func TestDefaultFailureAndNameConflictAllowRetry(t *testing.T) {
	registry := named.New[*testClient]("example")
	t.Cleanup(func() { _ = registry.CloseAll() })
	buildErr := errors.New("build failed")
	if got, err := registry.CreateDefault("orders", func() (*testClient, error) {
		return nil, buildErr
	}); got != nil || !errors.Is(err, buildErr) {
		t.Fatalf("默认构造失败 = %p, %v", got, err)
	}
	assertPanic(t, `example: client "orders" is not registered`, func() {
		registry.MustGet("orders")
	})
	other := &testClient{}
	if _, err := registry.Create("taken", func() (*testClient, error) {
		return other, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := registry.CreateDefault("taken", func() (*testClient, error) {
		t.Fatal("名称冲突执行了默认构造")
		return nil, nil
	}); got != nil || err == nil || err.Error() != `example: client "taken" is already registered` {
		t.Fatalf("默认名称冲突 = %p, %v", got, err)
	}
	assertPanic(t, "example: default client is not registered", func() {
		registry.MustDefault()
	})
	client := &testClient{}
	if got, err := registry.CreateDefault("orders", func() (*testClient, error) {
		return client, nil
	}); got != client || err != nil {
		t.Fatalf("默认重试 = %p, %v", got, err)
	}
	if registry.MustDefault() != client || registry.MustGet("orders") != client || registry.MustGet("taken") != other {
		t.Fatal("默认重试改变了命名实例")
	}
}
