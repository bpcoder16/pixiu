package elasticSearchx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// ResetNamedClientsForTest 仅在测试构建中导出，让版本集成测试隔离进程级注册表。
func ResetNamedClientsForTest(t *testing.T) {
	t.Helper()
	previous := namedClients
	registry := named.New[managedClient]("elasticSearchx")
	namedClients = registry
	t.Cleanup(func() {
		_ = registry.CloseAll()
		namedClients = previous
	})
}

func assertRegistryPanic(t *testing.T, want string, lookup func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("客户端查询 panic = %v, want %q", got, want)
		}
	}()
	lookup()
}

func TestNamedAndDefaultInitialization(t *testing.T) {
	ResetNamedClientsForTest(t)
	assertRegistryPanic(t, "elasticSearchx: default client is not registered", func() {
		Default()
	})
	buildErr := errors.New("startup failed")
	for _, create := range []func(string, func() (*Client, error)) (*Client, error){RegisterNamed, RegisterDefault} {
		for _, name := range []string{"", " \t "} {
			if got, err := create(name, func() (*Client, error) {
				t.Fatal("空名称执行了构造")
				return nil, nil
			}); got != nil || err == nil || err.Error() != "elasticSearchx: empty client name" {
				t.Fatalf("空名称 %q: client=%p err=%v", name, got, err)
			}
		}
		if got, err := create("retry", func() (*Client, error) {
			return nil, buildErr
		}); got != nil || !errors.Is(err, buildErr) {
			t.Fatalf("构造失败 = %p, %v", got, err)
		}
		assertRegistryPanic(t, `elasticSearchx: client "retry" is not registered`, func() {
			Named("retry")
		})
		assertRegistryPanic(t, "elasticSearchx: default client is not registered", func() {
			Default()
		})
	}
	other := &Client{name: "taken"}
	if got, err := RegisterNamed("taken", func() (*Client, error) {
		return other, nil
	}); got != other || err != nil {
		t.Fatalf("命名初始化 = %p, %v", got, err)
	}
	assertRegistryPanic(t, "elasticSearchx: default client is not registered", func() {
		Default()
	})
	if got, err := RegisterDefault("taken", func() (*Client, error) {
		t.Fatal("名称冲突执行了构造")
		return nil, nil
	}); got != nil || err == nil || err.Error() != `elasticSearchx: client "taken" is already registered` {
		t.Fatalf("默认名称冲突 = %p, %v", got, err)
	}
	client := &Client{name: "retry"}
	if got, err := RegisterDefault("retry", func() (*Client, error) {
		return client, nil
	}); got != client || err != nil {
		t.Fatalf("默认重试 = %p, %v", got, err)
	}
	if Default() != client || Named("retry") != client || Named("taken") != other {
		t.Fatal("默认与命名查询未返回已登记的实例")
	}
	for _, name := range []string{"retry", "rejected"} {
		if got, err := RegisterDefault(name, func() (*Client, error) {
			t.Fatal("重复默认初始化执行了构造")
			return nil, nil
		}); got != nil || err == nil || err.Error() != "elasticSearchx: default client is already registered" {
			t.Fatalf("重复默认初始化 = %p, %v", got, err)
		}
	}
	if _, err := RegisterNamed("retry", func() (*Client, error) {
		t.Fatal("重复命名初始化执行了构造")
		return nil, nil
	}); err == nil || err.Error() != `elasticSearchx: client "retry" is already registered` {
		t.Fatalf("重复命名初始化错误 = %v", err)
	}
	assertRegistryPanic(t, `elasticSearchx: client "rejected" is not registered`, func() {
		Named("rejected")
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				if Default() != client || Named("retry") != client || Named("taken") != other {
					t.Error("并发查询改变了客户端实例")
					return
				}
			}
		})
	}
	wg.Wait()
}

func TestCloseAllWithLifecycle(t *testing.T) {
	ResetNamedClientsForTest(t)
	closeErr := errors.New("native close failed")
	var closeCounts [2]atomic.Int32
	for i, create := range []func(string, func() (*Client, error)) (*Client, error){RegisterNamed, RegisterDefault} {
		name := fmt.Sprintf("search-%d", i)
		client := &Client{
			name: name,
			closeClient: func(ctx context.Context) error {
				if ctx == nil || ctx.Err() != nil {
					t.Error("统一关闭使用了无效的 context")
				}
				closeCounts[i].Add(1)
				return closeErr
			},
		}
		if _, err := create(name, func() (*Client, error) {
			return client, nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	var stack lifecycle.Stack
	if err := stack.Register(func() error {
		for i := range closeCounts {
			if got := closeCounts[i].Load(); got != 1 {
				t.Errorf("日志关闭时客户端 %d 已关闭 %d 次, want 1", i, got)
				return fmt.Errorf("日志关闭时客户端 %d 已关闭 %d 次, want 1", i, got)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := stack.Register(CloseAll); err != nil {
		t.Fatal(err)
	}
	if err := stack.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("生命周期关闭丢失客户端错误: %v", err)
	}
	first := CloseAll()
	if !errors.Is(first, closeErr) || CloseAll() != first {
		t.Fatalf("重复关闭未复用结果: %v", first)
	}
	if !strings.Contains(first.Error(), "search-0") || !strings.Contains(first.Error(), "search-1") {
		t.Fatalf("统一关闭未汇总全部客户端错误: %v", first)
	}
	for i := range closeCounts {
		if got := closeCounts[i].Load(); got != 1 {
			t.Fatalf("客户端 %d 关闭 %d 次, want 1", i, got)
		}
	}
	for _, lookup := range []func(){func() { Named("search-0") }, func() { Default() }} {
		assertRegistryPanic(t, "elasticSearchx: named clients closed", lookup)
	}
	for _, create := range []func(string, func() (*Client, error)) (*Client, error){RegisterNamed, RegisterDefault} {
		if got, err := create("later", func() (*Client, error) {
			t.Fatal("关闭后执行了构造")
			return nil, nil
		}); got != nil || err == nil || err.Error() != "elasticSearchx: named clients closed" {
			t.Fatalf("关闭后初始化 = %p, %v", got, err)
		}
	}
}
