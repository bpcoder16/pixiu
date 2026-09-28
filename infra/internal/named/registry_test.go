package named_test

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

type testClient struct {
	closed atomic.Int32
	err    error
}

func (c *testClient) Close() error {
	c.closed.Add(1)
	return c.err
}

func assertPanic(t *testing.T, want string, run func()) {
	t.Helper()
	defer func() {
		if got := recover(); got != want {
			t.Fatalf("panic = %v, want %q", got, want)
		}
	}()
	run()
}

func TestCreateAndConcurrentLookup(t *testing.T) {
	registry := named.New[*testClient]("example")
	client := &testClient{}
	builds := 0
	created, err := registry.Create("shared", func() (*testClient, error) {
		builds++
		return client, nil
	})
	if err != nil || created != client {
		t.Fatalf("创建客户端 = %p, %v", created, err)
	}
	if _, err := registry.Create("shared", func() (*testClient, error) {
		builds++
		return &testClient{}, nil
	}); err == nil || err.Error() != `example: client "shared" is already registered` {
		t.Fatalf("重复登记错误 = %v", err)
	}
	if builds != 1 {
		t.Fatalf("构造执行 %d 次, want 1", builds)
	}
	assertPanic(t, `example: client "missing" is not registered`, func() {
		registry.MustGet("missing")
	})

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				if got := registry.MustGet("shared"); got != client {
					t.Errorf("并发查询得到 %p, want %p", got, client)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := registry.CloseAll(); err != nil {
		t.Fatal(err)
	}
	if got := client.closed.Load(); got != 1 {
		t.Fatalf("客户端关闭 %d 次, want 1", got)
	}
	if _, err := registry.Create("later", func() (*testClient, error) {
		return &testClient{}, nil
	}); err == nil || err.Error() != "example: named clients closed" {
		t.Fatalf("关闭后登记错误 = %v", err)
	}
	assertPanic(t, "example: named clients closed", func() {
		registry.MustGet("shared")
	})
}

func TestConcurrentCreateClosesUnregisteredClient(t *testing.T) {
	registry := named.New[*testClient]("example")
	first := &testClient{}
	closeErr := errors.New("duplicate close failed")
	second := &testClient{err: closeErr}
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	type createResult struct {
		client *testClient
		err    error
	}
	firstResult := make(chan createResult, 1)
	secondResult := make(chan createResult, 1)
	go func() {
		client, err := registry.Create("shared", func() (*testClient, error) {
			close(firstEntered)
			<-releaseFirst
			return first, nil
		})
		firstResult <- createResult{client: client, err: err}
	}()
	<-firstEntered
	go func() {
		client, err := registry.Create("shared", func() (*testClient, error) {
			close(secondEntered)
			<-releaseSecond
			return second, nil
		})
		secondResult <- createResult{client: client, err: err}
	}()
	<-secondEntered
	close(releaseFirst)
	firstOutcome := <-firstResult
	close(releaseSecond)
	result := <-secondResult
	if firstOutcome.client != first || firstOutcome.err != nil {
		t.Fatalf("首次登记 = %p, %v", firstOutcome.client, firstOutcome.err)
	}
	if result.client != nil || result.err == nil {
		t.Fatalf("重复登记 = %p, %v", result.client, result.err)
	}
	if !strings.Contains(result.err.Error(), `example: client "shared" is already registered`) {
		t.Fatalf("重复登记错误缺少名称: %v", result.err)
	}
	if !errors.Is(result.err, closeErr) {
		t.Fatalf("重复登记错误未包含关闭失败: %v", result.err)
	}
	if got := registry.MustGet("shared"); got != first {
		t.Fatalf("重复登记替换了首次客户端: got=%p", got)
	}
	if got := second.closed.Load(); got != 1 {
		t.Fatalf("重复客户端关闭 %d 次, want 1", got)
	}
	if got := first.closed.Load(); got != 0 {
		t.Fatalf("已登记客户端提前关闭 %d 次", got)
	}
	if err := registry.CloseAll(); err != nil {
		t.Fatal(err)
	}
	if got := first.closed.Load(); got != 1 {
		t.Fatalf("已登记客户端关闭 %d 次, want 1", got)
	}
}

func TestCreateFailureAndCloseErrors(t *testing.T) {
	registry := named.New[*testClient]("example")
	buildErr := errors.New("build failed")
	if _, err := registry.Create("first", func() (*testClient, error) {
		return nil, buildErr
	}); !errors.Is(err, buildErr) {
		t.Fatalf("构造错误 = %v", err)
	}
	assertPanic(t, `example: client "first" is not registered`, func() {
		registry.MustGet("first")
	})
	closeErrors := []error{
		errors.New("first close failed"),
		errors.New("second close failed"),
	}
	clients := []*testClient{
		{err: closeErrors[0]},
		{err: closeErrors[1]},
	}
	names := []string{"first", "second"}
	for i, client := range clients {
		name := names[i]
		if _, err := registry.Create(name, func() (*testClient, error) {
			return client, nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	results := make([]error, 2)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i] = registry.CloseAll()
		}()
	}
	wg.Wait()
	for _, err := range results {
		for _, want := range closeErrors {
			if !errors.Is(err, want) {
				t.Fatalf("关闭错误 %v 未包含 %v", err, want)
			}
		}
	}
	if results[0] != results[1] {
		t.Fatal("并发 CloseAll 未返回同一次结果")
	}
	for _, client := range clients {
		if got := client.closed.Load(); got != 1 {
			t.Fatalf("客户端关闭 %d 次, want 1", got)
		}
	}
}
