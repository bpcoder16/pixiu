package elasticsearchx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	elasticsearchxv7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
	elasticsearchxv8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	elasticsearchxv9 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v9"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestVersionNamedAndDefaultFactoriesShareRegistry(t *testing.T) {
	elasticsearchx.ResetNamedClientsForTest(t)
	var stack lifecycle.Stack
	if err := stack.Register(elasticsearchx.CloseAll); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := stack.Close(); err != nil {
			t.Error(err)
		}
	})
	for _, factory := range []struct {
		major      int
		newNamed   func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
		newDefault func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
	}{
		{7, elasticsearchxv7.NewNamed, elasticsearchxv7.NewDefault},
		{8, elasticsearchxv8.NewNamed, elasticsearchxv8.NewDefault},
		{9, elasticsearchxv9.NewNamed, elasticsearchxv9.NewDefault},
	} {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			var startupCalls atomic.Int32
			var rejectStartup atomic.Bool
			rejectStartup.Store(true)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				if req.URL.Path == "/" {
					startupCalls.Add(1)
					if rejectStartup.Load() {
						w.WriteHeader(http.StatusServiceUnavailable)
						fmt.Fprint(w, `{"error":{"type":"unavailable"}}`)
						return
					}
					fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
					return
				}
				fmt.Fprint(w, `{"count":4}`)
			}))
			defer server.Close()
			cfg := elasticsearchx.Config{
				Name:      fmt.Sprintf("%s-v%d", t.Name(), factory.major),
				Addresses: []string{server.URL},
			}
			ctx := context.Background()
			if got, err := factory.newDefault(cfg, elasticsearchx.OptLogRequests(false)); got != nil || err == nil {
				t.Fatalf("默认启动失败未传播: client=%p err=%v", got, err)
			}
			rejectStartup.Store(false)
			client, err := factory.newNamed(cfg, elasticsearchx.OptLogRequests(false))
			if err != nil {
				t.Fatalf("默认初始化失败后，同名命名初始化失败: %v", err)
			}
			if elasticsearchx.Named(cfg.Name) != client {
				t.Fatal("版本适配包未登记到公共注册表")
			}
			if count, err := client.Count(ctx, "products", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}); err != nil || count != 4 {
				t.Fatalf("命名实例不可用: count=%d err=%v", count, err)
			}
			callsBeforeDuplicates := startupCalls.Load()
			// 不同版本也共用名称空间，名称冲突必须在发送启动请求之前拒绝。
			if got, err := elasticsearchxv7.NewNamed(cfg); got != nil || err == nil || err.Error() != fmt.Sprintf("elasticsearchx: client %q is already registered", cfg.Name) {
				t.Fatalf("跨版本重复名称 = %p, %v", got, err)
			}
			if got, err := factory.newDefault(cfg); got != nil || err == nil || err.Error() != fmt.Sprintf("elasticsearchx: client %q is already registered", cfg.Name) {
				t.Fatalf("默认名称冲突 = %p, %v", got, err)
			}
			if got := startupCalls.Load(); got != callsBeforeDuplicates {
				t.Fatalf("重复初始化仍发送启动请求: calls=%d, want %d", got, callsBeforeDuplicates)
			}
			if factory.major == 9 {
				cfg.Name += "-default"
				defaultClient, err := factory.newDefault(cfg, elasticsearchx.OptLogRequests(false))
				if err != nil {
					t.Fatal(err)
				}
				if elasticsearchx.Default() != defaultClient || elasticsearchx.Named(cfg.Name) != defaultClient {
					t.Fatal("默认和命名查询未返回同一个版本客户端")
				}
				callsBeforeDuplicateDefault := startupCalls.Load()
				if got, err := elasticsearchxv8.NewDefault(cfg); got != nil || err == nil || err.Error() != "elasticsearchx: default client is already registered" {
					t.Fatalf("跨版本重复默认初始化 = %p, %v", got, err)
				}
				if got := startupCalls.Load(); got != callsBeforeDuplicateDefault {
					t.Fatalf("重复默认初始化仍发送启动请求: calls=%d, want %d", got, callsBeforeDuplicateDefault)
				}
			}
		})
	}
	if err := stack.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := elasticsearchxv8.NewNamed(elasticsearchx.Config{Name: "later"}); got != nil || err == nil || err.Error() != "elasticsearchx: named clients closed" {
		t.Fatalf("关闭后仍执行版本初始化: client=%p err=%v", got, err)
	}
}
