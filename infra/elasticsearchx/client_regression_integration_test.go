package elasticsearchx_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	v7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
	v8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	v9 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v9"
)

func TestVersionFactoriesUseNodeHostAndRejectClosedOperations(t *testing.T) {
	for _, factory := range []struct {
		major int
		open  func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
	}{{7, v7.New}, {8, v8.New}, {9, v9.New}} {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			var requests atomic.Int32
			var host string
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests.Add(1)
				if req.Host != host {
					t.Errorf("请求 Host = %q, want %q", req.Host, host)
					w.WriteHeader(http.StatusMisdirectedRequest)
					return
				}
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				if req.URL.Path == "/" {
					fmt.Fprintf(w, `{"version":{"number":"%d.17.10","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
					return
				}
				if req.URL.Path == "/blocked/_count" {
					select {
					case <-req.Context().Done():
					case <-release:
					}
					return
				}
				fmt.Fprint(w, `{"count":1}`)
			}))
			defer server.Close()
			defer close(release)
			host = strings.TrimPrefix(server.URL, "http://")
			c, err := factory.open(elasticsearchx.Config{
				Name:      "host",
				Addresses: []string{server.URL},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if _, err := c.Count(ctx, "blocked", map[string]any{}); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("业务操作未遵守调用方期限: %v", err)
			}
			// 创建期限释放和一次操作超时均不能结束客户端生命周期。
			if _, err := c.Count(context.Background(), "products", map[string]any{}); err != nil {
				t.Fatal(err)
			}
			before := requests.Load()
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Count(context.Background(), "products", map[string]any{}); err == nil || requests.Load() != before {
				t.Fatalf("关闭后仍执行操作: err=%v requests=%d want=%d", err, requests.Load(), before)
			}
		})
	}
}

func TestVersionFactoriesUseStartupTimeout(t *testing.T) {
	for _, factory := range []struct {
		major int
		open  func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
	}{{7, v7.New}, {8, v8.New}, {9, v9.New}} {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				// TCP 已连接仍不返回响应，确保启动期限覆盖拨号后的验活。
				select {
				case <-req.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			done := make(chan error, 1)
			go func() {
				client, err := factory.open(elasticsearchx.Config{
					Name:           "startup-timeout",
					Addresses:      []string{server.URL},
					StartupTimeout: 50 * time.Millisecond,
				})
				if client != nil {
					_ = client.Close()
					t.Error("验活超时仍返回客户端")
				}
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("启动验活未使用配置期限: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("启动验活未在配置期限后返回")
			}
		})
	}
}
