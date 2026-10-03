package elasticSearchx_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/infra/elasticSearchx"
	v7 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v7"
	v8 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v8"
	v9 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v9"
)

func TestVersionFactoriesUseNodeHostAndRejectClosedOperations(t *testing.T) {
	for _, factory := range []struct {
		major int
		open  func(context.Context, elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
	}{{7, v7.New}, {8, v8.New}, {9, v9.New}} {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			var requests atomic.Int32
			var host string
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
				fmt.Fprint(w, `{"count":1}`)
			}))
			defer server.Close()
			host = strings.TrimPrefix(server.URL, "http://")
			c, err := factory.open(context.Background(), elasticSearchx.Config{
				Name:      "host",
				Addresses: []string{server.URL},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close(context.Background())
			if _, err := c.Count(context.Background(), "products", map[string]any{}); err != nil {
				t.Fatal(err)
			}
			before := requests.Load()
			if err := c.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Count(context.Background(), "products", map[string]any{}); err == nil || requests.Load() != before {
				t.Fatalf("关闭后仍执行操作: err=%v requests=%d want=%d", err, requests.Load(), before)
			}
		})
	}
}
