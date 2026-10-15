package elasticsearchx_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	elasticsearchxv7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
	elasticsearchxv8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	elasticsearchxv9 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v9"
)

func TestVersionFactoriesReloadSearchAnalyzers(t *testing.T) {
	factories := []struct {
		major int
		open  func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error)
	}{
		{7, elasticsearchxv7.New},
		{8, elasticsearchxv8.New},
		{9, elasticsearchxv9.New},
	}
	for _, factory := range factories {
		for _, cancelStage := range []int{0, 1, 2} {
			t.Run(fmt.Sprintf("v%d/cancel_stage_%d", factory.major, cancelStage), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					w.Header().Set("X-Elastic-Product", "Elasticsearch")
					w.Header().Set("Content-Type", "application/json")
					if req.URL.Path == "/" {
						fmt.Fprintf(w, `{"version":{"number":"%d.17.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
						return
					}
					stage := int(calls.Add(1))
					path, query := "/products/_reload_search_analyzers", ""
					response := `{"_shards":{"total":1,"successful":1,"failed":0},"reload_details":[{"index":"products","reloaded_analyzers":["search_synonyms"],"reloaded_node_ids":["node-a"]}]}`
					if stage == 2 {
						path, query = "/products/_cache/clear", "request=true"
						response = `{"_shards":{"total":2,"successful":2,"failed":0}}`
					}
					body, err := io.ReadAll(req.Body)
					if stage > 2 || err != nil || len(body) != 0 || req.Method != http.MethodPost || req.URL.Path != path || req.URL.RawQuery != query {
						t.Errorf("请求或顺序错误: stage=%d method=%s url=%s body=%s err=%v", stage, req.Method, req.URL, body, err)
					}
					if stage == cancelStage {
						// 请求已到达服务端再取消，验证官方 SDK 的在途 HTTP 取消及阶段结果保留。
						cancel()
						select {
						case <-req.Context().Done():
						case <-time.After(3 * time.Second):
							t.Error("取消未传递到服务端")
						}
						return
					}
					_, _ = io.WriteString(w, response)
				}))
				defer server.Close()
				client, err := factory.open(elasticsearchx.Config{
					Name:      "catalog",
					Addresses: []string{server.URL},
				})
				if err != nil {
					t.Fatal(err)
				}
				defer client.Close()
				result, err := client.ReloadSearchAnalyzers(ctx, "products")
				if cancelStage == 0 {
					if err != nil || calls.Load() != 2 || result.Shards.Successful != 1 || len(result.ReloadDetails) != 1 || result.CacheShards == nil || result.CacheShards.Successful != 2 {
						t.Fatalf("重载结果错误: result=%+v err=%v calls=%d", result, err, calls.Load())
					}
					detail := result.ReloadDetails[0]
					if detail.Index != "products" || len(detail.ReloadedAnalyzers) != 1 || detail.ReloadedAnalyzers[0] != "search_synonyms" || len(detail.ReloadedNodeIDs) != 1 || detail.ReloadedNodeIDs[0] != "node-a" {
						t.Fatalf("重载详情错误: %+v", detail)
					}
				} else if !errors.Is(err, context.Canceled) || int(calls.Load()) != cancelStage || result.CacheShards != nil || (result.ReloadDetails != nil) != (cancelStage == 2) {
					t.Fatalf("取消未生效或丢失已完成结果: result=%+v err=%v calls=%d", result, err, calls.Load())
				}
			})
		}
	}
}
