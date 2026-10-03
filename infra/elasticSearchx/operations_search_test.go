package elasticSearchx

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
)

func TestSearchWithoutTotal(t *testing.T) {
	c := &Client{
		name:           "search",
		durationPrefix: "elasticSearch_search",
		performer: performerFunc(func(req *http.Request) (*http.Response, error) {
			var dsl struct {
				TrackTotalHits *bool `json:"track_total_hits"`
			}
			if err := json.NewDecoder(req.Body).Decode(&dsl); err != nil {
				t.Fatal(err)
			}
			if dsl.TrackTotalHits == nil || *dsl.TrackTotalHits {
				t.Fatal("未发送 track_total_hits: false")
			}
			return jsonResponse(req, http.StatusOK, `{"took":3,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"hits":[{"_source":{"id":9007199254740993}}]}}`), nil
		}),
	}
	result, err := c.Search(context.Background(), "products", map[string]any{
		"track_total_hits": false,
	})
	if err != nil {
		t.Fatalf("未统计总数的合法响应被拒绝: %v", err)
	}
	if result.Total != nil {
		t.Fatalf("未统计总数应为 nil: %+v", result.Total)
	}
	if string(result.Hits) != `[{"_source":{"id":9007199254740993}}]` {
		t.Fatalf("命中结果丢失整数精度: %s", result.Hits)
	}
}

func TestSearchPreservesResponseStatus(t *testing.T) {
	// TerminatedEarly 启用后同步恢复变量和下方对应断言。
	// terminated, notTerminated := true, false
	tests := []struct {
		name string
		body string
		want SearchResult
	}{
		{
			name: "零命中与未统计总数区分",
			body: `{"took":0,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":0,"relation":"eq"},"hits":[]}}`,
			want: SearchResult{
				Total: &Total{
					Value:    0,
					Relation: "eq",
				},
				Hits: json.RawMessage(`[]`),
				Shards: ShardsInfo{
					Total:      1,
					Successful: 1,
				},
			},
		},
		{
			name: "数量下限与暂不启用的提前终止标记",
			body: `{"took":17,"timed_out":false,"_shards":{"total":2,"successful":2,"skipped":1,"failed":0},"hits":{"total":{"value":10000,"relation":"gte"},"hits":[]},"terminated_early":false}`,
			want: SearchResult{
				Total: &Total{
					Value:    10000,
					Relation: "gte",
				},
				Hits: json.RawMessage(`[]`),
				// Took: 17,
				Shards: ShardsInfo{
					Total:      2,
					Successful: 2,
					Skipped:    1,
				},
				// TerminatedEarly: &notTerminated,
			},
		},
		{
			name: "超时保留部分命中与聚合",
			body: `{"took":50,"timed_out":true,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"hits":[{"_id":"1","sort":[9007199254740993],"_source":{"id":9007199254740993}}]},"aggregations":{"ids":{"buckets":[{"key":9007199254740993,"doc_count":1}]}}}`,
			want: SearchResult{
				Hits:         json.RawMessage(`[{"_id":"1","sort":[9007199254740993],"_source":{"id":9007199254740993}}]`),
				Aggregations: json.RawMessage(`{"ids":{"buckets":[{"key":9007199254740993,"doc_count":1}]}}`),
				// Took: 50,
				TimedOut: true,
				Shards: ShardsInfo{
					Total:      1,
					Successful: 1,
				},
			},
		},
		{
			name: "分片失败保留原始原因",
			body: `{"took":23,"timed_out":false,"_shards":{"total":2,"successful":1,"skipped":0,"failed":1,"failures":[{"shard":0,"index":"products","reason":{"type":"query_shard_exception","reason":"查询失败","caused_by":{"type":"test_error","id":9007199254740993}}}]},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1"}]}}`,
			want: SearchResult{
				Total: &Total{
					Value:    1,
					Relation: "eq",
				},
				Hits: json.RawMessage(`[{"_id":"1"}]`),
				// Took: 23,
				Shards: ShardsInfo{
					Total:      2,
					Successful: 1,
					Failed:     1,
					Failures:   json.RawMessage(`[{"shard":0,"index":"products","reason":{"type":"query_shard_exception","reason":"查询失败","caused_by":{"type":"test_error","id":9007199254740993}}}]`),
				},
			},
		},
		{
			name: "超时与分片失败同时保留",
			body: `{"timed_out":true,"_shards":{"total":2,"successful":1,"failed":1},"hits":{"hits":[]}}`,
			want: SearchResult{
				Hits:     json.RawMessage(`[]`),
				TimedOut: true,
				Shards: ShardsInfo{
					Total:      2,
					Successful: 1,
					Failed:     1,
				},
			},
		},
		{
			name: "提前终止标记暂不启用",
			body: `{"took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_id":"1"}]},"terminated_early":true}`,
			want: SearchResult{
				Total: &Total{
					Value:    1,
					Relation: "eq",
				},
				Hits: json.RawMessage(`[{"_id":"1"}]`),
				// Took: 1,
				Shards: ShardsInfo{
					Total:      1,
					Successful: 1,
				},
				// TerminatedEarly: &terminated,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				name:           "search",
				durationPrefix: "elasticSearch_search",
				performer: performerFunc(func(req *http.Request) (*http.Response, error) {
					return jsonResponse(req, http.StatusOK, tt.body), nil
				}),
			}
			result, err := c.Search(context.Background(), "products", map[string]any{
				"query": map[string]any{
					"match_all": map[string]any{},
				},
			})
			if tt.want.TimedOut || tt.want.Shards.Failed > 0 {
				var partialErr *PartialSearchError
				if !errors.As(err, &partialErr) || partialErr.TimedOut != tt.want.TimedOut || partialErr.FailedShards != tt.want.Shards.Failed {
					t.Fatalf("部分搜索结果错误未保留: err=%v", err)
				}
			} else if err != nil {
				t.Fatalf("合法搜索响应被拒绝: %v", err)
			}
			if !reflect.DeepEqual(result, tt.want) {
				t.Fatalf("搜索结果与状态未完整保留: got=%+v want=%+v", result, tt.want)
			}
		})
	}
}

func TestSearchWithoutTotalRejectsMissingHits(t *testing.T) {
	c := &Client{
		name:           "search",
		durationPrefix: "elasticSearch_search",
		performer: performerFunc(func(req *http.Request) (*http.Response, error) {
			return jsonResponse(req, http.StatusOK, `{"took":0,"timed_out":false,"_shards":{"total":1,"successful":1,"failed":0},"hits":{}}`), nil
		}),
	}
	if _, err := c.Search(context.Background(), "products", map[string]any{
		"track_total_hits": false,
	}); err == nil {
		t.Fatal("未统计总数时接受了缺失 hits 的响应")
	}
}
