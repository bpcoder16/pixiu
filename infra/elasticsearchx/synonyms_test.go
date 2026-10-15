package elasticsearchx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
)

const reloadSearchAnalyzersResponse = `{"_shards":{"total":2,"successful":2,"failed":0},"reload_details":[{"index":"products","reloaded_analyzers":["search_synonyms"],"reloaded_node_ids":["node-a","node-b"]}]}`
const clearRequestCacheResponse = `{"_shards":{"total":3,"successful":2,"failed":0}}`

func TestReloadSearchAnalyzersRequestsAndResults(t *testing.T) {
	buf := captureElasticsearchLogs(t)
	ctx := logit.WithStart(context.Background())
	var bodies []*errorResponseBody
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		if req.Context() != ctx || req.Method != http.MethodPost || req.ContentLength != 0 {
			t.Fatalf("错误的重载请求: %s %s", req.Method, req.URL)
		}
		response := reloadSearchAnalyzersResponse
		switch len(bodies) {
		case 0:
			if req.URL.Path != "/products/_reload_search_analyzers" || req.URL.RawQuery != "" {
				t.Fatalf("错误的 reload 路径: %s", req.URL)
			}
		case 1:
			if bodies[0].closed != 1 || req.URL.Path != "/products/_cache/clear" || req.URL.RawQuery != "request=true" {
				t.Fatalf("未先收尾 reload 或清理了非 request cache: %s", req.URL)
			}
			response = clearRequestCacheResponse
		default:
			t.Fatalf("出现额外请求: %s", req.URL)
		}
		body := &errorResponseBody{Reader: strings.NewReader(response)}
		bodies = append(bodies, body)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       body,
		}, nil
	}, OptLogRequests(true))
	result, err := client.ReloadSearchAnalyzers(ctx, "products")
	want := ReloadSearchAnalyzersResult{
		Shards: ShardsInfo{Total: 2, Successful: 2},
		ReloadDetails: []ReloadDetail{
			{
				Index:             "products",
				ReloadedAnalyzers: []string{"search_synonyms"},
				ReloadedNodeIDs:   []string{"node-a", "node-b"},
			},
		},
		CacheShards: &ShardsInfo{Total: 3, Successful: 2},
	}
	if err != nil || !reflect.DeepEqual(result, want) || len(bodies) != 2 || bodies[1].closed != 1 {
		t.Fatalf("result=%+v err=%v bodies=%v", result, err, bodies)
	}
	logit.InfoDuration(ctx, "done")
	records := readLogRecords(t, buf)
	if len(records) != 3 {
		t.Fatalf("请求日志或耗时重复: %v", records)
	}
	for i, operation := range []string{"reload_search_analyzers", "clear_request_cache"} {
		details := records[i]["downstream_details"].(map[string]any)
		if records[i]["level"] != "INFO" || details["operation"] != operation || details["error_type"] != "" {
			t.Fatalf("请求结果日志错误: %v", records[i])
		}
	}
	if records[2]["elasticsearch_catalog_1_duration_ms"] == nil || records[2]["elasticsearch_catalog_2_duration_ms"] == nil || records[2]["elasticsearch_catalog_3_duration_ms"] != nil {
		t.Fatalf("两次请求的耗时未分别记录: %v", records[2])
	}
}

func TestReloadSearchAnalyzersPreservesCompletedStages(t *testing.T) {
	readErr := errors.New("read failed")
	closeErr := errors.New("close failed")
	for _, stage := range []string{"reload", "cache"} {
		for _, failure := range []string{"partial", "http", "read", "close", "partial_and_close", "transport", "cancel"} {
			t.Run(stage+"/"+failure, func(t *testing.T) {
				buf := captureElasticsearchLogs(t)
				calls := 0
				var failedBody *errorResponseBody
				client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
					calls++
					if stage == "cache" && calls == 1 {
						return jsonResponse(req, 200, reloadSearchAnalyzersResponse), nil
					}
					if failure == "transport" {
						return nil, io.EOF
					}
					if failure == "cancel" {
						return nil, context.Canceled
					}
					status, response := 200, reloadSearchAnalyzersResponse
					if stage == "cache" {
						response = clearRequestCacheResponse
					}
					switch failure {
					case "partial", "partial_and_close":
						response = strings.Replace(response, `"successful":2,"failed":0`, `"successful":1,"failed":1,"failures":[{"reason":"secret-rule"}]`, 1)
					case "http":
						status, response = 404, `{"error":{"type":"index_not_found_exception","reason":"secret-rule"}}`
					}
					var reader io.Reader = strings.NewReader(response)
					if failure == "read" {
						reader = &finalReadErrorBody{data: response, err: readErr}
					}
					failedBody = &errorResponseBody{Reader: reader}
					if failure == "close" || failure == "partial_and_close" {
						failedBody.closeErr = closeErr
					}
					return &http.Response{
						StatusCode: status,
						Body:       failedBody,
					}, nil
				}, OptLogRequests(true))
				result, err := client.ReloadSearchAnalyzers(context.Background(), "products")
				wantCalls := 1
				if stage == "cache" {
					wantCalls = 2
				}
				if err == nil || calls != wantCalls || (failedBody != nil && failedBody.closed != 1) {
					t.Fatalf("err=%v calls=%d body=%+v", err, calls, failedBody)
				}
				complete := failure == "partial" || failure == "close" || failure == "partial_and_close"
				if (result.ReloadDetails != nil) != (stage == "cache" || complete) || (result.CacheShards != nil) != (stage == "cache" && complete) {
					t.Fatalf("丢失已解析结果或保留了未验证结果: %+v", result)
				}
				if failure == "partial" || failure == "partial_and_close" {
					shards := result.Shards
					if stage == "cache" {
						shards = *result.CacheShards
					}
					if shards.Failed != 1 || !strings.Contains(string(shards.Failures), "secret-rule") || !strings.Contains(err.Error(), "failed_shards=1") {
						t.Fatalf("丢失失败状态: result=%+v err=%v", result, err)
					}
				}
				if (failure == "close" || failure == "partial_and_close") && !errors.Is(err, closeErr) {
					t.Fatalf("丢失关闭错误: %v", err)
				}
				if failure == "read" && !errors.Is(err, readErr) || failure == "transport" && !errors.Is(err, io.EOF) || failure == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("丢失底层错误: %v", err)
				}
				if failure == "http" {
					var httpErr *HTTPError
					if !errors.As(err, &httpErr) || httpErr.Status != 404 || httpErr.Reason != "secret-rule" || errors.Is(err, ErrNotFound) {
						t.Fatalf("错误 HTTP 语义: %v", err)
					}
				}
				records := readLogRecords(t, buf)
				level := "ERROR"
				if failure == "http" {
					level = "WARN"
				}
				if len(records) != wantCalls || records[len(records)-1]["level"] != level || strings.Contains(buf.String(), "secret-rule") || strings.Contains(err.Error(), "secret-rule") {
					t.Fatalf("失败日志错误或泄露规则: %s err=%v", buf.String(), err)
				}
			})
		}
	}
}

func TestReloadSearchAnalyzersRejectsInvalidResponses(t *testing.T) {
	for _, stage := range []string{"reload", "cache"} {
		responses := []string{
			`{`,
			`{}`,
			`{"_shards":null}`,
			`{"_shards":{"total":2,"successful":2}}`,
			`{"_shards":{"total":2,"successful":null,"failed":0}}`,
			`{"_shards":{"total":-1,"successful":0,"failed":0}}`,
			`{"_shards":{"total":2,"successful":3,"failed":0}}`,
			`{"_shards":{"total":2,"successful":2,"failed":1}}`,
		}
		if stage == "reload" {
			for _, details := range []string{"", `,"reload_details":null`, `,"reload_details":{}`, `,"reload_details":[null]`, `,"reload_details":[{"index":"products"}]`} {
				responses = append(responses, `{"_shards":{"total":2,"successful":2,"failed":0}`+details+`}`)
			}
		}
		for _, response := range responses {
			t.Run(stage+"/"+response, func(t *testing.T) {
				calls := 0
				client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
					calls++
					if stage == "cache" && calls == 1 {
						return jsonResponse(req, 200, reloadSearchAnalyzersResponse), nil
					}
					return jsonResponse(req, 200, response), nil
				})
				result, err := client.ReloadSearchAnalyzers(context.Background(), "products")
				if err == nil || result.CacheShards != nil || (result.ReloadDetails != nil) != (stage == "cache") {
					t.Fatalf("接受无效响应或丢失此前结果: result=%+v err=%v", result, err)
				}
				if stage == "reload" && calls != 1 || stage == "cache" && calls != 2 {
					t.Fatalf("失败后仍继续请求: %d", calls)
				}
			})
		}
	}
}

func TestReloadSearchAnalyzersAllowsEmptyDetailsAndTrailingContent(t *testing.T) {
	for _, details := range []string{`[]`, `[{"index":"products","reloaded_analyzers":[],"reloaded_node_ids":[]}]`} {
		t.Run(details, func(t *testing.T) {
			calls := 0
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				calls++
				response := `{"_shards":{"total":0,"successful":0,"failed":0},"reload_details":` + details + `} trailing`
				if calls == 2 {
					response = clearRequestCacheResponse + ` trailing`
				}
				return jsonResponse(req, 200, response), nil
			})
			if _, err := client.ReloadSearchAnalyzers(context.Background(), "products"); err != nil || calls != 2 {
				t.Fatalf("拒绝合法空结果或检查了尾部: err=%v calls=%d", err, calls)
			}
		})
	}
}

func TestReloadSearchAnalyzersRejectsInvalidInputs(t *testing.T) {
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		t.Fatalf("非法参数发出了请求: %s", req.URL)
		return nil, nil
	})
	for _, index := range []string{"", " ", "*", "_all", "a,b", "a/b", "a?", "UPPER", "<logs-{now/d}>", strings.Repeat("a", 256)} {
		if _, err := client.ReloadSearchAnalyzers(context.Background(), index); err == nil {
			t.Errorf("接受无效索引 %q", index)
		}
	}
	if _, err := client.ReloadSearchAnalyzers(nil, "products"); err == nil {
		t.Fatal("接受 nil context")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReloadSearchAnalyzers(context.Background(), "products"); !errors.Is(err, errClientClosed) {
		t.Fatalf("关闭后仍接受请求: %v", err)
	}
}
