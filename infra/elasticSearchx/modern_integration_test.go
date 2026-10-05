package elasticSearchx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticSearchx"
	elasticSearchxv7 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v7"
	elasticSearchxv8 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v8"
	elasticSearchxv9 "github.com/bpcoder16/pixiu/infra/elasticSearchx/v9"
	"github.com/bpcoder16/pixiu/logit"
)

var modernVersionFactories = []struct {
	major int
	open  func(elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
}{
	{8, elasticSearchxv8.New},
	{9, elasticSearchxv9.New},
}

func TestModernVersionFactoriesDoNotRetryEOF(t *testing.T) {
	for _, factory := range modernVersionFactories {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			for _, tt := range []struct {
				name    string
				method  string
				path    string
				body    string
				startup bool
			}{
				{
					name:    "启动验活",
					method:  http.MethodGet,
					path:    "/",
					startup: true,
				},
				{
					name:   "Get",
					method: http.MethodGet,
					path:   "/products/_doc/1",
				},
				{
					name:   "Count",
					method: http.MethodPost,
					path:   "/products/_count",
					body:   `{"value":1}`,
				},
				{
					name:   "Index",
					method: http.MethodPut,
					path:   "/products/_doc/1",
					body:   `{"value":1}`,
				},
				{
					name:   "Bulk",
					method: http.MethodPost,
					path:   "/products/_bulk",
					body:   "{\"index\":{\"_id\":\"1\"}}\n{\"value\":1}\n",
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						if req.URL.Path == "/" && !tt.startup {
							w.Header().Set("X-Elastic-Product", "Elasticsearch")
							// 使用新连接触发 EOF，排除标准库对失效复用连接的安全重发。
							w.Header().Set("Connection", "close")
							fmt.Fprintf(w, `{"version":{"number":"%d.0.0"}}`, factory.major)
							return
						}
						calls.Add(1)
						body, err := io.ReadAll(req.Body)
						if err != nil || req.Method != tt.method || req.URL.Path != tt.path || string(body) != tt.body {
							t.Errorf("断连前的请求不一致: method=%s path=%s body=%q err=%v", req.Method, req.URL.Path, body, err)
						}
						// 完整收到请求后不返回响应，模拟写入可能已生效的断连。
						conn, _, err := w.(http.Hijacker).Hijack()
						if err != nil {
							t.Errorf("接管连接: %v", err)
							return
						}
						if err := conn.Close(); err != nil {
							t.Errorf("关闭连接: %v", err)
						}
					}))
					defer server.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					client, err := factory.open(elasticSearchx.Config{
						Name:      "search",
						Addresses: []string{server.URL},
					})
					if client != nil {
						defer client.Close()
					}
					if tt.startup {
						if client != nil {
							t.Fatal("启动断连仍返回了客户端")
						}
					} else {
						if err != nil {
							t.Fatalf("创建客户端: %v", err)
						}
						switch tt.name {
						case "Get":
							_, err = client.Get(ctx, "products", "1")
						case "Count":
							_, err = client.Count(ctx, "products", map[string]any{"value": 1})
						case "Index":
							err = client.Index(ctx, "products", "1", map[string]any{"value": 1})
						case "Bulk":
							_, err = client.Bulk(ctx, "products", []elasticSearchx.BulkAction{
								elasticSearchx.NewBulkIndex("1", map[string]any{"value": 1}),
							})
						}
					}
					if !errors.Is(err, io.EOF) || calls.Load() != 1 {
						t.Fatalf("EOF 被改变或发生重试: err=%v calls=%d", err, calls.Load())
					}
				})
			}
		})
	}
}

func TestVersionFactoriesBulkResultsAndSingleLog(t *testing.T) {
	// SDK 的兼容模式会改写媒体类型；这里验证普通 NDJSON 请求路径。
	t.Setenv("ELASTIC_CLIENT_APIVERSIONING", "false")
	var buf bytes.Buffer
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&buf)))
	oldLogger := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(oldLogger)
		_ = logit.Close(logger)
	})
	const requestBody = "{\"index\":{\"_id\":\"1\"}}\n{\"value\":1}\n" +
		"{\"update\":{\"_id\":\"2\"}}\n{\"doc\":{\"value\":2},\"doc_as_upsert\":true}\n" +
		"{\"delete\":{\"_id\":\"3\"}}\n" +
		"{\"create\":{\"_id\":\"4\"}}\n{\"value\":4}\n" +
		"{\"update\":{\"_id\":\"5\"}}\n{\"doc\":{\"value\":5}}\n"
	factories := []struct {
		major int
		open  func(elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
	}{
		{7, elasticSearchxv7.New},
		{8, elasticSearchxv8.New},
		{9, elasticSearchxv9.New},
	}
	for _, factory := range factories {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			for _, tt := range []struct {
				name         string
				response     string
				succeeded    int
				failures     []elasticSearchx.BulkFailure
				failedShards int
				level        string
			}{
				{
					name:      "全部成功",
					response:  `{"errors":false,"items":[{"index":{"_id":"1","status":201}},{"update":{"_id":"2","status":200}},{"delete":{"_id":"3","status":404,"result":"not_found"}},{"create":{"_id":"4","status":201}},{"update":{"_id":"5","status":200}}]}`,
					succeeded: 5,
					level:     "INFO",
				},
				{
					name:      "部分失败",
					response:  `{"errors":true,"items":[{"index":{"_id":"1","status":201}},{"update":{"_id":"2","status":429,"error":{"type":"rejected","reason":"busy"}}},{"delete":{"_id":"3","status":404,"result":"not_found"}},{"create":{"_id":"4","status":201}},{"update":{"_id":"5","status":200}}]}`,
					succeeded: 4,
					failures: []elasticSearchx.BulkFailure{
						{
							Position: 1,
							ID:       "2",
							Status:   429,
							Type:     "rejected",
							Reason:   "busy",
						},
					},
					level: "ERROR",
				},
				{
					name:      "重复创建及更新缺失文档",
					response:  `{"errors":true,"items":[{"index":{"_id":"1","status":201}},{"update":{"_id":"2","status":200}},{"delete":{"_id":"3","status":404,"result":"not_found"}},{"create":{"_id":"4","status":409,"error":{"type":"version_conflict_engine_exception","reason":"already exists"}}},{"update":{"_id":"5","status":404,"error":{"type":"document_missing_exception","reason":"missing"}}}]}`,
					succeeded: 3,
					failures: []elasticSearchx.BulkFailure{
						{
							Position: 3,
							ID:       "4",
							Status:   409,
							Type:     "version_conflict_engine_exception",
							Reason:   "already exists",
						},
						{
							Position: 4,
							ID:       "5",
							Status:   404,
							Type:     "document_missing_exception",
							Reason:   "missing",
						},
					},
					level: "ERROR",
				},
				{
					name:         "仅副本失败",
					response:     `{"errors":false,"items":[{"index":{"_id":"1","status":201,"_shards":{"total":2,"successful":1,"failed":1}}},{"update":{"_id":"2","status":200}},{"delete":{"_id":"3","status":404}},{"create":{"_id":"4","status":201}},{"update":{"_id":"5","status":200}}]}`,
					succeeded:    5,
					failedShards: 1,
					level:        "WARN",
				},
				{
					name:      "分片失败与动作失败并存",
					response:  `{"errors":true,"items":[{"index":{"_id":"1","status":201,"_shards":{"total":2,"successful":1,"failed":1}}},{"update":{"_id":"2","status":200}},{"delete":{"_id":"3","status":404}},{"create":{"_id":"4","status":409,"error":{"type":"version_conflict_engine_exception","reason":"already exists"}}},{"update":{"_id":"5","status":200}}]}`,
					succeeded: 4,
					failures: []elasticSearchx.BulkFailure{
						{
							Position: 3,
							ID:       "4",
							Status:   409,
							Type:     "version_conflict_engine_exception",
							Reason:   "already exists",
						},
					},
					failedShards: 1,
					level:        "ERROR",
				},
			} {
				t.Run(tt.name, func(t *testing.T) {
					var calls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						w.Header().Set("X-Elastic-Product", "Elasticsearch")
						w.Header().Set("Content-Type", "application/json")
						if req.URL.Path == "/" {
							fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
							return
						}
						calls.Add(1)
						body, err := io.ReadAll(req.Body)
						if err != nil || req.Method != http.MethodPost || req.URL.Path != "/products/_bulk" || req.Header.Get("Content-Type") != "application/x-ndjson" || string(body) != requestBody {
							t.Errorf("Bulk 请求不一致: method=%s path=%s type=%s body=%q err=%v", req.Method, req.URL.Path, req.Header.Get("Content-Type"), body, err)
						}
						io.WriteString(w, tt.response)
					}))
					defer server.Close()
					client, err := factory.open(elasticSearchx.Config{
						Name:          "catalog",
						Addresses:     []string{server.URL},
						SlowThreshold: time.Hour,
					}, elasticSearchx.OptLogRequests(true), elasticSearchx.OptLogDetails(true))
					if err != nil {
						t.Fatal(err)
					}
					defer client.Close()
					buf.Reset()
					ctx := logit.WithStart(context.Background())
					result, err := client.Bulk(ctx, "products", []elasticSearchx.BulkAction{
						elasticSearchx.NewBulkIndex("1", map[string]any{"value": 1}),
						elasticSearchx.NewBulkUpsert("2", map[string]any{"value": 2}),
						elasticSearchx.NewBulkDelete("3"),
						elasticSearchx.NewBulkCreate("4", map[string]any{"value": 4}),
						elasticSearchx.NewBulkUpdate("5", map[string]any{"value": 5}),
					})
					if result.Succeeded != tt.succeeded || len(result.Failures) != len(tt.failures) || result.FailedShards != tt.failedShards || calls.Load() != 1 {
						t.Fatalf("Bulk 结果或请求次数错误: result=%+v calls=%d", result, calls.Load())
					}
					if len(tt.failures) > 0 {
						var bulkErr *elasticSearchx.BulkError
						if !errors.As(err, &bulkErr) || bulkErr.Failed != len(tt.failures) {
							t.Fatalf("逐项失败未返回 BulkError: err=%v", err)
						}
						for i, want := range tt.failures {
							if result.Failures[i] != want {
								t.Fatalf("失败详情或顺序错误: failures=%+v", result.Failures)
							}
						}
					} else if err != nil {
						t.Fatalf("全部成功仍返回错误: %v", err)
					}
					var record map[string]any
					decoder := json.NewDecoder(&buf)
					if err := decoder.Decode(&record); err != nil {
						t.Fatal(err)
					}
					var extra map[string]any
					if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
						t.Fatalf("Bulk 日志不是一条: extra=%v err=%v", extra, err)
					}
					details := record[logit.DownstreamDetailsKey].(map[string]any)
					if record["level"] != tt.level || record[logit.DownstreamIDKey] != "catalog" || details["operation"] != "bulk" || details["succeeded"] != float64(tt.succeeded) || details["failed"] != float64(len(tt.failures)) || details["request_body"] != requestBody || details["response_body"] != tt.response {
						t.Fatalf("Bulk 日志结果或详情错误: %v", record)
					}
					wantErrorType := ""
					if len(tt.failures) > 0 {
						wantErrorType = "bulk_error"
					}
					if details["error_type"] != wantErrorType {
						t.Fatalf("Bulk 动作失败的日志分类错误: %v", record)
					}
					if tt.failedShards > 0 {
						if details["failed_shards"] != float64(tt.failedShards) {
							t.Fatalf("副本异常观测信息丢失: %v", details)
						}
					} else if _, ok := details["failed_shards"]; ok {
						t.Fatalf("无副本异常时多余的观测信息: %v", details)
					}
					buf.Reset()
					logit.InfoDuration(ctx, "done")
					var durationRecord map[string]any
					if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &durationRecord); err != nil {
						t.Fatal(err)
					}
					if durationRecord["elasticSearch_catalog_1_duration_ms"] == nil || durationRecord["elasticSearch_catalog_2_duration_ms"] != nil {
						t.Fatalf("Bulk 耗时丢失或重复: %v", durationRecord)
					}
				})
			}
		})
	}
}

func TestModernVersionFactoriesRejectStartupFailuresAndCloseConnections(t *testing.T) {
	for _, factory := range modernVersionFactories {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			for _, name := range []string{"主版本不符", "缺少产品头", "验活超时"} {
				t.Run(name, func(t *testing.T) {
					started := make(chan struct{}, 1)
					closed := make(chan struct{}, 1)
					var calls atomic.Int32
					releaseCtx, release := context.WithCancel(context.Background())
					server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						calls.Add(1)
						if name != "缺少产品头" {
							w.Header().Set("X-Elastic-Product", "Elasticsearch")
						}
						if name == "验活超时" {
							// 返回部分响应并保持连接，覆盖响应读取阶段的启动超时。
							io.WriteString(w, `{"version":{"number":"`)
							w.(http.Flusher).Flush()
							started <- struct{}{}
							select {
							case <-req.Context().Done():
							case <-releaseCtx.Done():
							}
							return
						}
						major := factory.major
						if name == "主版本不符" {
							major++
						}
						fmt.Fprintf(w, `{"version":{"number":"%d.0.0"}}`, major)
					}))
					server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
						if state == http.StateClosed {
							select {
							case closed <- struct{}{}:
							default:
							}
						}
					}
					server.Start()
					defer server.Close()
					// 失败时先解除 Handler 的等待，再关闭模拟服务。
					defer release()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					type startupResult struct {
						client *elasticSearchx.Client
						err    error
					}
					done := make(chan startupResult, 1)
					go func() {
						client, err := factory.open(elasticSearchx.Config{
							Name:           "search",
							Addresses:      []string{server.URL},
							StartupTimeout: 100 * time.Millisecond,
						})
						done <- startupResult{client: client, err: err}
					}()
					if name == "验活超时" {
						select {
						case <-started:
						case <-ctx.Done():
							t.Fatal("等待验活响应超时")
						}
					}
					var result startupResult
					select {
					case result = <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("初始化失败未及时返回")
					}
					if result.client != nil {
						_ = result.client.Close()
						t.Fatal("初始化失败仍返回了客户端")
					}
					if result.err == nil {
						t.Fatal("初始化失败未返回错误")
					}
					switch name {
					case "主版本不符":
						if !strings.Contains(result.err.Error(), "server major mismatch") {
							t.Fatalf("主版本不符错误丢失: %v", result.err)
						}
					case "缺少产品头":
						if !strings.Contains(result.err.Error(), "server is not Elasticsearch") {
							t.Fatalf("产品校验错误丢失: %v", result.err)
						}
					case "验活超时":
						if !errors.Is(result.err, context.DeadlineExceeded) {
							t.Fatalf("启动超时错误丢失: %v", result.err)
						}
					}
					select {
					case <-closed:
					case <-time.After(5 * time.Second):
						t.Fatal("初始化失败后连接未关闭")
					}
					if calls.Load() != 1 {
						t.Fatalf("初始化失败发生重试: calls=%d", calls.Load())
					}
				})
			}
		})
	}
}
