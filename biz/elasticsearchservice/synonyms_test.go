package elasticsearchservice_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/elasticsearchservice"
	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	v7 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v7"
	v8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	v9 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v9"
)

func newHTTPService(t *testing.T, major int, handler http.HandlerFunc) *elasticsearchservice.Service {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/" {
			fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, major)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	open := map[int]func(elasticsearchx.Config, ...elasticsearchx.Option) (*elasticsearchx.Client, error){7: v7.New, 8: v8.New, 9: v9.New}[major]
	client, err := open(elasticsearchx.Config{
		Name:      "synonym-test",
		Addresses: []string{server.URL},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	svc, err := elasticsearchservice.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestValidationUsesStrictIndexAcrossVersions(t *testing.T) {
	for _, major := range []int{7, 8, 9} {
		t.Run(fmt.Sprint(major), func(t *testing.T) {
			var paths []string
			var definition map[string]any
			svc := newHTTPService(t, major, func(w http.ResponseWriter, r *http.Request) {
				paths = append(paths, r.Method+" "+r.URL.Path)
				switch r.Method {
				case http.MethodPut:
					if err := json.NewDecoder(r.Body).Decode(&definition); err != nil {
						t.Error(err)
					}
					fmt.Fprintf(w, `{"acknowledged":true,"shards_acknowledged":true,"index":%q}`, strings.TrimPrefix(r.URL.Path, "/"))
				case http.MethodDelete:
					fmt.Fprint(w, `{"acknowledged":true}`)
				default:
					t.Errorf("意外方法: %s", r.Method)
				}
			})
			got, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "analysis/synonyms.txt"})
			if err != nil || !got.Valid || got.Stage != elasticsearchservice.StageComplete || got.Cleanup != elasticsearchservice.CleanupSucceeded || got.MayExist {
				t.Fatalf("result=%+v err=%v", got, err)
			}
			if !strings.HasPrefix(got.Index, "pixiu-synonym-validation-") || len(got.Index) != len("pixiu-synonym-validation-")+32 {
				t.Fatalf("错误临时名称: %q", got.Index)
			}
			wantPaths := []string{"PUT /" + got.Index, "DELETE /" + got.Index}
			if !reflect.DeepEqual(paths, wantPaths) {
				t.Fatalf("请求次序或路径错误: %v", paths)
			}
			settings := definition["settings"].(map[string]any)
			if settings["number_of_shards"] != float64(1) || settings["number_of_replicas"] != float64(0) || settings["index.write.wait_for_active_shards"] != "1" {
				t.Fatalf("错误分片配置: %v", settings)
			}
			analysis := settings["analysis"].(map[string]any)
			filter := analysis["filter"].(map[string]any)["pixiu_validation_synonyms"].(map[string]any)
			wantFilter := map[string]any{"type": "synonym_graph", "synonyms_path": "analysis/synonyms.txt", "lenient": false, "updateable": false, "expand": true, "format": "solr"}
			if !reflect.DeepEqual(filter, wantFilter) {
				t.Fatalf("严格配置不匹配: %v", filter)
			}
			analyzers := analysis["analyzer"].(map[string]any)
			search := analyzers["pixiu_validation_search"].(map[string]any)
			index := analyzers["pixiu_validation_index"].(map[string]any)
			wantSearch := map[string]any{
				"type":      "custom",
				"tokenizer": "standard",
				"filter":    []any{"lowercase", "pixiu_validation_synonyms"},
			}
			if !reflect.DeepEqual(search, wantSearch) || index["type"] != "standard" || len(index) != 1 {
				t.Fatalf("错误分析链: %v", analyzers)
			}
			mappings := definition["mappings"].(map[string]any)
			queryText, ok := mappings["properties"].(map[string]any)["query_text"].(map[string]any)
			if !ok {
				t.Fatalf("缺少 query_text 字段: %v", mappings)
			}
			if mappings["dynamic"] != "strict" || queryText["type"] != "text" || queryText["analyzer"] != "pixiu_validation_index" || queryText["search_analyzer"] != "pixiu_validation_search" {
				t.Fatalf("字段未绑定分析器: %v", mappings)
			}
		})
	}
}

func TestValidationFailureStates(t *testing.T) {
	for _, tt := range []struct {
		name       string
		createCode int
		createBody string
		deleteCode int
		deleteBody string
		stage      elasticsearchservice.ValidationStage
		cleanup    elasticsearchservice.CleanupStatus
		valid      bool
		mayExist   bool
		wantErr    bool
		wantCalls  string
	}{
		{
			name:       "非法同义词规则",
			createCode: 400,
			createBody: `{"error":{"type":"illegal_argument_exception","caused_by":{"type":"parse_exception","reason":"Invalid synonym rule at line 3"}}}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "词库文件缺失",
			createCode: 400,
			createBody: `{"error":{"type":"illegal_argument_exception","caused_by":{"type":"file_not_found_exception","reason":"missing file"}}}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "名称冲突不删除已有索引",
			createCode: 400,
			createBody: `{"error":{"type":"resource_already_exists_exception"}}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "权限错误",
			createCode: 403,
			createBody: `{"error":{"type":"security_exception"}}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "创建未确认",
			createBody: `{"acknowledged":false,"shards_acknowledged":false,"index":"$index"}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "创建畸形响应",
			createBody: `{"acknowledged":true}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupNotAttempted,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT",
		},
		{
			name:       "分片启动超时仍清理",
			createBody: `{"acknowledged":true,"shards_acknowledged":false,"index":"$index"}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupSucceeded,
			wantErr:    true,
			wantCalls:  "PUT,DELETE",
		},
		{
			name:       "验证成功但清理失败",
			deleteCode: 403,
			deleteBody: `{"error":{"type":"security_exception"}}`,
			stage:      elasticsearchservice.StageComplete,
			cleanup:    elasticsearchservice.CleanupFailed,
			valid:      true,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT,DELETE",
		},
		{
			name:       "删除未确认",
			deleteBody: `{"acknowledged":false}`,
			stage:      elasticsearchservice.StageComplete,
			cleanup:    elasticsearchservice.CleanupFailed,
			valid:      true,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT,DELETE",
		},
		{
			name:       "索引已被删除",
			deleteCode: 404,
			deleteBody: `{"error":{"type":"index_not_found_exception"}}`,
			stage:      elasticsearchservice.StageComplete,
			cleanup:    elasticsearchservice.CleanupSucceeded,
			valid:      true,
			wantCalls:  "PUT,DELETE",
		},
		{
			name:       "非索引缺失的404",
			deleteCode: 404,
			deleteBody: `{"error":{"type":"other_error"}}`,
			stage:      elasticsearchservice.StageComplete,
			cleanup:    elasticsearchservice.CleanupFailed,
			valid:      true,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT,DELETE",
		},
		{
			name:       "验证和清理均失败",
			createBody: `{"acknowledged":true,"shards_acknowledged":false,"index":"$index"}`,
			deleteCode: 500,
			deleteBody: `{"error":{"type":"delete_error"}}`,
			stage:      elasticsearchservice.StageCreate,
			cleanup:    elasticsearchservice.CleanupFailed,
			mayExist:   true,
			wantErr:    true,
			wantCalls:  "PUT,DELETE",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls []string
			svc := newHTTPService(t, 8, func(w http.ResponseWriter, r *http.Request) {
				calls = append(calls, r.Method)
				code, body := tt.createCode, tt.createBody
				switch r.Method {
				case http.MethodPut:
					if body == "" {
						body = `{"acknowledged":true,"shards_acknowledged":true,"index":"$index"}`
					}
					body = strings.ReplaceAll(body, "$index", strings.TrimPrefix(r.URL.Path, "/"))
				case http.MethodDelete:
					code, body = tt.deleteCode, tt.deleteBody
					if body == "" {
						body = `{"acknowledged":true}`
					}
				default:
					t.Errorf("意外方法: %s", r.Method)
					http.Error(w, "unexpected request", http.StatusBadRequest)
					return
				}
				if code == 0 {
					code = 200
				}
				w.WriteHeader(code)
				fmt.Fprint(w, body)
			})
			got, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "analysis/test.txt"})
			if (err != nil) != tt.wantErr || got.Stage != tt.stage || got.Cleanup != tt.cleanup || got.Valid != tt.valid || got.MayExist != tt.mayExist || strings.Join(calls, ",") != tt.wantCalls {
				t.Fatalf("result=%+v err=%v calls=%v", got, err, calls)
			}
			if tt.name == "非法同义词规则" {
				var httpErr *elasticsearchx.HTTPError
				if !errors.As(err, &httpErr) || httpErr.CausedBy == nil || httpErr.CausedBy.Reason != "Invalid synonym rule at line 3" {
					t.Fatalf("原始行号诊断丢失: %v", err)
				}
			}
			if tt.name == "验证和清理均失败" && (!strings.Contains(err.Error(), "shards not ready") || !strings.Contains(err.Error(), "delete_error")) {
				t.Fatalf("错误链未保留两个失败: %v", err)
			}
		})
	}
}

type performFunc func(*http.Request) (*http.Response, error)

func (f performFunc) Perform(r *http.Request) (*http.Response, error) { return f(r) }

func response(body string) *http.Response {
	return &http.Response{
		StatusCode: 200,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func newDirectService(t *testing.T, perform performFunc) *elasticsearchservice.Service {
	t.Helper()
	cfg := elasticsearchx.Config{
		Name:      "direct",
		Addresses: []string{"http://localhost:9200"},
	}
	transport, err := elasticsearchx.NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := elasticsearchx.Attach(cfg, 8, performFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/" {
			return response(`{"version":{"number":"8.0.0"}}`), nil
		}
		return perform(r)
	}), nil, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	svc, err := elasticsearchservice.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return svc
}

func TestValidationCancellationAndBoundedCleanup(t *testing.T) {
	type contextKey struct{}
	for _, cleanupTimeout := range []bool{false, true} {
		t.Run(fmt.Sprint(cleanupTimeout), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), contextKey{}, "request"))
			defer cancel()
			deleted := false
			svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
				switch r.Method {
				case http.MethodPut:
					cancel()
					return response(fmt.Sprintf(`{"acknowledged":true,"shards_acknowledged":true,"index":%q}`, strings.TrimPrefix(r.URL.Path, "/"))), nil
				case http.MethodDelete:
					deleted = true
					deadline, ok := r.Context().Deadline()
					if r.Context().Err() != nil || r.Context().Value(contextKey{}) != "request" || !ok || time.Until(deadline) > time.Second {
						t.Fatal("清理未脱离取消、丢失作用域或未设置期限")
					}
					if cleanupTimeout {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					return response(`{"acknowledged":true}`), nil
				}
				t.Fatalf("意外请求 %s", r.Method)
				return nil, nil
			})
			got, err := svc.ValidateSynonyms(ctx, elasticsearchservice.ValidationOptions{
				SynonymsPath:   "analysis/test.txt",
				CleanupTimeout: 30 * time.Millisecond,
			})
			if !deleted || ctx.Err() != context.Canceled || (err != nil) != cleanupTimeout || !got.Valid || got.Stage != elasticsearchservice.StageComplete || got.MayExist != cleanupTimeout {
				t.Fatalf("取消与清理结果错误: %+v %v", got, err)
			}
			if cleanupTimeout && (!errors.Is(err, context.DeadlineExceeded) || got.Cleanup != elasticsearchservice.CleanupFailed) {
				t.Fatalf("清理超时丢失: %+v %v", got, err)
			}
		})
	}
}

func TestValidationRejectsLocalErrorsWithoutCreatingIndex(t *testing.T) {
	if _, err := elasticsearchservice.New(nil); err == nil {
		t.Fatal("接受 nil client")
	}
	svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
		t.Fatalf("本地错误仍发出请求: %s", r.URL)
		return nil, nil
	})
	for _, options := range []elasticsearchservice.ValidationOptions{
		{},
		{SynonymsPath: "  "},
		{SynonymsPath: "test\x00"},
		{
			SynonymsPath:   "test",
			CleanupTimeout: -time.Second,
		},
	} {
		got, err := svc.ValidateSynonyms(context.Background(), options)
		if err == nil || got.MayExist || got.Valid || got.Stage != elasticsearchservice.StagePrepare || got.Cleanup != elasticsearchservice.CleanupNotAttempted {
			t.Fatalf("本地失败状态错误: %+v %v", got, err)
		}
	}
	if _, err := svc.ValidateSynonyms(nil, elasticsearchservice.ValidationOptions{SynonymsPath: "test"}); err == nil {
		t.Fatal("接受 nil context")
	}
}

func TestValidationDelegatesCanceledContextToClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodPut || r.Context().Err() != context.Canceled {
			t.Fatalf("取消未交给创建请求处理: method=%s err=%v", r.Method, r.Context().Err())
		}
		return nil, r.Context().Err()
	})
	got, err := svc.ValidateSynonyms(ctx, elasticsearchservice.ValidationOptions{SynonymsPath: "test"})
	if calls != 1 || !errors.Is(err, context.Canceled) || got.Index == "" || !got.MayExist || got.Valid || got.Stage != elasticsearchservice.StageCreate || got.Cleanup != elasticsearchservice.CleanupNotAttempted {
		t.Fatalf("已取消请求状态错误: %+v %v calls=%d", got, err, calls)
	}
}

type closeErrorBody struct {
	io.Reader
	err error
}

func (b *closeErrorBody) Close() error { return b.err }

func TestCreatedIndexIsCleanedAfterResponseCloseFailure(t *testing.T) {
	closeErr := errors.New("create response close failed")
	deleted := false
	svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodPut:
			return &http.Response{
				StatusCode: 200,
				Body: &closeErrorBody{
					Reader: strings.NewReader(fmt.Sprintf(`{"acknowledged":true,"shards_acknowledged":true,"index":%q}`, strings.TrimPrefix(r.URL.Path, "/"))),
					err:    closeErr,
				},
			}, nil
		case http.MethodDelete:
			deleted = true
			return response(`{"acknowledged":true}`), nil
		}
		t.Fatalf("意外请求: %s", r.Method)
		return nil, nil
	})
	got, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "test"})
	if !deleted || !errors.Is(err, closeErr) || got.Valid || got.MayExist || got.Cleanup != elasticsearchservice.CleanupSucceeded || got.Stage != elasticsearchservice.StageCreate {
		t.Fatalf("关闭失败丢失已创建资源: %+v %v", got, err)
	}
}

func TestUnknownCreationDoesNotDeleteOrRetry(t *testing.T) {
	networkErr := errors.New("connection lost")
	calls := 0
	svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Method != http.MethodPut {
			t.Fatalf("创建未知时尝试删除: %s", r.Method)
		}
		return nil, networkErr
	})
	got, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "test"})
	if calls != 1 || !errors.Is(err, networkErr) || got.Index == "" || !got.MayExist || got.Cleanup != elasticsearchservice.CleanupNotAttempted {
		t.Fatalf("创建未知状态丢失: %+v %v calls=%d", got, err, calls)
	}
}

func TestDeleteMissingIndexDoesNotHideResponseCloseFailure(t *testing.T) {
	closeErr := errors.New("delete response close failed")
	svc := newDirectService(t, func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodPut:
			return response(fmt.Sprintf(`{"acknowledged":true,"shards_acknowledged":true,"index":%q}`, strings.TrimPrefix(r.URL.Path, "/"))), nil
		case http.MethodDelete:
			return &http.Response{
				StatusCode: 404,
				Body: &closeErrorBody{
					Reader: strings.NewReader(`{"error":{"type":"index_not_found_exception"}}`),
					err:    closeErr,
				},
			}, nil
		}
		t.Fatalf("意外请求: %s", r.Method)
		return nil, nil
	})
	got, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "test"})
	if !got.Valid || !errors.Is(err, closeErr) || got.Cleanup != elasticsearchservice.CleanupFailed || !got.MayExist {
		t.Fatalf("索引404掩盖关闭失败: %+v %v", got, err)
	}
}

func TestConcurrentValidationKeepsIndependentIndices(t *testing.T) {
	var mu sync.Mutex
	created, deleted := map[string]bool{}, map[string]bool{}
	svc := newHTTPService(t, 8, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		index := strings.TrimPrefix(r.URL.Path, "/")
		switch r.Method {
		case http.MethodPut:
			if created[index] {
				t.Error("并发调用共用临时索引")
			}
			created[index] = true
			fmt.Fprintf(w, `{"acknowledged":true,"shards_acknowledged":true,"index":%q}`, index)
		case http.MethodDelete:
			if !created[index] || deleted[index] {
				t.Errorf("删除非本次索引或重复删除: %s", index)
			}
			deleted[index] = true
			fmt.Fprint(w, `{"acknowledged":true}`)
		}
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if _, err := svc.ValidateSynonyms(context.Background(), elasticsearchservice.ValidationOptions{SynonymsPath: "analysis/test.txt"}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if len(created) != 8 || len(deleted) != 8 {
		t.Fatalf("未完整清理: created=%v deleted=%v", created, deleted)
	}
}
