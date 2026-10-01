package elasticSearchx_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
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
	"github.com/elastic/go-elasticsearch/v7/esapi"
)

func TestVersionFactoriesConnectAndRunCommonOperations(t *testing.T) {
	oldLogger := logit.Default()
	logger := logit.MustNew(logit.OptWriter(logit.NewWriter(io.Discard)))
	logit.SetDefault(logger)
	t.Cleanup(func() { logit.SetDefault(oldLogger); _ = logit.Close(logger) })
	factories := []struct {
		major int
		open  func(context.Context, elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
	}{
		{7, elasticSearchxv7.New}, {8, elasticSearchxv8.New}, {9, elasticSearchxv9.New},
	}
	for _, factory := range factories {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			requests := 0
			searchCalls := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests++
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				if factory.major == 8 {
					if got := req.Header.Get("Authorization"); got != "APIKey dGVzdA==" {
						t.Errorf("API Key 未传递: %q", got)
					}
				} else if user, password, ok := req.BasicAuth(); !ok || user != "reader" || password != "secret" {
					t.Errorf("基本认证未传递: %q %q %v", user, password, ok)
				}
				switch req.URL.Path {
				case "/":
					fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
				case "/products/_count":
					fmt.Fprint(w, `{"count":4}`)
				case "/products/_search":
					searchCalls++
					w.WriteHeader(http.StatusServiceUnavailable)
					fmt.Fprint(w, `{"error":{"type":"unavailable"}}`)
				default:
					t.Errorf("意外请求: %s", req.URL.Path)
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
			cfg := elasticSearchx.Config{Name: "search", Addresses: []string{server.URL}, CACert: ca}
			if factory.major == 8 {
				cfg.APIKey = "dGVzdA=="
			} else {
				cfg.Username, cfg.Password = "reader", "secret"
			}
			client, err := factory.open(context.Background(), cfg)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			defer client.Close(context.Background())
			count, err := client.Count(context.Background(), "products", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
			if err != nil || count != 4 || requests < 2 {
				t.Fatalf("Count: count=%d err=%v requests=%d", count, err, requests)
			}
			if factory.major == 7 {
				response, err := (esapi.CountRequest{Index: []string{"products"}}).Do(context.Background(), client)
				if err != nil {
					t.Fatalf("esapi 经共用客户端调用: %v", err)
				}
				response.Body.Close()
			}
			if _, err := client.Search(context.Background(), "products", map[string]any{"query": map[string]any{"match_all": map[string]any{}}}); err == nil || searchCalls != 1 {
				t.Fatalf("默认禁用重试: err=%v calls=%d", err, searchCalls)
			}
		})
	}
}

func TestVersionFactoriesPassLogOptionsAndPreserveResponse(t *testing.T) {
	var buf bytes.Buffer
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&buf)))
	old := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(old)
		_ = logit.Close(logger)
	})
	for _, factory := range []struct {
		major int
		open  func(context.Context, elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
	}{
		{7, elasticSearchxv7.New},
		{8, elasticSearchxv8.New},
		{9, elasticSearchxv9.New},
	} {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				w.Header().Set("Content-Type", "application/json")
				if req.URL.Path == "/" {
					fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
					return
				}
				data, err := io.ReadAll(req.Body)
				if err != nil || string(data) != `{"query":"all"}` {
					t.Errorf("详情采集改变了发送的请求: body=%q err=%v", data, err)
				}
				fmt.Fprint(w, `{"count":4}`)
			}))
			defer server.Close()
			cfg := elasticSearchx.Config{
				Name:      "catalog",
				Addresses: []string{server.URL},
			}
			for _, logRequests := range []bool{true, false} {
				client, err := factory.open(context.Background(), cfg, elasticSearchx.OptLogRequests(logRequests), elasticSearchx.OptLogDetails(true))
				if err != nil {
					t.Fatal(err)
				}
				buf.Reset()
				ctx := logit.WithStart(context.Background())
				count, err := client.Count(ctx, "products", map[string]any{"query": "all"})
				if err != nil || count != 4 {
					t.Fatalf("详情采集改变了响应解析: count=%d err=%v", count, err)
				}
				if logRequests {
					var record map[string]any
					if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
						t.Fatal(err)
					}
					details := record[logit.DownstreamDetailsKey].(map[string]any)
					if record["level"] != "INFO" || record[logit.DownstreamTypeKey] != "elasticSearch" || details["request_body"] != `{"query":"all"}` || details["response_body"] != `{"count":4}` || details["response_proto"] != "HTTP/1.1" || details["response_status_text"] != "200 OK" {
						t.Fatalf("官方客户端详情日志错误: %v", record)
					}
				} else if buf.Len() != 0 {
					t.Fatalf("官方客户端未传递日志开关: %s", buf.String())
				}
				buf.Reset()
				logit.InfoDuration(ctx, "done")
				if !strings.Contains(buf.String(), `"elasticSearch_catalog_1_duration_ms"`) || strings.Contains(buf.String(), `"elasticSearch_catalog_2_duration_ms"`) {
					t.Fatalf("官方客户端耗时记录丢失或重复: %s", buf.String())
				}
				_ = client.Close(context.Background())
			}
		})
	}
}

func TestVersionFactoriesRespectConnectionLimit(t *testing.T) {
	factories := []struct {
		major int
		open  func(context.Context, elasticSearchx.Config, ...elasticSearchx.Option) (*elasticSearchx.Client, error)
	}{
		{7, elasticSearchxv7.New},
		{8, elasticSearchxv8.New},
		{9, elasticSearchxv9.New},
	}
	for _, factory := range factories {
		t.Run(fmt.Sprint(factory.major), func(t *testing.T) {
			started := make(chan struct{})
			releaseCtx, release := context.WithCancel(context.Background())
			var countCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.Header().Set("X-Elastic-Product", "Elasticsearch")
				switch req.URL.Path {
				case "/":
					fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, factory.major)
				case "/products/_count":
					if countCalls.Add(1) == 1 {
						close(started)
						// 保持首个响应未完成，确保唯一连接处于使用中。
						select {
						case <-releaseCtx.Done():
						case <-req.Context().Done():
							return
						}
					}
					fmt.Fprint(w, `{"count":1}`)
				default:
					http.NotFound(w, req)
				}
			}))
			defer server.Close()
			// 先释放阻塞的请求，再等待服务端关闭，避免失败路径挂起。
			defer release()
			client, err := factory.open(context.Background(), elasticSearchx.Config{
				Name:            "search",
				Addresses:       []string{server.URL},
				MaxConnsPerHost: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			firstDone := make(chan error, 1)
			go func() {
				_, err := client.Count(ctx, "products", map[string]any{"query": "all"})
				firstDone <- err
			}()
			select {
			case <-started:
			case err := <-firstDone:
				t.Fatalf("首个请求未到达服务端: %v", err)
			case <-ctx.Done():
				t.Fatal("等待首个请求超时")
			}
			waitingCtx, cancelWaiting := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancelWaiting()
			_, err = client.Count(waitingCtx, "products", map[string]any{"query": "all"})
			if !errors.Is(err, context.DeadlineExceeded) || countCalls.Load() != 1 {
				t.Fatalf("连接上限未限制等待请求: err=%v calls=%d", err, countCalls.Load())
			}
			release()
			if err := <-firstDone; err != nil {
				t.Fatalf("首个请求失败: %v", err)
			}
			if count, err := client.Count(ctx, "products", map[string]any{"query": "all"}); err != nil || count != 1 {
				t.Fatalf("释放连接后请求失败: count=%d err=%v", count, err)
			}
		})
	}
}
