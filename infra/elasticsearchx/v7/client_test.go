package v7

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
)

func TestNewDoesNotRetryEOF(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		closeWithoutResponse(t, w)
	}))
	defer server.Close()
	client, err := New(elasticsearchx.Config{
		Name:      "search",
		Addresses: []string{server.URL},
	}, elasticsearchx.OptLogRequests(false))
	if client != nil {
		_ = client.Close()
	}
	if client != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("启动检查应返回 EOF: client=%v err=%v", client, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("启动检查发生重试: 请求次数=%d，期望 1", got)
	}
}

func TestOperationsDoNotRetryEOF(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
	}{
		{
			name:   "GET",
			method: http.MethodGet,
		},
		{
			name:   "POST_with_body",
			method: http.MethodPost,
			body:   `{"value":1}`,
		},
		{
			name:   "PUT_with_body",
			method: http.MethodPut,
			body:   `{"value":1}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if req.URL.Path == "/" {
					w.Header().Set("X-Elastic-Product", "Elasticsearch")
					// 避免复用验活连接，单独验证 SDK 的 EOF 重试行为。
					w.Header().Set("Connection", "close")
					_, _ = io.WriteString(w, `{"version":{"number":"7.17.10"}}`)
					return
				}
				calls.Add(1)
				if req.Method != tt.method {
					t.Errorf("请求方法不一致: got=%q want=%q", req.Method, tt.method)
				}
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Errorf("读取请求体: %v", err)
				} else if string(body) != tt.body {
					t.Errorf("请求体不一致: got=%q want=%q", body, tt.body)
				}
				// 收到完整请求体后断连，模拟写入可能已生效但响应丢失。
				closeWithoutResponse(t, w)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := New(elasticsearchx.Config{
				Name:      "search",
				Addresses: []string{server.URL},
			}, elasticsearchx.OptLogRequests(false))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			switch tt.method {
			case http.MethodGet:
				_, err = client.Get(ctx, "products", "1")
			case http.MethodPost:
				_, err = client.Count(ctx, "products", map[string]any{"value": 1})
			case http.MethodPut:
				err = client.Index(ctx, "products", "1", map[string]any{"value": 1})
			}
			if !errors.Is(err, io.EOF) {
				t.Fatalf("请求应返回 EOF: %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("请求发生重试: 请求次数=%d，期望 1", got)
			}
		})
	}
}

func closeWithoutResponse(t *testing.T, w http.ResponseWriter) {
	t.Helper()
	// 服务端已收到请求，但未发出响应即断开，模拟结果未知的 EOF。
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		t.Errorf("接管连接: %v", err)
		return
	}
	if err := conn.Close(); err != nil {
		t.Errorf("关闭连接: %v", err)
	}
}
