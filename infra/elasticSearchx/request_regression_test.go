package elasticSearchx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

func TestCountRejectsFailedShards(t *testing.T) {
	c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"count":7,"_shards":{"total":2,"successful":1,"failed":1}}`), nil
	})
	count, err := c.Count(context.Background(), "products", map[string]any{})
	if count != 0 || err == nil {
		t.Fatalf("部分计数被当作完整结果: count=%d err=%v", count, err)
	}
}

func TestOperationsLogFinalResult(t *testing.T) {
	for _, detailsEnabled := range []bool{false, true} {
		for _, tt := range []struct {
			name, operation, response, level, errorType string
			status                                      int
		}{
			{"搜索解析失败", "search", `{"hits":`, "ERROR", "response_error", 200},
			{"计数缺失", "count", `{}`, "ERROR", "response_error", 200},
			{"索引缺失", "get", `{"error":{"type":"index_not_found_exception"}}`, "WARN", "index_not_found_exception", 404},
			{"文档缺失", "get", `{"found":false}`, "INFO", "document_not_found", 404},
			{"查询错误", "search", `{"error":{"type":"parsing_exception","reason":"secret"}}`, "WARN", "parsing_exception", 400},
		} {
			t.Run(tt.name+map[bool]string{false: "概要", true: "详情"}[detailsEnabled], func(t *testing.T) {
				buf := captureElasticSearchLogs(t)
				c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
					return jsonResponse(req, tt.status, tt.response), nil
				}, OptLogRequests(true), OptLogDetails(detailsEnabled))
				// 仅启用 Error 时，后续解析错误也必须有日志和详情。
				if tt.level == "ERROR" {
					if err := logit.SetMinLevel(logit.Default(), logit.ErrorLevel); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				switch tt.operation {
				case "search":
					_, err = c.Search(context.Background(), "products", map[string]any{})
				case "count":
					_, err = c.Count(context.Background(), "products", map[string]any{})
				case "get":
					_, err = c.Get(context.Background(), "products", "1")
				}
				if err == nil {
					t.Fatal("错误响应未返回错误")
				}
				records := readLogRecords(t, buf)
				if len(records) != 1 || records[0]["level"] != tt.level {
					t.Fatalf("最终结果分级错误: %v", records)
				}
				details := records[0][logit.DownstreamDetailsKey].(map[string]any)
				if details["error_type"] != tt.errorType {
					t.Fatalf("错误类型丢失: %v", details)
				}
				if detailsEnabled && details["response_body"] != tt.response {
					t.Fatalf("解析失败详情丢失: %v", details)
				}
				if !detailsEnabled && strings.Contains(buf.String(), "secret") {
					t.Fatal("概要日志泄露服务端 reason")
				}
			})
		}
	}
}

type delayedResponseBody struct {
	io.Reader
	delay time.Duration
}

func (b *delayedResponseBody) Read(p []byte) (int, error) {
	if b.delay != 0 {
		time.Sleep(b.delay)
		b.delay = 0
	}
	return b.Reader.Read(p)
}

func (*delayedResponseBody) Close() error { return nil }

func TestOperationDurationIncludesBodyRead(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: &delayedResponseBody{
				Reader: strings.NewReader(`{"count":1}`),
				delay:  20 * time.Millisecond,
			},
		}, nil
	}, OptLogRequests(true))
	c.slowThreshold = time.Millisecond
	ctx := logit.WithStart(context.Background())
	if _, err := c.Count(ctx, "products", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	logit.InfoDuration(ctx, "done")
	records := readLogRecords(t, buf)
	if len(records) != 2 || records[0]["level"] != "WARN" {
		t.Fatalf("慢响应未记录 Warn: %v", records)
	}
	elapsed := records[0]["downstream_duration_ms"].(float64)
	if elapsed < 20 || records[1]["elasticSearch_catalog_1_duration_ms"] != elapsed || records[1]["elasticSearch_catalog_2_duration_ms"] != nil {
		t.Fatalf("响应读取未计时或重复记账: %v", records)
	}
}

type errorResponseBody struct {
	io.Reader
	closeErr error
	closed   int
}

func (b *errorResponseBody) Close() error {
	b.closed++
	return b.closeErr
}

func TestResponseAndTransportErrorClosesBody(t *testing.T) {
	for _, operation := range []string{"startup", "count", "bulk"} {
		t.Run(operation, func(t *testing.T) {
			transportErr := errors.New("transport failed")
			closeErr := errors.New("close failed")
			body := &errorResponseBody{Reader: strings.NewReader("unused"), closeErr: closeErr}
			perform := performerFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 503, Body: body}, transportErr
			})
			var err error
			if operation == "startup" {
				cfg := Config{
					Name:      "startup",
					Addresses: []string{"http://localhost:9200"},
				}
				tr, createErr := NewTransport(cfg)
				if createErr != nil {
					t.Fatal(createErr)
				}
				_, err = Attach(context.Background(), cfg, 8, perform, nil, tr)
			} else {
				c := newLoggingTestClient(t, perform)
				if operation == "count" {
					_, err = c.Count(context.Background(), "products", map[string]any{})
				} else {
					_, err = c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{}}})
				}
			}
			if !errors.Is(err, transportErr) || !errors.Is(err, closeErr) || body.closed != 1 {
				t.Fatalf("响应错误路径未收尾或丢失错误: err=%v closed=%d", err, body.closed)
			}
		})
	}
}

func TestIndexPreservesResponseReadAndCloseErrors(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	body := &failingLogBody{readErr: errors.New("read failed"), closeErr: errors.New("close failed")}
	c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body}, nil
	}, OptLogRequests(true), OptLogDetails(true))
	err := c.Index(context.Background(), "products", "1", map[string]any{})
	if !errors.Is(err, body.readErr) || !errors.Is(err, body.closeErr) || body.closed != 1 {
		t.Fatalf("读取或关闭错误丢失: err=%v closed=%d", err, body.closed)
	}
	records := readLogRecords(t, buf)
	if len(records) != 1 || records[0]["level"] != "ERROR" || records[0][logit.DownstreamDetailsKey].(map[string]any)["response_body"] != "partial" {
		t.Fatalf("收尾错误日志丢失: %v", records)
	}
}

// Reader 允许一次返回完整 JSON 和非 EOF 错误；解码器不能因此掩盖读取失败。
type finalReadErrorBody struct {
	data string
	err  error
}

func (b *finalReadErrorBody) Read(p []byte) (int, error) {
	if b.data == "" {
		return 0, io.EOF
	}
	n := copy(p, b.data)
	b.data = b.data[n:]
	if b.data == "" {
		return n, b.err
	}
	return n, nil
}

func (*finalReadErrorBody) Close() error { return nil }

func TestCompleteJSONPreservesReadError(t *testing.T) {
	for _, operation := range []string{"search", "count", "get", "bulk"} {
		t.Run(operation, func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			readErr := errors.New("response read failed")
			responses := map[string]string{
				"search": `{"hits":{"hits":[]}}`,
				"count":  `{"count":1}`,
				"get":    `{"_source":{}}`,
				"bulk":   `{"errors":false,"items":[{"index":{"_id":"1","status":201}}]}`,
			}
			c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: &finalReadErrorBody{data: responses[operation], err: readErr}}, nil
			}, OptLogRequests(true))
			var err error
			switch operation {
			case "search":
				_, err = c.Search(context.Background(), "products", map[string]any{})
			case "count":
				_, err = c.Count(context.Background(), "products", map[string]any{})
			case "get":
				_, err = c.Get(context.Background(), "products", "1")
			case "bulk":
				_, err = c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{}}})
			}
			records := readLogRecords(t, buf)
			if !errors.Is(err, readErr) || len(records) != 1 || records[0]["level"] != "ERROR" {
				t.Fatalf("完整 JSON 掩盖了读取错误: err=%v logs=%v", err, records)
			}
		})
	}
}

func TestResponseCloseErrorOverridesNonErrorLogLevel(t *testing.T) {
	for _, tt := range []struct {
		name     string
		status   int
		response string
	}{
		{"成功", 200, `{"_source":{}}`},
		{"文档缺失", 404, `{"found":false}`},
		{"索引缺失", 404, `{"error":{"type":"index_not_found_exception"}}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			closeErr := errors.New("close failed")
			body := &errorResponseBody{Reader: strings.NewReader(tt.response), closeErr: closeErr}
			c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tt.status, Body: body}, nil
			}, OptLogRequests(true))
			_, err := c.Get(context.Background(), "products", "1")
			records := readLogRecords(t, buf)
			if !errors.Is(err, closeErr) || body.closed != 1 || len(records) != 1 || records[0]["level"] != "ERROR" {
				t.Fatalf("关闭错误丢失或分级错误: err=%v closed=%d logs=%v", err, body.closed, records)
			}
		})
	}
}

func TestHTTPErrorPreservesResponseReadError(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	readErr := errors.New("response read failed")
	c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 400,
			Body: &finalReadErrorBody{
				data: `{"error":{"type":"parsing_exception"}}`,
				err:  readErr,
			},
		}, nil
	}, OptLogRequests(true))
	_, err := c.Count(context.Background(), "products", map[string]any{})
	var httpErr *HTTPError
	records := readLogRecords(t, buf)
	if !errors.Is(err, readErr) || !errors.As(err, &httpErr) || httpErr.Status != 400 || len(records) != 1 || records[0]["level"] != "ERROR" {
		t.Fatalf("HTTP 错误掩盖了读取错误: err=%v logs=%v", err, records)
	}
}

func TestStartupPreservesResponseCloseError(t *testing.T) {
	cfg := Config{
		Name:      "startup",
		Addresses: []string{"http://localhost:9200"},
	}
	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("response close failed")
	body := &errorResponseBody{Reader: strings.NewReader(`{"version":{"number":"8.0.0"}}`), closeErr: closeErr}
	c, err := Attach(context.Background(), cfg, 8, performerFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body}, nil
	}), nil, tr)
	if c != nil || !errors.Is(err, closeErr) || body.closed != 1 {
		t.Fatalf("启动验活未返回响应关闭错误: client=%v err=%v closed=%d", c, err, body.closed)
	}
}

func TestStartupFailurePreservesCleanupError(t *testing.T) {
	cfg := Config{
		Name:      "startup",
		Addresses: []string{"http://localhost:9200"},
	}
	tr, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	closeErr := errors.New("native close failed")
	_, err = Attach(context.Background(), cfg, 8, performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"version":{"number":"9.0.0"}}`), nil
	}), func(context.Context) error { return closeErr }, tr)
	if !errors.Is(err, closeErr) || !strings.Contains(err.Error(), "server major mismatch") {
		t.Fatalf("启动或清理错误丢失: %v", err)
	}
}

func TestClosedOperationsDoNotRecordDuration(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	c := newLoggingTestClient(t, func(*http.Request) (*http.Response, error) {
		t.Fatal("关闭后仍发送请求")
		return nil, nil
	}, OptLogRequests(true))
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx := logit.WithStart(context.Background())
	if _, err := c.Count(ctx, "products", map[string]any{}); err == nil {
		t.Fatal("关闭后未拒绝操作")
	}
	if buf.Len() != 0 {
		t.Fatalf("关闭后拒绝的操作产生了下游日志: %s", buf.String())
	}
	logit.InfoDuration(ctx, "done")
	records := readLogRecords(t, buf)
	if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] != nil {
		t.Fatalf("关闭后拒绝的操作被计时: %v", records)
	}
}
