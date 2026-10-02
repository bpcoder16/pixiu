package elasticSearchx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

func newLoggingTestClient(t *testing.T, perform performerFunc, opts ...Option) *Client {
	t.Helper()
	cfg := Config{
		Name:          "catalog",
		Addresses:     []string{"http://localhost:9200"},
		SlowThreshold: time.Hour,
	}
	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Attach(context.Background(), cfg, 8, performerFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/" {
			return jsonResponse(req, 200, `{"version":{"number":"8.0.0"}}`), nil
		}
		return perform(req)
	}), nil, transport, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close(context.Background()) })
	return client
}

func readLogRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(buf.Bytes()))
	var records []map[string]any
	for {
		var record map[string]any
		if err := decoder.Decode(&record); errors.Is(err, io.EOF) {
			return records
		} else if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
}

func TestPerformDoesNotLogOrRecordDuration(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	body := &trackedBody{Reader: strings.NewReader(`{"count":1}`)}
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
	}, OptLogRequests(true), OptLogDetails(true))
	buf.Reset()
	ctx := logit.WithStart(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/products/_count", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.perform(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.Request != req || res.Body != body || buf.Len() != 0 {
		t.Fatalf("底层执行方法附加了日志行为或未补齐请求: response=%+v logs=%s", res, buf.String())
	}
	logit.InfoDuration(ctx, "done")
	records := readLogRecords(t, buf)
	if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] != nil {
		t.Fatalf("底层执行方法登记了耗时: %v", records)
	}
}

func TestStartupCheckDoesNotLogOrRecordDuration(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "关闭日志", true: "开启日志及详情"}[enabled], func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			cfg := Config{
				Name:      "catalog",
				Addresses: []string{"http://localhost:9200"},
			}
			transport, err := NewTransport(cfg)
			if err != nil {
				t.Fatal(err)
			}
			ctx := logit.WithStart(context.Background())
			client, err := Attach(ctx, cfg, 8, performerFunc(func(req *http.Request) (*http.Response, error) {
				return jsonResponse(req, 200, `{"version":{"number":"8.0.0"}}`), nil
			}), nil, transport, OptLogRequests(enabled), OptLogDetails(enabled))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(context.Background()) })
			if buf.Len() != 0 {
				t.Fatalf("启动验活输出了请求日志: %s", buf.String())
			}
			logit.InfoDuration(ctx, "done")
			records := readLogRecords(t, buf)
			if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] != nil {
				t.Fatalf("启动验活登记了下游耗时: %v", records)
			}
		})
	}
}

func TestRequestsWithoutHTTPDoNotRecordDuration(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	client := newLoggingTestClient(t, func(*http.Request) (*http.Response, error) {
		t.Fatal("无效参数或空批次发出了请求")
		return nil, nil
	})
	ctx := logit.WithStart(context.Background())
	if _, err := client.Count(ctx, "invalid/index", map[string]any{}); err == nil {
		t.Fatal("普通请求未拒绝无效索引")
	}
	if _, err := client.Count(ctx, "products", make(chan int)); err == nil {
		t.Fatal("普通请求未拒绝无法编码的请求体")
	}
	if _, err := client.Bulk(ctx, "products", nil); err != nil {
		t.Fatalf("空批次未直接返回: %v", err)
	}
	if _, err := client.Bulk(ctx, "products", []BulkAction{{Kind: BulkIndex}}); err == nil {
		t.Fatal("Bulk 未拒绝无效动作")
	}
	if _, err := client.Bulk(ctx, "invalid/index", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{}}}); err == nil {
		t.Fatal("Bulk 未拒绝无效索引")
	}
	logit.InfoDuration(ctx, "done")
	records := readLogRecords(t, buf)
	if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] != nil {
		t.Fatalf("未发出的请求登记了耗时: %v", records)
	}
}

func TestNonBulkLogOptionsAndDetails(t *testing.T) {
	for _, tt := range []struct {
		name    string
		opts    []Option
		log     bool
		details bool
	}{
		{
			name: "默认关闭结果",
		},
		{
			name: "显式开启结果",
			opts: []Option{OptLogRequests(true)},
			log:  true,
		},
		{
			name: "仅开启详情不输出结果",
			opts: []Option{OptLogDetails(true)},
		},
		{
			name:    "启用结果和详情",
			opts:    []Option{OptLogRequests(true), OptLogDetails(true)},
			log:     true,
			details: true,
		},
		{
			name: "关闭结果也关闭详情",
			opts: []Option{OptLogRequests(false), OptLogDetails(true)},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			const requestBody = `{"query":{"match_all":{}}}`
			const responseBody = `{"hits":{"hits":[]}}`
			body := &trackedBody{Reader: strings.NewReader(responseBody)}
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				got, err := io.ReadAll(req.Body)
				if err != nil || string(got) != requestBody {
					t.Fatalf("日志改变了发送的请求体: body=%q err=%v", got, err)
				}
				return &http.Response{
					StatusCode: 200,
					Status:     "200 OK",
					Proto:      "HTTP/1.1",
					Header:     http.Header{"X-Secret": []string{"header-secret"}},
					Body:       body,
					Request:    req,
				}, nil
			}, tt.opts...)
			if buf.Len() != 0 {
				t.Fatalf("启动验活输出了请求日志: %s", buf.String())
			}
			buf.Reset()
			req, err := http.NewRequest(http.MethodPost, "http://localhost/products/_search", strings.NewReader(requestBody))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "ApiKey auth-secret")
			res, err := client.performNonBulk(req)
			if err != nil {
				t.Fatal(err)
			}
			if tt.details && buf.Len() != 0 {
				t.Fatal("详情日志提前读取并输出了响应体")
			}
			got, err := io.ReadAll(res.Body)
			if err != nil || string(got) != responseBody {
				t.Fatalf("业务读取响应体失败: body=%q err=%v", got, err)
			}
			if err := res.Body.Close(); err != nil || !body.closed {
				t.Fatalf("原始响应体未关闭: err=%v closed=%v", err, body.closed)
			}
			records := readLogRecords(t, buf)
			if !tt.log {
				if len(records) != 0 {
					t.Fatalf("关闭开关后仍输出日志: %v", records)
				}
				return
			}
			if len(records) != 1 || records[0]["level"] != "INFO" || records[0]["msg"] != "elasticSearch" || records[0][logit.DownstreamIDKey] != "catalog" || records[0][logit.DownstreamTypeKey] != "elasticSearch" {
				t.Fatalf("结果日志错误: %v", records)
			}
			details := records[0][logit.DownstreamDetailsKey].(map[string]any)
			if tt.details {
				if len(details) != 9 || details["request_body"] != requestBody || details["response_body"] != responseBody || details["response_proto"] != "HTTP/1.1" || details["response_status_text"] != "200 OK" {
					t.Fatalf("详情字段错误: %v", details)
				}
			} else if len(details) != 5 {
				t.Fatalf("默认日志输出了额外详情: %v", details)
			}
			if strings.Contains(buf.String(), "secret") {
				t.Fatalf("日志包含了请求或响应 Header: %s", buf.String())
			}
		})
	}
}

func TestRequestDurationUsesClientNameWhenLogsDisabledOrFiltered(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(map[bool]string{false: "关闭日志", true: "级别过滤"}[filtered], func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				if strings.HasSuffix(req.URL.Path, "/_bulk") {
					return jsonResponse(req, 200, `{"errors":false,"items":[{"index":{"_id":"1","status":201}}]}`), nil
				}
				return jsonResponse(req, 200, `{"count":1}`), nil
			}, OptLogRequests(filtered), OptLogDetails(true))
			if filtered {
				if err := logit.SetMinLevel(logit.Default(), logit.ErrorLevel); err != nil {
					t.Fatal(err)
				}
			}
			buf.Reset()
			ctx := logit.WithStart(context.Background())
			for range 2 {
				if _, err := client.Count(ctx, "products", map[string]any{"query": "all"}); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Bulk(ctx, "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{"name": "first"}}}); err != nil {
					t.Fatal(err)
				}
			}
			if buf.Len() != 0 {
				t.Fatalf("结果日志开关或过滤失效: %s", buf.String())
			}
			if err := logit.SetMinLevel(logit.Default(), logit.InfoLevel); err != nil {
				t.Fatal(err)
			}
			logit.InfoDuration(ctx, "done")
			records := readLogRecords(t, buf)
			if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] == nil || records[0]["elasticSearch_catalog_2_duration_ms"] == nil || records[0]["elasticSearch_catalog_3_duration_ms"] == nil || records[0]["elasticSearch_catalog_4_duration_ms"] == nil || records[0]["elasticSearch_catalog_5_duration_ms"] != nil {
				t.Fatalf("命名耗时丢失或重复: %v", records)
			}
		})
	}
}

func TestBulkDetailsAndDurationAreRecordedOnce(t *testing.T) {
	transportErr := errors.New("network failed")
	for _, tt := range []struct {
		name     string
		status   int
		response string
		level    string
		wantErr  bool
		err      error
	}{
		{name: "成功", status: 200, response: `{"errors":false,"items":[{"index":{"_id":"1","status":201}}]}` + "\n", level: "INFO"},
		{name: "逐项失败", status: 200, response: `{"errors":true,"items":[{"index":{"_id":"1","status":429,"error":{"type":"rejected"}}}]}`, level: "ERROR", wantErr: true},
		{name: "畸形响应", status: 200, response: `{"errors":false,"items":[]}`, level: "ERROR", wantErr: true},
		{name: "HTTP 失败", status: 503, response: `{"error":{"type":"unavailable"}}`, level: "ERROR", wantErr: true},
		{name: "HTTP 4xx", status: 429, response: `{"error":{"type":"rejected"}}`, level: "WARN", wantErr: true},
		{name: "传输失败", level: "ERROR", wantErr: true, err: transportErr},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			var requestBody string
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				requestBody = string(body)
				if tt.err != nil {
					return nil, tt.err
				}
				res := jsonResponse(req, tt.status, tt.response)
				res.Proto = "HTTP/1.1"
				res.Status = http.StatusText(tt.status)
				return res, nil
			}, OptLogRequests(true), OptLogDetails(true))
			buf.Reset()
			ctx := logit.WithStart(context.Background())
			_, err := client.Bulk(ctx, "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{"name": "first"}}})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Bulk 返回错误: %v", err)
			}
			if tt.err != nil && !errors.Is(err, tt.err) {
				t.Fatalf("Bulk 传输错误未保留: %v", err)
			}
			records := readLogRecords(t, buf)
			if len(records) != 1 || records[0]["level"] != tt.level {
				t.Fatalf("Bulk 日志重复或分级错误: %v", records)
			}
			details := records[0][logit.DownstreamDetailsKey].(map[string]any)
			proto := "HTTP/1.1"
			if tt.err != nil {
				proto = ""
			}
			if details["request_body"] != requestBody || details["response_body"] != tt.response || details["response_proto"] != proto || details["response_status_text"] != http.StatusText(tt.status) {
				t.Fatalf("Bulk 详情丢失: %v", details)
			}
			buf.Reset()
			logit.InfoDuration(ctx, "done")
			records = readLogRecords(t, buf)
			if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] == nil || records[0]["elasticSearch_catalog_2_duration_ms"] != nil {
				t.Fatalf("Bulk 耗时重复或未记录: %v", records)
			}
		})
	}
}

type failingLogBody struct {
	readErr  error
	closeErr error
	read     bool
	closed   int
}

func (b *failingLogBody) Read(p []byte) (int, error) {
	if b.read {
		return 0, io.EOF
	}
	b.read = true
	return copy(p, "partial"), b.readErr
}

func (b *failingLogBody) Close() error {
	b.closed++
	return b.closeErr
}

func TestDetailsPreserveBodyErrorsAndPartialRead(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	body := &failingLogBody{readErr: errors.New("read failed"), closeErr: errors.New("close failed")}
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: body, Request: req}, nil
	}, OptLogRequests(true), OptLogDetails(true))
	buf.Reset()
	req, err := http.NewRequest(http.MethodGet, "http://localhost/products/_search", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.performNonBulk(req)
	if err != nil || body.read {
		t.Fatalf("详情采集提前读取了响应: err=%v read=%v", err, body.read)
	}
	data, err := io.ReadAll(res.Body)
	if string(data) != "partial" || !errors.Is(err, body.readErr) {
		t.Fatalf("读取错误或数据被改变: data=%q err=%v", data, err)
	}
	for range 2 {
		if err := res.Body.Close(); !errors.Is(err, body.closeErr) {
			t.Fatalf("关闭错误被改变: %v", err)
		}
	}
	records := readLogRecords(t, buf)
	if body.closed != 1 || len(records) != 1 || records[0][logit.DownstreamDetailsKey].(map[string]any)["response_body"] != "partial" {
		t.Fatalf("关闭或日志重复，部分响应丢失: closed=%d logs=%v", body.closed, records)
	}
}

func TestNonBulkErrorLogWithDetailsIsImmediate(t *testing.T) {
	for _, withResponse := range []bool{false, true} {
		t.Run(map[bool]string{false: "无响应", true: "返回响应和错误"}[withResponse], func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			transportErr := errors.New("network failed")
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				if withResponse {
					return jsonResponse(req, 503, `{"error":"unavailable"}`), transportErr
				}
				return nil, transportErr
			}, OptLogRequests(true), OptLogDetails(true))
			buf.Reset()
			ctx := logit.WithStart(context.Background())
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/products/_search", strings.NewReader(`{"query":"all"}`))
			if err != nil {
				t.Fatal(err)
			}
			res, err := client.performNonBulk(req)
			if !errors.Is(err, transportErr) {
				t.Fatalf("传输错误被改变: %v", err)
			}
			records := readLogRecords(t, buf)
			if len(records) != 1 || records[0]["level"] != "ERROR" {
				t.Fatalf("错误日志未立即输出: %v", records)
			}
			details := records[0][logit.DownstreamDetailsKey].(map[string]any)
			if details["request_body"] != `{"query":"all"}` || details["response_body"] != "" || details["response_proto"] != "" || details["response_status_text"] != "" {
				t.Fatalf("错误日志详情缺值约定错误: %v", details)
			}
			if res != nil {
				_ = res.Body.Close()
			}
			buf.Reset()
			logit.InfoDuration(ctx, "done")
			records = readLogRecords(t, buf)
			if len(records) != 1 || records[0]["elasticSearch_catalog_1_duration_ms"] == nil {
				t.Fatalf("传输失败未记录耗时: %v", records)
			}
		})
	}
}

func TestFilteredOrDisabledDetailsDoNotReadBodyCopies(t *testing.T) {
	for _, filtered := range []bool{false, true} {
		t.Run(map[bool]string{false: "关闭日志", true: "级别过滤"}[filtered], func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			body := &trackedBody{Reader: strings.NewReader("response")}
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: body, Request: req}, nil
			}, OptLogRequests(filtered), OptLogDetails(true))
			if filtered {
				if err := logit.SetMinLevel(logit.Default(), logit.ErrorLevel); err != nil {
					t.Fatal(err)
				}
			}
			buf.Reset()
			req, err := http.NewRequest(http.MethodPost, "http://localhost/products/_search", strings.NewReader("request"))
			if err != nil {
				t.Fatal(err)
			}
			req.GetBody = func() (io.ReadCloser, error) {
				t.Fatal("不可输出的日志读取了请求副本")
				return nil, nil
			}
			res, err := client.performNonBulk(req)
			if err != nil || res.Body != body {
				t.Fatalf("不可输出的日志包装了响应流: err=%v", err)
			}
			_ = res.Body.Close()
			if buf.Len() != 0 {
				t.Fatalf("日志过滤失效: %s", buf.String())
			}
		})
	}
}
