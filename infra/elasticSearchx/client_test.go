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

type performerFunc func(*http.Request) (*http.Response, error)

func (f performerFunc) Perform(req *http.Request) (*http.Response, error) { return f(req) }

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func jsonResponse(req *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}
}

func captureElasticSearchLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
	old := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() { logit.SetDefault(old); _ = logit.Close(logger) })
	return buf
}

func TestConfigRejectsUnsafeConnectionSettings(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Name: "search"},
		{Name: "search", Addresses: []string{"https://user:secret@example.test:9200"}},
		{Name: "search", Addresses: []string{"http://example.test:9200/?token=secret"}},
		{Name: "search", Addresses: []string{"ftp://example.test"}},
		{Name: "search", Addresses: []string{"https://example.test"}, Username: "user", Password: "pass", APIKey: "key"},
		{Name: "search", Addresses: []string{"https://example.test"}, CACert: []byte("invalid")},
		{Name: "search", Addresses: []string{"https://example.test"}, DialTimeout: -time.Second},
	} {
		if _, err := NewTransport(cfg); err == nil {
			t.Fatalf("NewTransport(%+v) 未拒绝无效配置", cfg)
		}
	}
}

func TestNewTransportConnectionPoolConfig(t *testing.T) {
	base := http.DefaultTransport.(*http.Transport)
	defaults := base.Clone()
	tests := []struct {
		name string
		cfg  Config
		want *http.Transport
	}{
		{
			name: "零值保留默认设置",
			want: &http.Transport{
				MaxIdleConns:        defaults.MaxIdleConns,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     defaults.MaxConnsPerHost,
				IdleConnTimeout:     defaults.IdleConnTimeout,
			},
		},
		{
			name: "自定义所有连接池参数",
			cfg: Config{
				MaxIdleConns:        80,
				MaxIdleConnsPerHost: 20,
				MaxConnsPerHost:     40,
				IdleConnTimeout:     30 * time.Second,
			},
			want: &http.Transport{
				MaxIdleConns:        80,
				MaxIdleConnsPerHost: 20,
				MaxConnsPerHost:     40,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		{
			name: "单项配置不覆盖其他默认设置",
			cfg: Config{
				MaxConnsPerHost: 1,
			},
			want: &http.Transport{
				MaxIdleConns:        defaults.MaxIdleConns,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     1,
				IdleConnTimeout:     defaults.IdleConnTimeout,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.Name = "search"
			cfg.Addresses = []string{"http://example.test:9200"}
			transport, err := NewTransport(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.CloseIdleConnections()
			if transport.MaxIdleConns != tt.want.MaxIdleConns || transport.MaxIdleConnsPerHost != tt.want.MaxIdleConnsPerHost || transport.MaxConnsPerHost != tt.want.MaxConnsPerHost || transport.IdleConnTimeout != tt.want.IdleConnTimeout {
				t.Fatalf("连接池配置错误: idle=%d per_host_idle=%d per_host_total=%d timeout=%v", transport.MaxIdleConns, transport.MaxIdleConnsPerHost, transport.MaxConnsPerHost, transport.IdleConnTimeout)
			}
			if transport == base || base.MaxIdleConns != defaults.MaxIdleConns || base.MaxIdleConnsPerHost != defaults.MaxIdleConnsPerHost || base.MaxConnsPerHost != defaults.MaxConnsPerHost || base.IdleConnTimeout != defaults.IdleConnTimeout {
				t.Fatal("实例连接池修改了全局默认 Transport")
			}
		})
	}
}

func TestNewTransportRejectsNegativeConnectionPoolConfig(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			name: "总空闲连接上限",
			cfg: Config{
				MaxIdleConns: -1,
			},
		},
		{
			name: "每节点空闲连接上限",
			cfg: Config{
				MaxIdleConnsPerHost: -1,
			},
		},
		{
			name: "每节点总连接上限",
			cfg: Config{
				MaxConnsPerHost: -1,
			},
		},
		{
			name: "空闲连接保留时间",
			cfg: Config{
				IdleConnTimeout: -time.Second,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.cfg
			cfg.Name = "search"
			cfg.Addresses = []string{"http://example.test:9200"}
			transport, err := NewTransport(cfg)
			if transport != nil {
				transport.CloseIdleConnections()
			}
			if err == nil || transport != nil {
				t.Fatalf("负值连接池配置未被拒绝: transport=%v err=%v", transport, err)
			}
		})
	}
}

func TestAttachVerifiesServerMajorAndCleansUp(t *testing.T) {
	cfg := Config{Name: "search", Addresses: []string{"http://example.test:9200"}}
	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	closed := 0
	client, err := Attach(context.Background(), cfg, 8, performerFunc(func(req *http.Request) (*http.Response, error) {
		if req.Method != http.MethodGet || req.URL.Path != "/" {
			t.Errorf("验活请求错误: %s %s", req.Method, req.URL.Path)
		}
		return jsonResponse(req, 200, `{"version":{"number":"7.17.10"}}`), nil
	}), func(context.Context) error { closed++; return nil }, transport)
	if err == nil || client != nil || closed != 1 || strings.Contains(err.Error(), "example.test") {
		t.Fatalf("版本不符: client=%v err=%v closed=%d", client, err, closed)
	}
}

func TestAttachRejectsInvalidHTTPResponsesAndCleansUp(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response *http.Response
	}{
		{name: "nil response"},
		{
			name:     "nil body",
			response: &http.Response{StatusCode: http.StatusOK},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{
				Name:      "search",
				Addresses: []string{"http://example.test:9200"},
			}
			transport, err := NewTransport(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(transport.CloseIdleConnections)
			closed := 0
			client, err := Attach(context.Background(), cfg, 8, performerFunc(func(*http.Request) (*http.Response, error) {
				return tt.response, nil
			}), func(context.Context) error {
				closed++
				return nil
			}, transport)
			if client != nil || err == nil || !strings.Contains(err.Error(), "startup check") || closed != 1 {
				t.Fatalf("无效响应未返回启动错误并清理: client=%v err=%v closed=%d", client, err, closed)
			}
		})
	}
}

func TestAttachRejectsNilContextAndCloseIsIdempotent(t *testing.T) {
	cfg := Config{Name: "search", Addresses: []string{"http://example.test:9200"}}
	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	closed := 0
	perform := performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"version":{"number":"7.17.10"}}`), nil
	})
	client, err := Attach(nil, cfg, 7, perform, func(context.Context) error { closed++; return nil }, transport)
	if err == nil || client != nil || closed != 1 {
		t.Fatalf("nil context 未清理: client=%v err=%v closed=%d", client, err, closed)
	}
	transport, err = NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err = Attach(context.Background(), cfg, 7, perform, func(context.Context) error { closed++; return nil }, transport)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil || closed != 2 {
		t.Fatalf("重复关闭: err=%v closed=%d", err, closed)
	}
}

func TestAttachHonorsCanceledContextAndCleansUp(t *testing.T) {
	cfg := Config{Name: "search", Addresses: []string{"http://example.test:9200"}}
	transport, err := NewTransport(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	closed := 0
	client, err := Attach(ctx, cfg, 7, performerFunc(func(req *http.Request) (*http.Response, error) {
		return nil, req.Context().Err()
	}), func(context.Context) error { closed++; return nil }, transport)
	if client != nil || !errors.Is(err, context.Canceled) || closed != 1 {
		t.Fatalf("取消验活未释放资源: client=%v err=%v closed=%d", client, err, closed)
	}
}

func TestPerformResponseBodyContract(t *testing.T) {
	captureElasticSearchLogs(t)
	upstreamErr := errors.New("upstream failure")
	for _, tt := range []struct {
		name      string
		response  *http.Response
		err       error
		wantError string
	}{
		{
			name:      "nil response",
			wantError: "elasticSearchx: empty HTTP response",
		},
		{
			name:      "nil body",
			response:  &http.Response{StatusCode: http.StatusOK},
			wantError: "elasticSearchx: nil HTTP response body",
		},
		{
			name: "empty body",
			response: &http.Response{
				StatusCode: http.StatusNoContent,
				Body:       http.NoBody,
			},
		},
		{
			name:      "upstream error",
			response:  &http.Response{StatusCode: http.StatusBadGateway},
			err:       upstreamErr,
			wantError: upstreamErr.Error(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{
				name:          "search",
				logRequests:   true,
				logDetails:    true,
				slowThreshold: time.Second,
				performer: performerFunc(func(*http.Request) (*http.Response, error) {
					return tt.response, tt.err
				}),
			}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/", nil)
			if err != nil {
				t.Fatal(err)
			}
			res, err := c.perform(req)
			if res != tt.response {
				t.Fatalf("返回响应被替换: got=%p want=%p", res, tt.response)
			}
			if tt.wantError != "" {
				if err == nil || err.Error() != tt.wantError {
					t.Fatalf("响应检查错误: got=%v want=%q", err, tt.wantError)
				}
				if tt.err != nil && !errors.Is(err, tt.err) {
					t.Fatalf("底层错误未保留: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer res.Body.Close()
			body, err := io.ReadAll(res.Body)
			if err != nil || len(body) != 0 {
				t.Fatalf("合法空响应体被改变: body=%q err=%v", body, err)
			}
		})
	}
}

func TestNonBulkLogsStatusAndContextWithoutPayload(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	ctx := logit.WithContext(context.Background())
	logit.AddField(ctx, logit.Str("request_id", "req-1"))
	c := &Client{name: "search", logRequests: true, slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 503, `{"error":{"type":"unavailable","reason":"secret-body"}}`), nil
	})}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/private-index/_search?token=secret-query", strings.NewReader(`{"query":"secret-dsl"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "ApiKey secret-key")
	res, err := c.performNonBulk(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if strings.Contains(buf.String(), "secret") || strings.Contains(buf.String(), "private-index") {
		t.Fatalf("日志泄露请求内容: %s", buf.String())
	}
	var record map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "ERROR" || record["request_id"] != "req-1" || record[logit.DownstreamTypeKey] != "elasticSearch" || record[logit.DownstreamIDKey] != "search" {
		t.Fatalf("统一日志错误: %v", record)
	}
}

func TestNonBulkRoutesLogToNamedLogger(t *testing.T) {
	var namedBuf bytes.Buffer
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&namedBuf)))
	name := t.Name()
	logit.SetNamed(name, logger)
	t.Cleanup(func() { logit.SetNamed(name, logit.Default()); _ = logit.Close(logger) })
	c := &Client{name: "search", logRequests: true, slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 503, `{"error":{"type":"unavailable"}}`), nil
	})}
	ctx := logit.WithLoggerName(context.Background(), name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.performNonBulk(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if !strings.Contains(namedBuf.String(), `"downstream_id":"search"`) {
		t.Fatalf("未写入命名 Logger: %q", namedBuf.String())
	}
}

func TestSearchAndCountPreserveResultShape(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/products/_search":
			return jsonResponse(req, 200, `{"hits":{"total":{"value":2,"relation":"eq"},"hits":[{"_source":{"id":9007199254740993}}]},"aggregations":{"k":{"value":1}}}`), nil
		case "/products/_count":
			return jsonResponse(req, 200, `{"count":3}`), nil
		default:
			t.Fatalf("意外请求: %s", req.URL.Path)
			return nil, nil
		}
	})}
	result, err := c.Search(context.Background(), "products", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	if err != nil || result.Total.Value != 2 || result.Total.Relation != "eq" || !bytes.Contains(result.Hits, []byte("9007199254740993")) {
		t.Fatalf("Search: result=%+v err=%v", result, err)
	}
	count, err := c.Count(context.Background(), "products", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	if err != nil || count != 3 {
		t.Fatalf("Count: count=%d err=%v", count, err)
	}
}

func TestSearchAndCountRejectMissingRequiredFields(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{}`), nil
	})}
	if _, err := c.Search(context.Background(), "products", map[string]any{"query": "x"}); err == nil {
		t.Fatal("Search 接受了缺失 hits 的响应")
	}
	if _, err := c.Count(context.Background(), "products", map[string]any{"query": "x"}); err == nil {
		t.Fatal("Count 接受了缺失 count 的响应")
	}
}

func TestOperationsCloseResponseBodies(t *testing.T) {
	var bodies []*trackedBody
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		status, content := 200, `{"count":1}`
		if req.Method == http.MethodPut {
			status, content = 400, `{"error":{"type":"mapper_parsing_exception"}}`
		}
		body := &trackedBody{Reader: strings.NewReader(content)}
		bodies = append(bodies, body)
		return &http.Response{StatusCode: status, Body: body, Header: make(http.Header), Request: req}, nil
	})}
	if _, err := c.Count(context.Background(), "products", map[string]any{"query": "all"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Index(context.Background(), "products", "1", map[string]any{"name": "x"}); err == nil {
		t.Fatal("Index 接受了 HTTP 400")
	}
	if len(bodies) != 2 || !bodies[0].closed || !bodies[1].closed {
		t.Fatalf("响应 Body 未全部关闭: %+v", bodies)
	}
}

func TestGetNotFoundAndBulkPartialFailure(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	calls := 0
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if req.Method == http.MethodGet {
			return jsonResponse(req, 404, `{"found":false}`), nil
		}
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		lines := bytes.Split(bytes.TrimSpace(body), []byte{'\n'})
		if len(lines) != 4 || !bytes.Contains(lines[0], []byte(`"version_type":"external_gte"`)) || !bytes.Contains(lines[0], []byte(`"version":12`)) {
			t.Fatalf("Bulk NDJSON 错误: %s", body)
		}
		return jsonResponse(req, 200, `{"errors":true,"items":[{"index":{"_id":"1","status":201}},{"index":{"_id":"2","status":429,"error":{"type":"rejected","reason":"secret-reason"}}}]}`), nil
	})}
	_, err := c.Get(context.Background(), "products", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get 404: %v", err)
	}
	result, err := c.Bulk(context.Background(), "products", []BulkAction{
		{Kind: BulkIndex, ID: "1", Document: map[string]any{"name": "first"}, Version: 12, VersionType: VersionExternalGTE},
		{Kind: BulkIndex, ID: "2", Document: map[string]any{"name": "second"}},
	})
	var bulkErr *BulkError
	if !errors.As(err, &bulkErr) || result.Succeeded != 1 || len(result.Failures) != 1 || result.Failures[0].Status != 429 || calls != 2 {
		t.Fatalf("Bulk 部分失败: result=%+v err=%v calls=%d", result, err, calls)
	}
	if strings.Contains(buf.String(), "secret-reason") {
		t.Fatalf("Bulk 日志泄露原因: %s", buf.String())
	}
}

func TestGetDistinguishesMissingIndexFromMissingDocument(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 404, `{"error":{"type":"index_not_found_exception","reason":"secret-index"},"status":404}`), nil
	})}
	_, err := c.Get(context.Background(), "missing-index", "1")
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.Status != 404 || statusErr.Type != "index_not_found_exception" {
		t.Fatalf("索引缺失应保留 HTTP 错误类型: %v", err)
	}
}

func TestBulkRejectsMalformedResponseAndLogsError(t *testing.T) {
	buf := captureElasticSearchLogs(t)
	c := &Client{name: "search", logRequests: true, slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"errors":false,"items":[]}`), nil
	})}
	_, err := c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkUpsert, ID: "1", Document: map[string]any{"name": "first"}}})
	if err == nil || !strings.Contains(err.Error(), "item count mismatch") || !strings.Contains(buf.String(), `"level":"ERROR"`) {
		t.Fatalf("畸形响应被误判为成功: err=%v log=%q", err, buf.String())
	}
}

func TestBulkDoesNotCountInformationalStatusAsSuccess(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"errors":true,"items":[{"index":{"_id":"1","status":102}}]}`), nil
	})}
	result, err := c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{"name": "first"}}})
	var bulkErr *BulkError
	if !errors.As(err, &bulkErr) || result.Succeeded != 0 || len(result.Failures) != 1 || result.Failures[0].Status != 102 {
		t.Fatalf("1xx 批量项被误判为成功: result=%+v err=%v", result, err)
	}
}

func TestBulkRejectsResponseWithoutErrorsFlag(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 200, `{"items":[{"index":{"_id":"1","status":201}}]}`), nil
	})}
	if _, err := c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkIndex, ID: "1", Document: map[string]any{"name": "first"}}}); err == nil {
		t.Fatal("Bulk 接受了缺失 errors 标志的响应")
	}
}

func TestBulkUpsertSuccessUsesNDJSON(t *testing.T) {
	c := &Client{name: "search", slowThreshold: time.Second, performer: performerFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasSuffix(body, []byte{'\n'}) || !bytes.Contains(body, []byte(`"doc_as_upsert":true`)) || !bytes.Contains(body, []byte(`"update"`)) {
			t.Fatalf("Upsert NDJSON 错误: %s", body)
		}
		return jsonResponse(req, 200, `{"errors":false,"items":[{"update":{"_id":"1","status":200}}]}`), nil
	})}
	result, err := c.Bulk(context.Background(), "products", []BulkAction{{Kind: BulkUpsert, ID: "1", Document: map[string]any{"name": "first"}}})
	if err != nil || result.Succeeded != 1 || len(result.Failures) != 0 {
		t.Fatalf("Bulk Upsert: result=%+v err=%v", result, err)
	}
}
