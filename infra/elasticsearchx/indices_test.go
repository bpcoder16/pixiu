package elasticsearchx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCreateIndexResponseAndOwnership(t *testing.T) {
	closeErr := errors.New("close failed")
	for _, tt := range []struct {
		name     string
		body     string
		closeErr error
		want     CreateIndexResult
		wantErr  bool
	}{
		{
			name: "成功",
			body: `{"acknowledged":true,"shards_acknowledged":true,"index":"validation"}`,
			want: CreateIndexResult{
				Index:              "validation",
				Acknowledged:       true,
				ShardsAcknowledged: true,
			},
		},
		{
			name: "分片未启动仍保留创建结果",
			body: `{"acknowledged":true,"shards_acknowledged":false,"index":"validation"}`,
			want: CreateIndexResult{
				Index:        "validation",
				Acknowledged: true,
			},
		},
		{
			name: "创建未确认",
			body: `{"acknowledged":false,"shards_acknowledged":false,"index":"validation"}`,
			want: CreateIndexResult{Index: "validation"},
		},
		{
			name:     "关闭失败不能丢失已创建结果",
			body:     `{"acknowledged":true,"shards_acknowledged":true,"index":"validation"}`,
			closeErr: closeErr,
			want: CreateIndexResult{
				Index:              "validation",
				Acknowledged:       true,
				ShardsAcknowledged: true,
			},
			wantErr: true,
		},
		{
			name:    "空响应",
			body:    `{}`,
			wantErr: true,
		},
		{
			name:    "缺少分片标记",
			body:    `{"acknowledged":true,"index":"validation"}`,
			wantErr: true,
		},
		{
			name:    "空标记",
			body:    `{"acknowledged":null,"shards_acknowledged":false,"index":"validation"}`,
			wantErr: true,
		},
		{
			name:    "错误索引",
			body:    `{"acknowledged":true,"shards_acknowledged":true,"index":"other"}`,
			wantErr: true,
		},
		{
			name:    "矛盾标记",
			body:    `{"acknowledged":false,"shards_acknowledged":true,"index":"validation"}`,
			wantErr: true,
		},
		{
			name:    "畸形响应",
			body:    `{"acknowledged":true`,
			wantErr: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &errorResponseBody{
				Reader:   strings.NewReader(tt.body),
				closeErr: tt.closeErr,
			}
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodPut || req.URL.Path != "/validation" || req.Header.Get("Content-Type") != "application/json" {
					t.Fatalf("错误创建请求: %s %s", req.Method, req.URL)
				}
				return &http.Response{
					StatusCode: 200,
					Body:       body,
				}, nil
			})
			got, err := client.CreateIndex(context.Background(), "validation", map[string]any{"settings": map[string]any{"number_of_shards": 1}})
			if got != tt.want || (err != nil) != tt.wantErr || body.closed != 1 {
				t.Fatalf("result=%+v err=%v closed=%d", got, err, body.closed)
			}
			if tt.closeErr != nil && !errors.Is(err, tt.closeErr) {
				t.Fatalf("丢失关闭错误: %v", err)
			}
		})
	}
}

func TestIndexManagementRejectsSelectorsBeforeSending(t *testing.T) {
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		t.Fatalf("非法参数发出了请求: %s", req.URL)
		return nil, nil
	})
	for _, index := range []string{"", " ", "_all", "*", "a,b", "a?", "a/b", "a\\b", "a#b", "a:b", "<logs-{now/d}>", ".", "..", "UPPER", "-x", "+x", "a\nb", "a b", strings.Repeat("中", 86), string([]byte{0xff})} {
		if _, err := client.CreateIndex(context.Background(), index, map[string]any{}); err == nil {
			t.Errorf("CreateIndex 接受 %q", index)
		}
		if err := client.DeleteIndex(context.Background(), index); err == nil {
			t.Errorf("DeleteIndex 接受 %q", index)
		}
		if _, err := client.Analyze(context.Background(), index, "standard", "text"); err == nil {
			t.Errorf("Analyze 接受 %q", index)
		}
	}
	if _, err := client.CreateIndex(context.Background(), "validation", nil); err == nil {
		t.Fatal("接受 nil definition")
	}
	if _, err := client.Analyze(context.Background(), "validation", " ", "text"); err == nil {
		t.Fatal("接受空分析器")
	}
}

func TestDeleteIndexRequiresAcknowledgement(t *testing.T) {
	for _, body := range []string{`{"acknowledged":true}`, `{"acknowledged":false}`, `{}`, `{"acknowledged":null}`} {
		t.Run(body, func(t *testing.T) {
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				if req.Method != http.MethodDelete || req.URL.Path != "/validation" {
					t.Fatalf("错误删除请求: %s %s", req.Method, req.URL)
				}
				return jsonResponse(req, 200, body), nil
			})
			if err := client.DeleteIndex(context.Background(), "validation"); (err == nil) != (body == `{"acknowledged":true}`) {
				t.Fatalf("错误确认语义: %v", err)
			}
		})
	}
}

func TestAnalyzeValidatesTokensAndRequest(t *testing.T) {
	for _, tokens := range []string{`[]`, `[{"token":"电脑","position":0}]`, `null`, `{}`, `"text"`, ``} {
		t.Run(tokens, func(t *testing.T) {
			client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil || req.Method != http.MethodPost || req.URL.Path != "/validation/_analyze" || string(body) != `{"analyzer":"search","text":"电脑"}` {
					t.Fatalf("错误分析请求: %s %s %s %v", req.Method, req.URL, body, err)
				}
				response := `{}`
				if tokens != "" {
					response = `{"tokens":` + tokens + `}`
				}
				return jsonResponse(req, 200, response), nil
			})
			got, err := client.Analyze(context.Background(), "validation", "search", "电脑")
			valid := strings.HasPrefix(tokens, "[")
			if (err == nil) != valid || (valid && string(got.Tokens) != tokens) || (!valid && got.Tokens != nil) {
				t.Fatalf("tokens=%s result=%s err=%v", tokens, got.Tokens, err)
			}
		})
	}
}

func TestIndexHTTPErrorPreservesCausesWithoutLoggingDetails(t *testing.T) {
	buf := captureElasticsearchLogs(t)
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return jsonResponse(req, 400, `{"error":{"type":"illegal_argument_exception","reason":"secret-top","root_cause":[{"type":"illegal_argument_exception","reason":"secret-root"}],"caused_by":{"type":"parse_exception","reason":"secret-line-3","caused_by":{"type":"illegal_argument_exception","reason":"secret-rule"}}}}`), nil
	}, OptLogRequests(true))
	_, err := client.CreateIndex(context.Background(), "validation", map[string]any{})
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) || httpErr.Status != 400 || httpErr.Reason != "secret-top" || len(httpErr.RootCause) != 1 || httpErr.RootCause[0].Reason != "secret-root" || httpErr.CausedBy == nil || httpErr.CausedBy.Reason != "secret-line-3" || httpErr.CausedBy.CausedBy == nil || httpErr.CausedBy.CausedBy.Reason != "secret-rule" {
		t.Fatalf("错误原因丢失: %+v", httpErr)
	}
	if strings.Contains(err.Error(), "secret") || strings.Contains(buf.String(), "secret") {
		t.Fatal("错误文案或概要日志泄露规则")
	}
	records := readLogRecords(t, buf)
	if len(records) != 1 || records[0]["level"] != "WARN" {
		t.Fatalf("未复用统一请求日志: %v", records)
	}
}

func TestCreateIndexDoesNotClaimOwnershipAfterReadFailure(t *testing.T) {
	readErr := errors.New("read failed")
	client := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body: &errorResponseBody{Reader: &finalReadErrorBody{
				data: `{"acknowledged":true,"shards_acknowledged":true,"index":"validation"}`,
				err:  readErr,
			}},
		}, nil
	})
	got, err := client.CreateIndex(context.Background(), "validation", map[string]any{})
	if got != (CreateIndexResult{}) || !errors.Is(err, readErr) {
		t.Fatalf("不完整读取被认定为拥有索引: %+v %v", got, err)
	}
}
