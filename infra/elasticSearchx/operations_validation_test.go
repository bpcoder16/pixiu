package elasticSearchx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
)

func TestSearchRejectsNonArrayHits(t *testing.T) {
	for _, hits := range []string{"null", "{}", "123", `"hits"`, "true"} {
		for _, partial := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/partial=%t", hits, partial), func(t *testing.T) {
				body := &errorResponseBody{Reader: strings.NewReader(fmt.Sprintf(`{"timed_out":%t,"hits":{"hits":%s}}`, partial, hits))}
				c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: 200, Body: body}, nil
				})
				result, err := c.Search(context.Background(), "products", map[string]any{})
				var partialErr *PartialSearchError
				if err == nil || errors.As(err, &partialErr) || result.Hits != nil || result.TimedOut || body.closed != 1 {
					t.Fatalf("无效命中数组未被拒绝或未收尾: result=%+v err=%v closed=%d", result, err, body.closed)
				}
			})
		}
	}
}

func TestGetValidatesResponseShape(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
		want     string
		wantErr  bool
	}{
		{
			name:     "空对象",
			response: `{}`,
			wantErr:  true,
		},
		{
			name:     "null响应",
			response: `null`,
			wantErr:  true,
		},
		{
			name:     "缺少found",
			response: `{"_source":{}}`,
			wantErr:  true,
		},
		{
			name:     "null标记",
			response: `{"found":null}`,
			wantErr:  true,
		},
		{
			name:     "成功状态下未找到",
			response: `{"found":false}`,
			wantErr:  true,
		},
		{
			name:     "数字source",
			response: `{"found":true,"_source":123}`,
			wantErr:  true,
		},
		{
			name:     "数组source",
			response: `{"found":true,"_source":[]}`,
			wantErr:  true,
		},
		{
			name:     "null source",
			response: `{"found":true,"_source":null}`,
			wantErr:  true,
		},
		{
			name:     "禁用source",
			response: `{"found":true}`,
		},
		{
			name:     "空文档",
			response: `{"found":true,"_source":{}}`,
			want:     `{}`,
		},
		{
			name:     "精度与空白",
			response: "{\"found\":true,\"_source\": \n {\"id\":9007199254740993}}",
			want:     `{"id":9007199254740993}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &errorResponseBody{Reader: strings.NewReader(tt.response)}
			c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: body}, nil
			})
			result, err := c.Get(context.Background(), "products", "1")
			if (err != nil) != tt.wantErr || errors.Is(err, ErrNotFound) || string(result) != tt.want || body.closed != 1 {
				t.Fatalf("Get 响应校验错误: result=%s err=%v closed=%d", result, err, body.closed)
			}
			if tt.name == "禁用source" && result != nil {
				t.Fatal("禁用 source 应返回 nil")
			}
		})
	}
}

func TestIndexValidatesResponseShape(t *testing.T) {
	for _, tt := range []struct {
		name     string
		response string
		wantErr  bool
	}{
		{
			name:    "空响应",
			wantErr: true,
		},
		{
			name:     "空对象",
			response: `{}`,
			wantErr:  true,
		},
		{
			name:     "null响应",
			response: `null`,
			wantErr:  true,
		},
		{
			name:     "截断",
			response: `{"result":`,
			wantErr:  true,
		},
		{
			name:     "缺少分片",
			response: `{"result":"created"}`,
			wantErr:  true,
		},
		{
			name:     "缺少结果",
			response: `{"_shards":{"total":1,"successful":1,"failed":0}}`,
			wantErr:  true,
		},
		{
			name:     "缺少失败数",
			response: `{"result":"created","_shards":{"total":1,"successful":1}}`,
			wantErr:  true,
		},
		{
			name:     "null失败数",
			response: `{"result":"created","_shards":{"total":1,"successful":1,"failed":null}}`,
			wantErr:  true,
		},
		{
			name:     "负计数",
			response: `{"result":"created","_shards":{"total":1,"successful":1,"failed":-1}}`,
			wantErr:  true,
		},
		{
			name:     "矛盾计数",
			response: `{"result":"created","_shards":{"total":1,"successful":1,"failed":1}}`,
			wantErr:  true,
		},
		{
			name:     "额外JSON",
			response: `{"result":"created","_shards":{"total":1,"successful":1,"failed":0}} {}`,
			wantErr:  true,
		},
		{
			name:     "创建",
			response: `{"result":"created","_shards":{"total":1,"successful":1,"failed":0}}`,
		},
		{
			name:     "覆盖与未分配副本",
			response: `{"result":"updated","_shards":{"total":2,"successful":1,"failed":0}}`,
		},
		{
			name:     "pipeline丢弃",
			response: `{"result":"noop","_shards":{"total":0,"successful":0,"failed":0}}`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			body := &errorResponseBody{Reader: strings.NewReader(tt.response)}
			c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 201, Body: body}, nil
			})
			err := c.Index(context.Background(), "products", "1", map[string]any{})
			if (err != nil) != tt.wantErr || body.closed != 1 {
				t.Fatalf("Index 响应校验错误: err=%v closed=%d", err, body.closed)
			}
		})
	}
}

func TestIndexRejectsFailedShards(t *testing.T) {
	for _, closeFails := range []bool{false, true} {
		t.Run(fmt.Sprint(closeFails), func(t *testing.T) {
			buf := captureElasticSearchLogs(t)
			var closeErr error
			if closeFails {
				closeErr = errors.New("close failed")
			}
			body := &errorResponseBody{
				Reader:   strings.NewReader(`{"result":"created","_shards":{"total":2,"successful":1,"failed":1,"failures":[{"reason":"secret"}]}}`),
				closeErr: closeErr,
			}
			c := newLoggingTestClient(t, func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 201, Body: body}, nil
			}, OptLogRequests(true))
			ctx := logit.WithStart(context.Background())
			err := c.Index(ctx, "products", "1", map[string]any{})
			var partialErr *PartialIndexError
			if !errors.As(err, &partialErr) || partialErr.FailedShards != 1 || (closeErr != nil && !errors.Is(err, closeErr)) || body.closed != 1 {
				t.Fatalf("分片失败或关闭错误丢失: err=%v closed=%d", err, body.closed)
			}
			logit.InfoDuration(ctx, "done")
			records := readLogRecords(t, buf)
			if len(records) != 2 || records[0]["level"] != "ERROR" || records[0][logit.DownstreamDetailsKey].(map[string]any)["error_type"] != "partial_index_error" {
				t.Fatalf("分片失败日志错误: %v", records)
			}
			elapsed, ok := records[1]["elasticSearch_catalog_1_duration_ms"]
			if !ok || elapsed != records[0]["downstream_duration_ms"] || records[1]["elasticSearch_catalog_2_duration_ms"] != nil {
				t.Fatalf("分片失败耗时丢失或重复: %v", records)
			}
			if strings.Contains(err.Error(), "secret") || strings.Contains(buf.String(), "secret") {
				t.Fatal("分片失败泄露服务端原因")
			}
		})
	}
}
