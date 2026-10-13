package httpcall_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

func TestTraceHeaderPropagation(t *testing.T) {
	for _, tt := range []struct {
		name          string
		options       []httpcall.Option
		logID         string
		requestHeader http.Header
		commonHeader  http.Header
		want          []string
	}{
		{
			name:  "默认关闭",
			logID: "ctx-id",
		},
		{
			name:    "显式关闭",
			options: []httpcall.Option{httpcall.OptPropagateLogID(false)},
			logID:   "ctx-id",
		},
		{
			name:    "传递已有ID",
			options: []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:   "ctx-id",
			want:    []string{"ctx-id"},
		},
		{
			name:    "无日志作用域",
			options: []httpcall.Option{httpcall.OptPropagateLogID(true)},
		},
		{
			name:          "请求Header优先",
			options:       []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:         "ctx-id",
			requestHeader: http.Header{"Pixiu-Log-Id": {"request-id"}},
			commonHeader:  http.Header{"Pixiu-Log-Id": {"client-id"}},
			want:          []string{"request-id"},
		},
		{
			name:          "请求空Header优先",
			options:       []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:         "ctx-id",
			requestHeader: http.Header{"Pixiu-Log-Id": {""}},
			want:          []string{""},
		},
		{
			name:          "请求Header忽略大小写",
			options:       []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:         "ctx-id",
			requestHeader: http.Header{"pixiu-log-id": {"request-id"}},
			want:          []string{"request-id"},
		},
		{
			name:         "公共Header优先",
			options:      []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:        "ctx-id",
			commonHeader: http.Header{"Pixiu-Log-Id": {"client-id"}},
			want:         []string{"client-id"},
		},
		{
			name:         "公共空Header优先",
			options:      []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:        "ctx-id",
			commonHeader: http.Header{"Pixiu-Log-Id": {""}},
			want:         []string{""},
		},
		{
			name:         "公共Header忽略大小写",
			options:      []httpcall.Option{httpcall.OptPropagateLogID(true)},
			logID:        "ctx-id",
			commonHeader: http.Header{"PIXIU-LOG-ID": {"client-id"}},
			want:         []string{"client-id"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			if tt.logID != "" {
				ctx = logit.WithContext(ctx)
				logit.AddMeta(ctx, logit.Str(logit.LogId, tt.logID))
			}
			client := httpcall.New("trace", append(tt.options, httpcall.OptLogRequests(false))...)
			if tt.commonHeader != nil {
				client.Resty().Header = tt.commonHeader.Clone()
			}
			var got []string
			client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				for key, values := range req.Header {
					if strings.EqualFold(key, httpcall.TraceHeader) {
						got = append(got, values...)
					}
				}
				return response(req, http.StatusOK), nil
			}))
			req := client.Request(ctx)
			if tt.requestHeader != nil {
				req.Header = tt.requestHeader.Clone()
			}
			if _, err := req.Get("https://example.test/trace"); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("出站链路Header = %q, 期望 %q", got, tt.want)
			}
			if id, _ := logit.LogIDFromContext(ctx); id != tt.logID {
				t.Fatalf("请求改写了调用方logId: %q", id)
			}
		})
	}
}

func TestTraceHeaderRetriesAndRequestIsolation(t *testing.T) {
	client := httpcall.New("trace",
		httpcall.OptPropagateLogID(true),
		httpcall.OptLogRequests(false),
		httpcall.OptResty(func(r *resty.Client) {
			r.SetRetryCount(1)
			r.SetRetryWaitTime(time.Millisecond)
			r.SetRetryMaxWaitTime(time.Millisecond)
			r.AddRetryCondition(func(resp *resty.Response, err error) bool {
				return resp != nil && resp.StatusCode() == http.StatusServiceUnavailable
			})
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				id, _ := logit.LogIDFromContext(req.Context())
				if got := req.Header.Get(httpcall.TraceHeader); got != id {
					t.Errorf("请求或重试的logId串用: Header=%q, context=%q", got, id)
				}
				return response(req, http.StatusServiceUnavailable), nil
			}))
		}),
	)
	for i := range 8 {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Parallel()
			ctx := logit.WithContext(context.Background())
			logit.AddMeta(ctx, logit.Str(logit.LogId, fmt.Sprintf("ctx-%d", i)))
			resp, err := client.Request(ctx).Get("https://example.test/trace")
			if err != nil {
				t.Fatal(err)
			}
			if resp.Request.Attempt != 2 {
				t.Fatalf("尝试次数 = %d, 期望 2", resp.Request.Attempt)
			}
		})
	}
}

func TestTraceHeaderUsesIDAtSendAndKeepsItOnRetry(t *testing.T) {
	ctx := logit.WithContext(context.Background())
	logit.AddMeta(ctx, logit.Str(logit.LogId, "preview-id"))
	var got []string
	client := httpcall.New("trace",
		httpcall.OptPropagateLogID(true),
		httpcall.OptLogRequests(false),
		httpcall.OptResty(func(r *resty.Client) {
			r.SetRetryCount(1)
			r.SetRetryWaitTime(time.Millisecond)
			r.SetRetryMaxWaitTime(time.Millisecond)
			r.AddRetryCondition(func(resp *resty.Response, err error) bool {
				return resp != nil && resp.StatusCode() == http.StatusServiceUnavailable
			})
			r.AddRetryHook(func(*resty.Response, error) {
				logit.AddMeta(ctx, logit.Str(logit.LogId, "later-id"))
			})
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				got = append(got, req.Header.Get(httpcall.TraceHeader))
				return response(req, http.StatusServiceUnavailable), nil
			}))
		}),
	)
	req := client.Request(ctx).SetDebug(true).EnableGenerateCurlOnDebug()
	req.Method = http.MethodGet
	req.URL = "https://example.test/trace"
	if req.GenerateCurlCommand() == "" {
		t.Fatal("未生成 curl 命令")
	}
	logit.AddMeta(ctx, logit.Str(logit.LogId, "send-id"))
	if _, err := req.Send(); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"send-id", "send-id"}) {
		t.Fatalf("应在首次发送时读取ID，并在重试期间沿用: %q", got)
	}
}
