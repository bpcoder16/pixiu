package httpcall_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
		Request:    req,
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(&buf)))
	old := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() { logit.SetDefault(old) })
	return &buf
}

func captureStderr(t *testing.T, run func()) string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "stderr-*")
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = file
	defer func() {
		os.Stderr = old
		file.Close()
	}()
	run()
	os.Stderr = old
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func records(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	var result []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'}) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("解析日志 %q: %v", line, err)
		}
		result = append(result, record)
	}
	return result
}

func downstreamDetails(t *testing.T, record map[string]any) map[string]any {
	t.Helper()
	details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
	if !ok {
		t.Fatalf("downstream_details 不是对象: %v", record)
	}
	return details
}

func TestRequestMethodsAndUnifiedLog(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("inventory")
	client.Resty().SetBaseURL("https://user:password@example.test")
	var methods []string
	client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		methods = append(methods, req.Method)
		if got := req.Context().Value(testContextKey{}); got != "trace-1" {
			t.Errorf("请求没有继承 context: %v", got)
		}
		if req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodPatch {
			body, err := io.ReadAll(req.Body)
			if err != nil || string(body) != `{"secret":"private-body"}` {
				t.Errorf("%s 请求体 = %q, %v", req.Method, body, err)
			}
		}
		return response(req, http.StatusOK), nil
	}))
	ctx := context.WithValue(context.Background(), testContextKey{}, "trace-1")
	ctx = logit.WithContext(ctx)
	logit.AddField(ctx, logit.Str("trace", "trace-1"))

	requests := []struct {
		method string
		call   func() error
	}{
		{"GET", func() error { _, err := client.Request(ctx).Get("/items?token=private-query"); return err }},
		{"POST", func() error {
			_, err := client.Request(ctx).SetBody(`{"secret":"private-body"}`).Post("/items")
			return err
		}},
		{"PUT", func() error {
			_, err := client.Request(ctx).SetBody(`{"secret":"private-body"}`).Put("/items/1")
			return err
		}},
		{"PATCH", func() error {
			_, err := client.Request(ctx).SetBody(`{"secret":"private-body"}`).Patch("/items/1")
			return err
		}},
		{"DELETE", func() error { _, err := client.Request(ctx).Delete("/items/1"); return err }},
		{"HEAD", func() error { _, err := client.Request(ctx).Head("/items/1"); return err }},
		{"OPTIONS", func() error { _, err := client.Request(ctx).Options("/items"); return err }},
	}
	for _, req := range requests {
		if err := req.call(); err != nil {
			t.Fatalf("%s: %v", req.method, err)
		}
	}
	logs := records(t, buf)
	if len(logs) != len(requests) || len(methods) != len(requests) {
		t.Fatalf("发送 %d 次，实际 %d 次，日志 %d 条", len(requests), len(methods), len(logs))
	}
	for i, req := range requests {
		log := logs[i]
		details := downstreamDetails(t, log)
		if methods[i] != req.method || details["method"] != req.method {
			t.Errorf("第 %d 次方法: HTTP=%q 日志=%v", i, methods[i], details["method"])
		}
		if log["msg"] != "HttpCall" {
			t.Errorf("第 %d 条日志消息: %v", i, log["msg"])
		}
		if log["level"] != "INFO" || log[logit.DownstreamTypeKey] != "HttpCall" || log[logit.DownstreamIDKey] != "inventory" || details["status"] != float64(200) || log["trace"] != "trace-1" {
			t.Errorf("第 %d 条日志: %v", i, log)
		}
		if _, ok := log[logit.DownstreamDurationMSKey].(float64); !ok {
			t.Errorf("第 %d 条缺少毫秒耗时: %v", i, log)
		}
		if details["attempt"] != float64(1) {
			t.Errorf("第 %d 条尝试次数: %v", i, details["attempt"])
		}
		if len(details) != 5 || details["err"] != "" {
			t.Errorf("第 %d 条详情字段不完整: %v", i, details)
		}
		if _, old := log["method"]; old {
			t.Errorf("第 %d 条方法仍位于顶层: %v", i, log)
		}
	}
	if url := downstreamDetails(t, logs[0])["url"]; url != "https://user:password@example.test/items?token=private-query" {
		t.Errorf("请求 URL 未保留原文: %v", url)
	}
	if strings.Contains(buf.String(), "private-body") {
		t.Fatalf("日志意外包含请求体: %s", buf.String())
	}
}

func TestRequestDurationWithMutedResultLogs(t *testing.T) {
	buf := captureLogs(t)
	muted := logit.MustNew(logit.OptMinLevel(logit.FatalLevel), logit.OptWriter(logit.NewWriter(io.Discard)))
	logit.SetNamed(t.Name(), muted)
	t.Cleanup(func() { _ = logit.Close(muted) })
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/fail" {
				return nil, errors.New("network failed")
			}
			return response(req, http.StatusOK), nil
		}))
	}))
	ctx := logit.WithStart(context.Background())
	for i := 0; i < 2; i++ {
		if _, err := client.Request(logit.WithLoggerName(ctx, t.Name())).Get("https://example.test/items"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := client.Request(logit.WithLoggerName(ctx, t.Name())).Get("https://example.test/fail"); err == nil {
		t.Fatal("预期传输错误")
	}
	if buf.Len() != 0 {
		t.Fatalf("禁用结果日志时不应写入默认 Logger: %q", buf.String())
	}
	logit.InfoDuration(ctx, "request done")
	got := records(t, buf)
	if len(got) != 1 {
		t.Fatalf("耗时汇总日志数=%d, want 1: %v", len(got), got)
	}
	for _, key := range []string{"httpcall_1_duration_ms", "httpcall_2_duration_ms", "httpcall_3_duration_ms"} {
		if _, ok := got[0][key].(float64); !ok {
			t.Errorf("缺少下游耗时 %q: %v", key, got[0])
		}
	}
}

func TestLogDetailsCapturesRequestAndResponse(t *testing.T) {
	buf := captureLogs(t)
	requestBody := `{"secret":"request"}`
	responseBody := `{"secret":"response"}`
	client := httpcall.New("inventory",
		httpcall.OptLogDetails(true),
		httpcall.OptResty(func(r *resty.Client) {
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				body, err := io.ReadAll(req.Body)
				if err != nil || string(body) != requestBody {
					t.Errorf("实际发送的请求体 = %q, %v", body, err)
				}
				resp := response(req, http.StatusCreated)
				resp.Body = io.NopCloser(strings.NewReader(responseBody))
				resp.Header = http.Header{"X-Response": {"one", "two"}}
				resp.ContentLength = int64(len(responseBody))
				resp.Status = "201 Created"
				resp.Proto = "HTTP/1.1"
				return resp, nil
			}))
		}),
	)
	const target = "https://example.test/items?trace=1"
	if _, err := client.Request(context.Background()).
		SetHeader("Authorization", "Bearer token").
		SetBody(requestBody).
		Post(target); err != nil {
		t.Fatal(err)
	}
	logs := records(t, buf)
	if len(logs) != 1 {
		t.Fatalf("详细日志条数 = %d", len(logs))
	}
	details := downstreamDetails(t, logs[0])
	if len(details) != 14 || details["request_body"] != requestBody ||
		details["response_body"] != responseBody ||
		details["final_url"] != target ||
		details["response_status_text"] != "201 Created" ||
		details["response_proto"] != "HTTP/1.1" ||
		details["request_content_length"] != float64(len(requestBody)) ||
		details["response_content_length"] != float64(len(responseBody)) {
		t.Fatalf("详细信息不完整: %v", details)
	}
	requestHeaders, ok := details["request_headers"].(map[string]any)
	if !ok {
		t.Fatalf("请求 Header 不是对象: %v", details["request_headers"])
	}
	authorization, ok := requestHeaders["Authorization"].([]any)
	if !ok || len(authorization) != 1 || authorization[0] != "Bearer token" {
		t.Fatalf("请求 Header 未保留原文: %v", requestHeaders)
	}
	responseHeaders, ok := details["response_headers"].(map[string]any)
	if !ok {
		t.Fatalf("响应 Header 不是对象: %v", details["response_headers"])
	}
	values, ok := responseHeaders["X-Response"].([]any)
	if !ok || len(values) != 2 || values[0] != "one" || values[1] != "two" {
		t.Fatalf("响应 Header 未保留多值: %v", responseHeaders)
	}
}

func TestLogDetailsMissingResponseAndStreamingBody(t *testing.T) {
	t.Run("invalid request", func(t *testing.T) {
		buf := captureLogs(t)
		client := httpcall.New("inventory", httpcall.OptLogDetails(true))
		if _, err := client.Request(context.Background()).
			SetFileReader("file", "x.txt", strings.NewReader("x")).
			Get("https://example.test/upload"); err == nil {
			t.Fatal("预期请求构造前校验失败")
		}
		details := downstreamDetails(t, records(t, buf)[0])
		if len(details) != 14 || details["request_body"] != "" ||
			details["request_content_length"] != float64(0) ||
			details["response_body"] != "" {
			t.Fatalf("未构造请求的详细信息默认值: %v", details)
		}
		if headers, ok := details["request_headers"].(map[string]any); !ok || len(headers) != 0 {
			t.Fatalf("未构造请求的 Header 应为空对象: %v", details["request_headers"])
		}
	})

	t.Run("transport error", func(t *testing.T) {
		buf := captureLogs(t)
		client := httpcall.New("inventory",
			httpcall.OptLogDetails(true),
			httpcall.OptResty(func(r *resty.Client) {
				r.SetTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
					return nil, errors.New("dial failed")
				}))
			}),
		)
		if _, err := client.Request(context.Background()).Get("https://example.test/items"); err == nil {
			t.Fatal("预期传输失败")
		}
		details := downstreamDetails(t, records(t, buf)[0])
		if len(details) != 14 || details["request_body"] != "" ||
			details["response_body"] != "" || details["final_url"] != "" ||
			details["response_status_text"] != "" || details["response_proto"] != "" ||
			details["response_content_length"] != float64(0) {
			t.Fatalf("无响应时的详细信息默认值: %v", details)
		}
		if headers, ok := details["response_headers"].(map[string]any); !ok || len(headers) != 0 {
			t.Fatalf("无响应时 Header 应为空对象: %v", details["response_headers"])
		}
	})

	t.Run("streaming response", func(t *testing.T) {
		buf := captureLogs(t)
		client := httpcall.New("inventory",
			httpcall.OptLogDetails(true),
			httpcall.OptResty(func(r *resty.Client) {
				r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
					resp := response(req, http.StatusOK)
					resp.Body = io.NopCloser(strings.NewReader("streamed response"))
					resp.ContentLength = int64(len("streamed response"))
					return resp, nil
				}))
			}),
		)
		resp, err := client.Request(context.Background()).
			SetDoNotParseResponse(true).
			Get("https://example.test/items")
		if err != nil {
			t.Fatal(err)
		}
		details := downstreamDetails(t, records(t, buf)[0])
		if details["response_body"] != "" ||
			details["response_content_length"] != float64(len("streamed response")) {
			t.Fatalf("流式响应应保留给业务读取: %v", details)
		}
		body, err := io.ReadAll(resp.RawBody())
		if err != nil || string(body) != "streamed response" {
			t.Fatalf("业务响应流被日志消费: %q, %v", body, err)
		}
		_ = resp.RawBody().Close()
	})
}

func TestLogDetailsFinalURLAfterRedirect(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("inventory",
		httpcall.OptLogDetails(true),
		httpcall.OptResty(func(r *resty.Client) {
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path == "/start" {
					resp := response(req, http.StatusFound)
					resp.Header.Set("Location", "/final")
					return resp, nil
				}
				return response(req, http.StatusOK), nil
			}))
		}),
	)
	if _, err := client.Request(context.Background()).Get("https://example.test/start"); err != nil {
		t.Fatal(err)
	}
	details := downstreamDetails(t, records(t, buf)[0])
	if details["url"] != "https://example.test/start" ||
		details["final_url"] != "https://example.test/final" {
		t.Fatalf("初始和最终 URL: %v", details)
	}
}

func TestRequestUsesContextLoggerName(t *testing.T) {
	var defaultBuf bytes.Buffer
	old := logit.Default()
	logit.SetDefault(logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&defaultBuf)),
		logit.OptMinLevel(logit.WarnLevel),
	))
	t.Cleanup(func() { logit.SetDefault(old) })
	var namedBuf bytes.Buffer
	name := t.Name()
	logit.SetNamed(name, logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&namedBuf)),
		logit.OptCaller(true),
	))
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.URL.Path == "/failure" {
				return nil, errors.New("dial failed")
			}
			if req.URL.Path == "/default" {
				return response(req, http.StatusBadRequest), nil
			}
			return response(req, http.StatusOK), nil
		}))
	}))
	ctx := logit.WithLoggerName(context.Background(), name)
	if _, err := client.Request(ctx).Get("https://example.test/named"); err != nil {
		t.Fatal(err)
	}
	namedLogs := records(t, &namedBuf)
	if len(namedLogs) != 1 || namedLogs[0][logit.DownstreamIDKey] != "inventory" {
		t.Fatalf("命名 Logger 的请求记录: %v", namedLogs)
	}
	caller, _ := namedLogs[0]["caller"].(string)
	if !strings.Contains(caller, "httpcall.go") || strings.Contains(caller, "logit/global.go") {
		t.Fatalf("HTTP 日志 caller = %q", caller)
	}
	if defaultBuf.Len() != 0 {
		t.Fatalf("命名请求意外写入默认 Logger: %q", defaultBuf.String())
	}
	logit.SetNamed(name, logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&namedBuf)),
		logit.OptMinLevel(logit.ErrorLevel),
	))
	if _, err := client.Request(ctx).Get("https://example.test/default"); err != nil {
		t.Fatal(err)
	}
	if got := records(t, &namedBuf); len(got) != 1 || defaultBuf.Len() != 0 {
		t.Fatalf("命名 Logger 禁用 Warn 后仍输出: named=%v default=%q", got, defaultBuf.String())
	}
	if _, err := client.Request(context.Background()).Get("https://example.test/default"); err != nil {
		t.Fatal(err)
	}
	if got := records(t, &defaultBuf); len(got) != 1 || got[0][logit.DownstreamIDKey] != "inventory" || got[0]["level"] != "WARN" {
		t.Fatalf("默认 Logger 的请求记录: %v", got)
	}
	if got := records(t, &namedBuf); len(got) != 1 {
		t.Fatalf("默认请求意外写入命名 Logger: %v", got)
	}
	logit.SetDefault(logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(&defaultBuf)),
		logit.OptMinLevel(logit.FatalLevel),
	))
	if _, err := client.Request(ctx).Get("https://example.test/failure"); err == nil {
		t.Fatal("预期传输失败")
	}
	if got := records(t, &namedBuf); len(got) != 2 || got[1]["level"] != "ERROR" {
		t.Fatalf("命名 Logger 的 Error 记录: %v", got)
	}
}

type testContextKey struct{}

func TestHTTPStatusAndTransportError(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("payments")
	var calls int
	client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return response(req, http.StatusBadRequest), nil
		}
		return nil, errors.New("dial failed")
	}))
	resp, err := client.Request(context.Background()).Get("https://example.test/pay")
	if err != nil || resp.StatusCode() != http.StatusBadRequest {
		t.Fatalf("HTTP 400 必须按 Resty 语义返回响应: %v, %v", resp, err)
	}
	_, err = client.Request(context.Background()).Post("https://user:password@example.test/pay?token=private-query")
	if err == nil || !strings.Contains(err.Error(), "dial failed") {
		t.Fatalf("传输错误未返回给业务: %v", err)
	}
	logs := records(t, buf)
	if len(logs) != 2 || logs[0]["level"] != "WARN" || downstreamDetails(t, logs[0])["status"] != float64(400) {
		t.Fatalf("HTTP 错误状态日志: %v", logs)
	}
	if logs[1]["level"] != "ERROR" || !strings.Contains(downstreamDetails(t, logs[1])["err"].(string), "dial failed") {
		t.Fatalf("传输错误日志: %v", logs[1])
	}
	if got := downstreamDetails(t, logs[1])["err"]; got != err.Error() {
		t.Fatalf("错误日志未保留原始错误文本: got %v, want %q", got, err.Error())
	}
	if details := downstreamDetails(t, logs[1]); len(details) != 5 || details["status"] != float64(0) {
		t.Fatalf("传输错误未包含固定状态字段: %v", details)
	}
}

func TestRetryDurationIncludesWaitAndLogsOnce(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("inventory")
	client.Resty().SetRetryCount(1).SetRetryWaitTime(30 * time.Millisecond).SetRetryMaxWaitTime(30 * time.Millisecond)
	var attempts atomic.Int32
	client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if attempts.Add(1) == 1 {
			return nil, errors.New("temporary failure")
		}
		return response(req, http.StatusOK), nil
	}))
	if _, err := client.Request(context.Background()).Get("https://example.test/items"); err != nil {
		t.Fatal(err)
	}
	logs := records(t, buf)
	if attempts.Load() != 2 || len(logs) != 1 {
		t.Fatalf("重试 %d 次，日志 %d 条", attempts.Load(), len(logs))
	}
	if downstreamDetails(t, logs[0])["attempt"] != float64(2) || logs[0][logit.DownstreamDurationMSKey].(float64) < 25 {
		t.Fatalf("未计入重试等待: %v", logs[0])
	}
}

func TestInvalidRequestAndPanicAreLogged(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("uploads")
	if _, err := client.Request(context.Background()).SetFileReader("file", "x.txt", strings.NewReader("x")).Get("https://example.test/upload"); err == nil {
		t.Fatal("multipart GET 应为无效请求")
	}
	client.Resty().SetTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		panic("transport panic")
	}))
	func() {
		defer func() {
			if recover() == nil {
				t.Error("Resty panic 应继续向业务抛出")
			}
		}()
		_, _ = client.Request(context.Background()).Get("https://example.test/items")
	}()
	logs := records(t, buf)
	if len(logs) != 2 || logs[0]["level"] != "ERROR" || logs[1]["level"] != "ERROR" {
		t.Fatalf("无效请求和 panic 应各记录一条 Error: %v", logs)
	}
	for i, log := range logs {
		if details := downstreamDetails(t, log); len(details) != 5 {
			t.Errorf("第 %d 条错误日志详情字段不完整: %v", i, details)
		}
	}
	invalid := downstreamDetails(t, logs[0])
	if invalid["method"] != "" || invalid["attempt"] != float64(0) ||
		invalid["url"] != "" || invalid["status"] != float64(0) {
		t.Errorf("请求构造前错误的默认值不正确: %v", invalid)
	}
	if message, ok := invalid["err"].(string); !ok || message == "" {
		t.Errorf("请求构造前错误缺少描述: %v", invalid)
	}
}

func TestOptRestySuccessHookPanicLogsOnce(t *testing.T) {
	buf := captureLogs(t)
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return response(req, http.StatusOK), nil
		}))
		r.OnSuccess(func(*resty.Client, *resty.Response) {
			panic("callback failed")
		})
	}))
	ctx := logit.WithStart(context.Background())
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_, _ = client.Request(ctx).Get("https://example.test/items")
	}()
	if recovered != "callback failed" {
		t.Fatalf("回调 panic 未传给调用方: %v", recovered)
	}

	logs := records(t, buf)
	if len(logs) != 1 || logs[0]["level"] != "ERROR" {
		t.Fatalf("回调 panic 应只记录一条 Error: %v", logs)
	}
	if errText, _ := downstreamDetails(t, logs[0])["err"].(string); !strings.Contains(errText, "callback failed") {
		t.Fatalf("Error 未包含回调 panic: %v", logs[0])
	}
	logit.InfoDuration(ctx, "request done")
	logs = records(t, buf)
	if len(logs) != 2 {
		t.Fatalf("应仅增加一条耗时汇总日志: %v", logs)
	}
	if _, ok := logs[1]["httpcall_1_duration_ms"]; !ok {
		t.Fatalf("缺少唯一的 HTTP 下游耗时: %v", logs[1])
	}
	if _, ok := logs[1]["httpcall_2_duration_ms"]; ok {
		t.Fatalf("回调 panic 导致同一次调用重复计时: %v", logs[1])
	}
}

func TestRequestContextControlsTimeoutWithinClientMax(t *testing.T) {
	_ = captureLogs(t)
	client := httpcall.New("inventory")
	if got := client.Resty().GetClient().Timeout; got != time.Minute {
		t.Fatalf("默认下游最大超时 = %s, 期望 1m", got)
	}
	client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/slow" {
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		return response(req, http.StatusOK), nil
	}))

	shortCtx, cancelShort := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelShort()
	_, err := client.Request(shortCtx).Get("https://example.test/slow")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("短超时接口应由自身 context 截止: %v", err)
	}

	longCtx, cancelLong := context.WithTimeout(context.Background(), time.Second)
	defer cancelLong()
	resp, err := client.Request(longCtx).Get("https://example.test/fast")
	if err != nil || resp.StatusCode() != http.StatusOK {
		t.Fatalf("同一客户端的另一接口应使用独立截止时间: %v, %v", resp, err)
	}
}

func TestRestyOptionTimeout(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  time.Duration
	}{
		{"custom", 90 * time.Second},
		{"disabled", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
				r.SetTimeout(tc.set)
			}))
			if got := client.Resty().GetClient().Timeout; got != tc.set {
				t.Fatalf("最大超时 = %s, 期望 %s", got, tc.set)
			}
		})
	}
}

func TestRestyOptionConfiguresDuringNew(t *testing.T) {
	_ = captureLogs(t)
	configured := false
	var requestURL string
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		configured = true
		r.SetBaseURL("https://example.test")
		r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
			requestURL = req.URL.String()
			return response(req, http.StatusOK), nil
		}))
	}))
	if !configured {
		t.Fatal("OptResty 未在 New 返回前执行")
	}
	if _, err := client.Request(context.Background()).Get("/items"); err != nil {
		t.Fatal(err)
	}
	if requestURL != "https://example.test/items" {
		t.Fatalf("请求 URL = %q, want https://example.test/items", requestURL)
	}
}

func TestRestyOptionRejectsNil(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("nil Resty 配置回调应在构造 option 时失败")
		}
	}()
	_ = httpcall.OptResty(nil)
}

func TestRestyWarningsAndErrorsGoToStderr(t *testing.T) {
	logs := captureLogs(t)
	stderr := captureStderr(t, func() {
		client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
			r.SetRetryCount(1).SetRetryWaitTime(time.Millisecond).SetRetryMaxWaitTime(time.Millisecond)
			r.SetTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
				return nil, &url.Error{Op: "Get", URL: "https://user:password@example.test/items?token=private", Err: errors.New("dial failed\nnext line")}
			}))
		}))
		if _, err := client.Request(context.Background()).Get("https://user:password@example.test/items?token=private"); err == nil {
			t.Error("传输失败应返回给调用方")
		}
	})
	if !strings.Contains(stderr, `httpcall: name="inventory" level=WARN`) ||
		!strings.Contains(stderr, `httpcall: name="inventory" level=ERROR`) ||
		!strings.Contains(stderr, "dial failed") {
		t.Fatalf("Resty 诊断日志缺少名称、级别或错误原因: %q", stderr)
	}
	for _, line := range strings.Split(strings.TrimSuffix(stderr, "\n"), "\n") {
		if !strings.HasPrefix(line, `httpcall: name="inventory" level=`) {
			t.Errorf("stderr 出现未标识的物理行: %q", line)
		}
	}
	fullURL := "https://user:password@example.test/items?token=private"
	if !strings.Contains(stderr, fullURL) || !strings.Contains(logs.String(), fullURL) {
		t.Fatalf("诊断或结果日志未保留完整 URL: stderr=%q logit=%q", stderr, logs.String())
	}
	if got := len(records(t, logs)); got != 1 {
		t.Fatalf("logit 结果日志 %d 条, want 1", got)
	}
}

func TestRestyInternalWarningAndDebugOutput(t *testing.T) {
	_ = captureLogs(t)
	stderr := captureStderr(t, func() {
		client := httpcall.New("payments", httpcall.OptResty(func(r *resty.Client) {
			r.SetBasicAuth("user", "password").SetDebug(true)
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return response(req, http.StatusOK), nil
			}))
		}))
		if _, err := client.Request(context.Background()).Get("http://example.test/pay"); err != nil {
			t.Error(err)
		}
	})
	if !strings.Contains(stderr, `httpcall: name="payments" level=WARN`) ||
		!strings.Contains(stderr, "Using Basic Auth in HTTP mode") {
		t.Fatalf("Resty 内部警告未写入 stderr: %q", stderr)
	}
	if strings.Contains(stderr, "RESPONSE") || strings.Contains(stderr, "password") {
		t.Fatalf("Debugf 输出或认证信息泄漏到 stderr: %q", stderr)
	}
	if got := strings.Count(stderr, "\n"); got != 1 {
		t.Fatalf("stderr 诊断日志 %d 行, want 1: %q", got, stderr)
	}
}

func TestClientMaxTimeoutStopsRequestBeforeLongerContext(t *testing.T) {
	_ = captureLogs(t)
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		r.SetTimeout(30 * time.Millisecond)
	}))
	client.Resty().SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := client.Request(ctx).Get("https://example.test/slow")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("下游最大超时未终止请求: %v", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("下游最大超时不应取消调用方 context: %v", err)
	}
}

func TestRequestDeadlineCoversRetryWait(t *testing.T) {
	_ = captureLogs(t)
	client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
		r.SetTimeout(time.Second)
	}))
	client.Resty().SetRetryCount(2).SetRetryWaitTime(200 * time.Millisecond).SetRetryMaxWaitTime(200 * time.Millisecond)
	var attempts atomic.Int32
	client.Resty().SetTransport(roundTripFunc(func(*http.Request) (*http.Response, error) {
		attempts.Add(1)
		return nil, errors.New("temporary failure")
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.Request(ctx).Get("https://example.test/items")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("请求截止时间未覆盖重试等待: %v", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("截止时间到期后仍重试: %d 次", got)
	}
}
