package httpcall

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

const downstreamHTTPMessage = "HttpCall"

type startKey struct{}

// restyStderrLogger 保留 Resty 内部警告和错误，不输出调试日志。
type restyStderrLogger struct{ name string }

func (l restyStderrLogger) Errorf(format string, args ...any) { l.write("ERROR", format, args...) }
func (l restyStderrLogger) Warnf(format string, args ...any)  { l.write("WARN", format, args...) }
func (restyStderrLogger) Debugf(string, ...any)               {}

func (l restyStderrLogger) write(level, format string, args ...any) {
	message := fmt.Sprintf(format, args...)
	// 一次写入一条物理行，便于并发请求的 stderr 收集器分辨记录。
	record := fmt.Appendf(nil, "httpcall: name=%q level=%s message=%q\n", l.name, level, message)
	_, _ = os.Stderr.Write(record)
}

// Client 是可复用的 HTTP 下游客户端。运行期可以并发发请求，Resty 配置应在启动期完成。
type Client struct {
	name       string
	resty      *resty.Client
	logDetails bool
}

// Option 配置下游客户端，创建后不应在运行期并发修改配置。
type Option func(*Client)

// OptResty 在 New 内配置底层 Resty 客户端；nil 回调会 panic。
func OptResty(configure func(*resty.Client)) Option {
	if configure == nil {
		panic("httpcall: nil resty configure")
	}
	return func(c *Client) { configure(c.resty) }
}

// OptLogDetails 控制是否记录请求和响应的详细信息，默认关闭。
// 开启后会记录原始 Header 和可读取的 Body，不脱敏或截断。
func OptLogDetails(enabled bool) Option {
	return func(c *Client) { c.logDetails = enabled }
}

// New 创建下游客户端。默认每次 HTTP 尝试最长 60 秒，不自动重试。
func New(name string, opts ...Option) *Client {
	c := &Client{name: name, resty: resty.New().SetTimeout(time.Minute).SetLogger(restyStderrLogger{name: name})}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	c.resty.OnBeforeRequest(c.onBeforeRequest)
	c.resty.OnSuccess(c.onSuccess)
	c.resty.OnError(c.onError)
	c.resty.OnInvalid(c.onInvalid)
	c.resty.OnPanic(c.onPanic)
	return c
}

// Resty 返回底层客户端；自定义事件回调应通过 OptResty 在 New 内注册。
// New 后追加事件回调不保证每次调用只记录一条结果日志。
func (c *Client) Resty() *resty.Client { return c.resty }

// Request 创建带调用方 context 的 Resty 请求。业务使用返回值的 Get/Post/Put 等方法。
func (c *Client) Request(ctx context.Context) *resty.Request {
	return c.resty.R().SetContext(ctx)
}

func (c *Client) onBeforeRequest(_ *resty.Client, req *resty.Request) error {
	c.markStart(req)
	return nil
}

func (c *Client) onSuccess(_ *resty.Client, resp *resty.Response) {
	c.logSuccess(resp)
}

func (c *Client) onError(req *resty.Request, err error) {
	c.logError(req, err)
}

func (c *Client) onInvalid(req *resty.Request, err error) {
	c.logError(req, err)
}

func (c *Client) onPanic(req *resty.Request, err error) {
	c.logError(req, err)
}

// 首次执行时写入时间戳，后续重试沿用，因此耗时包含重试等待。
func (*Client) markStart(req *resty.Request) {
	ctx := req.Context()
	if ctx.Value(startKey{}) == nil {
		req.SetContext(context.WithValue(ctx, startKey{}, time.Now()))
	}
}

func (c *Client) logSuccess(resp *resty.Response) {
	duration := recordDuration(resp.Request)
	ctx := resp.Request.Context()
	level := logit.InfoLevel
	if resp.StatusCode() >= 400 {
		if !logit.WarnEnabled(ctx) {
			return
		}
		level = logit.WarnLevel
	} else if !logit.InfoEnabled(ctx) {
		return
	}
	c.log(resp.Request, level, resp, nil, duration)
}

func (c *Client) logError(req *resty.Request, err error) {
	duration := recordDuration(req)
	if !logit.ErrorEnabled(req.Context()) {
		return
	}
	var resp *resty.Response
	if responseError, ok := errors.AsType[*resty.ResponseError](err); ok {
		resp = responseError.Response
	}
	c.log(req, logit.ErrorLevel, resp, err, duration)
}

// 结果日志被过滤时仍记录请求级耗时，供业务在结束时调用 InfoDuration 汇总。
func recordDuration(req *resty.Request) time.Duration {
	ctx := req.Context()
	duration := time.Duration(0)
	if started, ok := ctx.Value(startKey{}).(time.Time); ok {
		duration = time.Since(started)
	}
	logit.AddDownstreamDurationAuto(ctx, "httpcall", duration)
	return duration
}

func (c *Client) log(req *resty.Request, level logit.Level, resp *resty.Response, err error, duration time.Duration) {
	ctx := req.Context()
	status := 0
	if resp != nil {
		status = resp.StatusCode()
	}
	errText := ""
	if err != nil {
		errText = err.Error()
	}
	details := map[string]any{
		"method":  req.Method,
		"attempt": req.Attempt,
		"url":     requestURL(req),
		"status":  status,
		"err":     errText,
	}
	if c.logDetails {
		appendHTTPDetails(details, req, resp)
	}
	fields := logit.DownstreamFields(downstreamHTTPMessage, c.name, duration, details)
	// 跳过本方法和包级 Output 的栈帧，让 caller 指向 logSuccess/logError。
	logit.Output(ctx, level, 1, downstreamHTTPMessage, fields...)
}

func appendHTTPDetails(details map[string]any, req *resty.Request, resp *resty.Response) {
	requestHeaders := http.Header{}
	requestContentLength := int64(0)
	if req.RawRequest != nil {
		requestHeaders = req.RawRequest.Header.Clone()
		requestContentLength = req.RawRequest.ContentLength
	} else if req.Header != nil {
		requestHeaders = req.Header.Clone()
	}
	if requestHeaders == nil {
		requestHeaders = http.Header{}
	}

	responseHeaders := http.Header{}
	responseBody := ""
	finalURL := ""
	statusText := ""
	proto := ""
	responseContentLength := int64(0)
	if resp != nil {
		responseHeaders = resp.Header().Clone()
		responseBody = string(resp.Body())
		statusText = resp.Status()
		proto = resp.Proto()
		if raw := resp.RawResponse; raw != nil {
			responseContentLength = raw.ContentLength
			if raw.Request != nil && raw.Request.URL != nil {
				finalURL = raw.Request.URL.String()
			}
		}
	}
	if responseHeaders == nil {
		responseHeaders = http.Header{}
	}

	details["request_headers"] = requestHeaders
	details["request_content_length"] = requestContentLength
	details["request_body"] = requestBodyForLog(req)

	details["response_proto"] = proto
	details["response_headers"] = responseHeaders
	details["response_body"] = responseBody
	details["response_content_length"] = responseContentLength
	details["response_status_text"] = statusText
	details["final_url"] = finalURL
}

// 只读取 Resty 准备的可重读副本，不能消耗业务请求流。
func requestBodyForLog(req *resty.Request) string {
	if req.RawRequest == nil || req.RawRequest.GetBody == nil {
		return ""
	}
	reader, err := req.RawRequest.GetBody()
	if err != nil {
		return ""
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		return ""
	}
	return string(body)
}

// 已构造时取初始 HTTP 请求的 URL；构造前失败时取 Resty 请求中的原文。
func requestURL(req *resty.Request) string {
	if req.RawRequest != nil && req.RawRequest.URL != nil {
		return req.RawRequest.URL.String()
	}
	return req.URL
}
