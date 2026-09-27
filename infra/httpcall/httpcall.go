package httpcall

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

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
	name  string
	resty *resty.Client
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

// Resty 返回底层客户端；新代码可通过 OptResty 在 New 内完成配置。
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
	c.log(resp.Request, level, resp.StatusCode(), nil)
}

func (c *Client) logError(req *resty.Request, err error) {
	if !logit.ErrorEnabled(req.Context()) {
		return
	}
	status := 0
	if responseError, ok := errors.AsType[*resty.ResponseError](err); ok {
		status = responseError.Response.StatusCode()
	}
	c.log(req, logit.ErrorLevel, status, err)
}

func (c *Client) log(req *resty.Request, level logit.Level, status int, err error) {
	ctx := req.Context()
	duration := time.Duration(0)
	if started, ok := ctx.Value(startKey{}).(time.Time); ok {
		duration = time.Since(started)
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
	fields := logit.DownstreamFields("httpcall", c.name, duration, details)
	// 跳过本方法和包级 Output 的栈帧，让 caller 指向 logSuccess/logError。
	logit.Output(ctx, level, 1, "downstream http", fields...)
}

// 请求已构造时记录最终 URL；构造前失败时保留 Resty 请求中的原文。
func requestURL(req *resty.Request) string {
	if req.RawRequest != nil && req.RawRequest.URL != nil {
		return req.RawRequest.URL.String()
	}
	return req.URL
}
