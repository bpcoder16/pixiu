package httpcall

import (
	"context"
	"time"

	"github.com/go-resty/resty/v2"
)

// Client 是可复用的 HTTP 下游客户端。运行期可以并发发请求，Resty 配置应在启动期完成。
type Client struct {
	name           string
	durationPrefix string
	resty          *resty.Client
	logRequests    bool
	logDetails     bool
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

// OptLogRequests 控制是否输出每次调用的 HttpCall 结果日志，默认开启。
// 关闭时仍记录请求级耗时，Resty 自身的 stderr 诊断不受影响。
func OptLogRequests(enabled bool) Option {
	return func(c *Client) { c.logRequests = enabled }
}

// New 创建下游客户端。默认每次 HTTP 尝试最长 60 秒，不自动重试。
func New(name string, opts ...Option) *Client {
	c := &Client{
		name:           name,
		durationPrefix: downstreamHTTPMessage + "_" + name,
		resty:          resty.New().SetTimeout(time.Minute).SetLogger(restyStderrLogger{name: name}),
		logRequests:    true,
	}
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
