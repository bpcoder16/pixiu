package stsx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	stssdk "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/bpcoder16/pixiu/infra/internal/aliyun"
)

// ErrClosed 表示客户端已关闭，不再接受新操作。
var ErrClosed = errors.New("stsx: client closed")

// Config 是 STS 的独立连接配置，不包含业务角色或资源策略。
type Config struct {
	// Name 是日志和命名注册表中的实例名称。
	Name string
	// Region 是必填的阿里云地域 ID（如 cn-shanghai），用于 SDK 配置和默认 STS 地址。
	Region string
	// AccessKeyID、AccessKeySecret 必须成对提供，与 Credential 互斥。
	AccessKeyID     string
	AccessKeySecret string
	// Credential 由调用方持有；动态提供者须并发安全，Close 不关闭它。
	Credential credentials.Credential
	// Endpoint 为 HTTP(S) 地址；留空使用当前地域公网 STS 地址。
	Endpoint string
	// RequestTimeout 零值默认 60 秒，调用方的更短期限优先。
	RequestTimeout time.Duration
	// SlowThreshold 零值默认 500 毫秒。
	SlowThreshold time.Duration
}

// Option 控制本包行为，不暴露认证或连接池选项。
type Option func(*Client)

// OptLogRequests 控制结果日志，默认关闭；不输出凭据、策略和原始错误。
func OptLogRequests(enabled bool) Option {
	return func(c *Client) {
		c.logRequests = enabled
	}
}

// Client 可并发使用；配置在 New 返回后固定。
type Client struct {
	name           string
	sdk            *stssdk.Client
	transport      *http.Transport
	requestTimeout time.Duration
	slowThreshold  time.Duration
	logRequests    bool
	closed         atomic.Bool
}

// New 只校验和构造独立客户端，不联网验活，也不登记为共享实例。
func New(cfg Config, opts ...Option) (*Client, error) {
	if cfg.Endpoint == "" {
		cfg.Endpoint = "https://sts." + cfg.Region + ".aliyuncs.com"
	}
	if err := aliyun.ValidateConfig(cfg.Name, cfg.Region, cfg.Endpoint, cfg.RequestTimeout, cfg.SlowThreshold); err != nil {
		return nil, fmt.Errorf("stsx: %w", err)
	}
	credential, err := aliyun.ResolveCredential(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.Credential)
	if err != nil {
		return nil, fmt.Errorf("stsx: %w", err)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 60 * time.Second
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 500 * time.Millisecond
	}
	httpClient, transport, err := aliyun.NewHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("stsx: %w", err)
	}
	sdk, err := newSTSClient(cfg.Region, cfg.Endpoint, credential, httpClient)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("stsx: create client: %w", err)
	}
	c := &Client{
		name:           cfg.Name,
		sdk:            sdk,
		transport:      transport,
		requestTimeout: cfg.RequestTimeout,
		slowThreshold:  cfg.SlowThreshold,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	return c, nil
}

func (c *Client) check(ctx context.Context) error {
	if c.closed.Load() {
		return ErrClosed
	}
	if ctx == nil {
		return errors.New("stsx: nil context")
	}
	return ctx.Err()
}

// Close 幂等关闭空闲连接并拒绝新操作，不关闭借用凭据。
// 应用须先停止并等待使用方，最后关闭日志。
func (c *Client) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.transport.CloseIdleConnections()
	}
	return nil
}
