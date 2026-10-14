package esx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/utils"
	essdk "github.com/alibabacloud-go/elasticsearch-20170613/v6/client"
	"github.com/alibabacloud-go/tea/dara"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/bpcoder16/pixiu/infra/internal/aliyun"
)

// ErrClosed 表示客户端已关闭，不再接受新操作。
var ErrClosed = errors.New("esx: client closed")

// Config 是本包独立的连接配置；可与另一阿里云模块使用不同账号。
type Config struct {
	// Name 是日志和命名注册表中的实例名称。
	Name   string
	Region string
	// InstanceID 固定为该客户端的管理目标；多实例分别创建客户端。
	InstanceID string
	// AccessKeyID、AccessKeySecret 必须成对提供，与 Credential 互斥。
	AccessKeyID     string
	AccessKeySecret string
	// Credential 可选，由调用方持有；动态提供者须并发安全，Close 不关闭它。
	Credential credentials.Credential
	// Endpoint 可选；自定义值须为不含凭据、查询或路径的 HTTP(S) URL。
	Endpoint string
	// RequestTimeout 零值默认 60 秒，调用方的更短期限优先。
	RequestTimeout time.Duration
	// SlowThreshold 零值默认 200 毫秒。
	SlowThreshold time.Duration
}

// Option 控制本包行为，不暴露会覆盖认证或连接池的原生 SDK 选项。
type Option func(*Client)

// OptLogRequests 控制结果日志，默认关闭；不输出正文和签名。
func OptLogRequests(enabled bool) Option {
	return func(c *Client) { c.logRequests = enabled }
}

// Client 可并发使用；配置在 New 返回后固定。
type Client struct {
	name           string
	instanceID     string
	sdk            *essdk.Client
	transport      *http.Transport
	requestTimeout time.Duration
	slowThreshold  time.Duration
	logRequests    bool
	closed         atomic.Bool
}

// New 只校验和构造独立客户端，不发起网络验活，也不登记为共享实例。
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := aliyun.ValidateConfig(cfg.Name, cfg.Region, cfg.Endpoint, cfg.RequestTimeout, cfg.SlowThreshold); err != nil {
		return nil, fmt.Errorf("esx: %w", err)
	}
	if cfg.InstanceID == "" || strings.TrimSpace(cfg.InstanceID) != cfg.InstanceID {
		return nil, errors.New("esx: instance ID is required and must not have surrounding whitespace")
	}
	credential, err := aliyun.ResolveCredential(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.Credential)
	if err != nil {
		return nil, fmt.Errorf("esx: %w", err)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 60 * time.Second
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 200 * time.Millisecond
	}
	httpClient, transport, err := aliyun.NewHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("esx: %w", err)
	}
	c := &Client{
		name:           cfg.Name,
		instanceID:     cfg.InstanceID,
		transport:      transport,
		requestTimeout: cfg.RequestTimeout,
		slowThreshold:  cfg.SlowThreshold,
	}
	sdkConfig := &openapi.Config{
		RegionId:   new(cfg.Region),
		Credential: credential,
		HttpClient: &sdkHTTPClient{client: httpClient},
		RetryOptions: &dara.RetryOptions{
			Retryable:   false,
			MaxAttempts: 1,
		},
	}
	if cfg.Endpoint != "" {
		endpoint, _ := url.Parse(cfg.Endpoint)
		sdkConfig.Endpoint = new(endpoint.Host)
		sdkConfig.Protocol = new(endpoint.Scheme)
	}
	sdk, err := essdk.NewClient(sdkConfig)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("esx: create SDK client: %w", err)
	}
	c.sdk = sdk
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
		return errors.New("esx: nil context")
	}
	return ctx.Err()
}

// Close 幂等关闭空闲连接并拒绝新操作；不排空在途请求。
// 应用须先停止并等待使用方，最后关闭日志。
func (c *Client) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.transport.CloseIdleConnections()
	}
	return nil
}

// sdkHTTPClient 使用实例私有的连接池，忽略 SDK 为单次请求创建的 Transport，
// 避免 dara 的全局客户端缓存共享账号生命周期及修改 HTTP Client 引发竞态。
type sdkHTTPClient struct{ client *http.Client }

func (c *sdkHTTPClient) Call(req *http.Request, _ *http.Transport) (*http.Response, error) {
	return c.client.Do(req)
}
