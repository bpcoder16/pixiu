package ossx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/bpcoder16/pixiu/infra/internal/aliyun"
)

// ErrClosed 表示客户端已关闭，不再接受新操作。
var ErrClosed = errors.New("ossx: client closed")

// Config 是本包独立的连接配置；可与另一阿里云模块使用不同账号。
type Config struct {
	// Name 是日志和命名注册表中的实例名称。
	Name   string
	Region string
	// Bucket 固定为该实例的上传目标；多 Bucket 使用多个实例。
	Bucket string
	// AccessKeyID、AccessKeySecret 必须成对提供，与 Credential 互斥。
	AccessKeyID     string
	AccessKeySecret string
	// Credential 可选，由调用方持有；动态提供者须并发安全，Close 不关闭它。
	Credential credentials.Credential
	// Endpoint 可选；自定义值须为不含凭据、查询或路径的 HTTP(S) URL。
	Endpoint string
	// RequestTimeout 零值默认 60 秒，调用方的更短期限优先。
	RequestTimeout time.Duration
	// SlowThreshold 零值默认 500 毫秒。
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
	region         string
	endpoint       string
	bucket         string
	sdk            *oss.Client
	transport      *http.Transport
	requestTimeout time.Duration
	slowThreshold  time.Duration
	logRequests    bool
	closed         atomic.Bool
}

// Info 只包含非敏感的连接信息，不暴露凭据。
type Info struct {
	Name     string
	Region   string
	Bucket   string
	Endpoint string
}

func (c *Client) Info() Info {
	return Info{
		Name:     c.name,
		Region:   c.region,
		Bucket:   c.bucket,
		Endpoint: c.endpoint,
	}
}

// New 只校验和构造独立客户端，不发起网络验活，也不登记为共享实例。
func New(cfg Config, opts ...Option) (*Client, error) {
	if err := aliyun.ValidateConfig(cfg.Name, cfg.Region, cfg.Endpoint, cfg.RequestTimeout, cfg.SlowThreshold); err != nil {
		return nil, fmt.Errorf("ossx: %w", err)
	}
	if !oss.IsValidRegion(cfg.Region) {
		return nil, fmt.Errorf("ossx: %w", oss.NewErrParamInvalid("Region"))
	}
	if !oss.IsValidBucketName(&cfg.Bucket) {
		return nil, fmt.Errorf("ossx: %w", oss.NewErrParamInvalid("Bucket"))
	}
	credential, err := aliyun.ResolveCredential(cfg.AccessKeyID, cfg.AccessKeySecret, cfg.Credential)
	if err != nil {
		return nil, fmt.Errorf("ossx: %w", err)
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 60 * time.Second
	}
	if cfg.SlowThreshold == 0 {
		cfg.SlowThreshold = 500 * time.Millisecond
	}
	httpClient, transport, err := aliyun.NewHTTPClient()
	if err != nil {
		return nil, fmt.Errorf("ossx: %w", err)
	}
	c := &Client{
		name:           cfg.Name,
		region:         cfg.Region,
		endpoint:       strings.TrimRight(cfg.Endpoint, "/"),
		bucket:         cfg.Bucket,
		transport:      transport,
		requestTimeout: cfg.RequestTimeout,
		slowThreshold:  cfg.SlowThreshold,
	}
	// 先确定实际地址，确保 SDK 与 Info 使用同一个 Endpoint。
	if c.endpoint == "" {
		c.endpoint = "https://oss-" + cfg.Region + ".aliyuncs.com"
	}
	c.sdk = newOSSClient(cfg.Region, c.endpoint, credential, httpClient)
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
		return errors.New("ossx: nil context")
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
