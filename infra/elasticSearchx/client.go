package elasticSearchx

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bpcoder16/pixiu/jsonx"
)

// Config 配置一个可复用的 Elasticsearch 连接。
// Name 和 Addresses 必填，其余字段可选；认证配置取决于服务端要求。
// 所有时长及连接数配置必须非负，零值使用各字段说明中的默认值。
type Config struct {
	// Name 必填，是下游日志和耗时前缀中的实例名称，不能是空字符串或纯空白。
	Name string
	// Addresses 必填，至少提供一个主机名非空的 HTTP(S) 节点地址。
	Addresses []string
	// Username 可选，用于基本认证；不能与 APIKey 同时设置。
	Username string
	// Password 可选，用于基本认证；非空时必须设置 Username，不能与 APIKey 同时设置。
	Password string
	// APIKey 可选，用于 API Key 认证；不能与 Username 或 Password 同时设置。
	APIKey string
	// CACert 可选，是追加到系统根证书池的 PEM 证书内容；为空时继承默认 Transport 的信任配置。
	CACert []byte
	// DialTimeout 可选，是建立连接的超时；零值使用 5 秒，请求总时长由 context 控制。
	DialTimeout time.Duration
	// StartupTimeout 可选，是启动验活的总超时；零值使用 5 秒，不影响后续操作。
	StartupTimeout time.Duration
	// MaxIdleConns 可选，是所有节点合计的空闲连接上限；零值继承默认 Transport。
	MaxIdleConns int
	// MaxIdleConnsPerHost 可选，是每节点空闲连接上限；零值使用 10。
	MaxIdleConnsPerHost int
	// MaxConnsPerHost 可选，是每节点总连接上限；零值继承默认 Transport（标准默认不限）。
	MaxConnsPerHost int
	// IdleConnTimeout 可选，是空闲连接保留时间；零值继承默认 Transport。
	IdleConnTimeout time.Duration
	// SlowThreshold 可选，是慢调用日志阈值；零值使用 200 毫秒。
	SlowThreshold time.Duration
}

// Option 在客户端创建时配置日志行为。
type Option func(*Client)

// OptLogRequests 控制是否输出请求结果日志，默认关闭；设为 true 开启，关闭时仍记录请求级耗时。
func OptLogRequests(enabled bool) Option {
	return func(c *Client) { c.logRequests = enabled }
}

// OptLogDetails 控制是否采集请求体和响应详情，默认关闭；与请求日志开关同时开启时才采集。
// 日志级别仅在最终输出时判断，不参与详情采集判断。
// 所有操作在解析和收尾完成后输出日志，响应体只包含实际读取的内容。
func OptLogDetails(enabled bool) Option {
	return func(c *Client) { c.logDetails = enabled }
}

// Performer 是三个官方客户端共同实现的底层请求接口。
// 错误为 nil 时必须返回非 nil 响应及 Body；无响应体时使用 http.NoBody。
type Performer interface {
	Perform(*http.Request) (*http.Response, error)
}

// Client 提供跨版本的基础操作，请求执行由内部方法统一处理。
type Client struct {
	name           string
	durationPrefix string
	performer      Performer
	transport      *http.Transport
	closeClient    func(context.Context) error
	slowThreshold  time.Duration
	logRequests    bool
	logDetails     bool
	close          sync.Once
	closeErr       error
	closed         atomic.Bool
}

var errClientClosed = errors.New("elasticSearchx: client is closed")

// NewTransport 验证配置并创建由该客户端独占的 HTTP 连接池。
func NewTransport(cfg Config) (*http.Transport, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("elasticSearchx: empty client name")
	}
	if len(cfg.Addresses) == 0 {
		return nil, errors.New("elasticSearchx: empty addresses")
	}
	for _, address := range cfg.Addresses {
		u, err := url.Parse(address)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
			return nil, errors.New("elasticSearchx: invalid address")
		}
	}
	if cfg.APIKey != "" && (cfg.Username != "" || cfg.Password != "") {
		return nil, errors.New("elasticSearchx: conflicting authentication settings")
	}
	if cfg.Password != "" && cfg.Username == "" {
		return nil, errors.New("elasticSearchx: password requires username")
	}
	if cfg.DialTimeout < 0 || cfg.StartupTimeout < 0 || cfg.SlowThreshold < 0 {
		return nil, errors.New("elasticSearchx: negative timeout")
	}
	if cfg.MaxIdleConns < 0 || cfg.MaxIdleConnsPerHost < 0 || cfg.MaxConnsPerHost < 0 || cfg.IdleConnTimeout < 0 {
		return nil, errors.New("elasticSearchx: negative connection pool setting")
	}
	timeout := cfg.DialTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("elasticSearchx: unsupported default HTTP transport")
	}
	transport := base.Clone()
	// HTTPS 也须经过本模块的拨号与 TLS 校验，不能继承绕过这些配置的专用拨号器。
	transport.DialTLS = nil
	transport.DialTLSContext = nil
	transport.DialContext = (&net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.MaxIdleConnsPerHost = 10
	// 零值保留原有连接池设置，正值只覆盖对应参数。
	if cfg.MaxIdleConns > 0 {
		transport.MaxIdleConns = cfg.MaxIdleConns
	}
	if cfg.MaxIdleConnsPerHost > 0 {
		transport.MaxIdleConnsPerHost = cfg.MaxIdleConnsPerHost
	}
	if cfg.MaxConnsPerHost > 0 {
		transport.MaxConnsPerHost = cfg.MaxConnsPerHost
	}
	if cfg.IdleConnTimeout > 0 {
		transport.IdleConnTimeout = cfg.IdleConnTimeout
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if transport.TLSClientConfig != nil {
		tlsConfig = transport.TLSClientConfig.Clone()
	}
	// 只抬高版本下限，避免放宽应用已有的 TLS 策略。
	if tlsConfig.MinVersion < tls.VersionTLS12 {
		tlsConfig.MinVersion = tls.VersionTLS12
	}
	tlsConfig.InsecureSkipVerify = false
	if len(cfg.CACert) != 0 {
		roots, err := x509.SystemCertPool()
		if err != nil {
			return nil, errors.New("elasticSearchx: load system CA certificates")
		}
		if !roots.AppendCertsFromPEM(cfg.CACert) {
			return nil, errors.New("elasticSearchx: invalid CA certificate")
		}
		tlsConfig.RootCAs = roots
	}
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}

// Attach 由版本适配包调用：绑定官方客户端并在内部启动期限内验证服务端版本。
// 传入的 transport 由返回的 Client 接管；失败时也会关闭它。
func Attach(cfg Config, major int, performer Performer, closeClient func(context.Context) error, transport *http.Transport, opts ...Option) (*Client, error) {
	if performer == nil || transport == nil {
		return nil, errors.New("elasticSearchx: invalid client initialization")
	}
	threshold := cfg.SlowThreshold
	if threshold == 0 {
		threshold = 200 * time.Millisecond
	}
	c := &Client{
		name:           cfg.Name,
		durationPrefix: downstreamElasticSearchMessage + "_" + cfg.Name,
		performer:      performer,
		transport:      transport,
		closeClient:    closeClient,
		slowThreshold:  threshold,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	timeout := cfg.StartupTimeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	// 验活期限只约束初始化；释放它不会取消后续请求或关闭客户端。
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := c.verify(ctx, major); err != nil {
		return nil, errors.Join(err, c.Close())
	}
	return c, nil
}

func (c *Client) verify(ctx context.Context, major int) (resultErr error) {
	// 使用相对路径，Host 随 SDK 选择的实际节点发送。
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "/", nil)
	if err != nil {
		return fmt.Errorf("elasticSearchx: build startup check: %w", err)
	}
	res, err := c.perform(req)
	if err != nil {
		return fmt.Errorf("elasticSearchx: startup check: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("elasticSearchx: close startup response: %w", err))
		}
	}()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("elasticSearchx: startup check: HTTP %d", res.StatusCode)
	}
	var info struct {
		Version struct {
			Number string `json:"number"`
		} `json:"version"`
	}
	if err := jsonx.DecodeOne(io.LimitReader(res.Body, 64<<10), &info); err != nil {
		return fmt.Errorf("elasticSearchx: decode startup check: %w", err)
	}
	var found int
	if _, err := fmt.Sscanf(info.Version.Number, "%d.", &found); err != nil || found != major {
		return versionError(major, info.Version.Number)
	}
	return nil
}

func versionError(expected int, actual string) error {
	return fmt.Errorf("elasticSearchx: server major mismatch: expected %d, got %q", expected, actual)
}

// perform 只执行底层请求并校验响应；日志、耗时及 Body 由具体请求方法处理。
// 错误为 nil 时响应及 Body 均非 nil；失败时关闭可能返回的 Body。
func (c *Client) perform(req *http.Request) (*http.Response, error) {
	if c.closed.Load() {
		return nil, errClientClosed
	}
	res, err := c.performer.Perform(req)
	if err == nil {
		switch {
		case res == nil:
			err = errors.New("elasticSearchx: empty HTTP response")
		case res.Body == nil:
			err = errors.New("elasticSearchx: nil HTTP response body")
		}
	}
	if res != nil && res.Request == nil {
		res.Request = req
	}
	if err != nil && res != nil && res.Body != nil {
		if closeErr := res.Body.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}
	return res, err
}

// Close 关闭客户端持有的资源；重复调用返回首次结果。停止请求后再调用。
// 等待 SDK 资源收尾，不继承启动或业务请求的取消，也不额外设置关闭期限。
func (c *Client) Close() error {
	c.close.Do(func() {
		// v7 没有 SDK Close，必须由共同层阻止后续重新建连。
		c.closed.Store(true)
		if c.closeClient != nil {
			c.closeErr = c.closeClient(context.Background())
		}
		if c.transport != nil {
			c.transport.CloseIdleConnections()
		}
	})
	return c.closeErr
}
