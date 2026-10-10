package v9

import (
	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/elastic/go-elasticsearch/v9"
)

// New 创建并验证 Elasticsearch 9 连接。
func New(cfg elasticsearchx.Config, opts ...elasticsearchx.Option) (*elasticsearchx.Client, error) {
	transport, err := elasticsearchx.NewTransport(cfg)
	if err != nil {
		return nil, err
	}
	options := []elasticsearch.Option{
		elasticsearch.WithAddresses(cfg.Addresses...),
		elasticsearch.WithTransportOptions(elastictransport.WithTransport(transport), elastictransport.WithDisableRetry()),
	}
	if cfg.APIKey != "" {
		options = append(options, elasticsearch.WithAPIKey(cfg.APIKey))
	} else if cfg.Username != "" {
		options = append(options, elasticsearch.WithBasicAuth(cfg.Username, cfg.Password))
	}
	native, err := elasticsearch.NewBase(options...)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return elasticsearchx.Attach(cfg, 9, native, native.Close, transport, opts...)
}

// NewNamed 创建并按 Config.Name 登记 Elasticsearch 9 客户端；重复名称返回错误。
// 启动阶段须串行初始化；全部创建完成后才开始业务，不与 CloseAll 并发。
func NewNamed(cfg elasticsearchx.Config, opts ...elasticsearchx.Option) (*elasticsearchx.Client, error) {
	return elasticsearchx.RegisterNamed(cfg.Name, func() (*elasticsearchx.Client, error) {
		return New(cfg, opts...)
	})
}

// NewDefault 创建默认客户端，同时按 Config.Name 登记；Name 仍必填。
// 已有默认实例时返回错误，创建失败可重试；初始化约束与 NewNamed 相同。
func NewDefault(cfg elasticsearchx.Config, opts ...elasticsearchx.Option) (*elasticsearchx.Client, error) {
	return elasticsearchx.RegisterDefault(cfg.Name, func() (*elasticsearchx.Client, error) {
		return New(cfg, opts...)
	})
}
