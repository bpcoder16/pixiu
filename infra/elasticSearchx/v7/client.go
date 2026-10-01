package v7

import (
	"context"

	"github.com/bpcoder16/pixiu/infra/elasticSearchx"
	"github.com/elastic/go-elasticsearch/v7"
)

// New 创建并验证 Elasticsearch 7 连接。
func New(ctx context.Context, cfg elasticSearchx.Config, opts ...elasticSearchx.Option) (*elasticSearchx.Client, error) {
	transport, err := elasticSearchx.NewTransport(cfg)
	if err != nil {
		return nil, err
	}
	native, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    cfg.Addresses,
		Username:     cfg.Username,
		Password:     cfg.Password,
		APIKey:       cfg.APIKey,
		Transport:    transport,
		DisableRetry: true,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return elasticSearchx.Attach(ctx, cfg, 7, native, nil, transport, opts...)
}

// NewNamed 创建并按 Config.Name 登记 Elasticsearch 7 客户端；重复名称返回错误。
// 启动阶段须串行初始化；全部创建完成后才开始业务，不与 CloseAll 并发。
func NewNamed(ctx context.Context, cfg elasticSearchx.Config, opts ...elasticSearchx.Option) (*elasticSearchx.Client, error) {
	return elasticSearchx.RegisterNamed(cfg.Name, func() (*elasticSearchx.Client, error) {
		return New(ctx, cfg, opts...)
	})
}

// NewDefault 创建默认客户端，同时按 Config.Name 登记；Name 仍必填。
// 已有默认实例时返回错误，创建失败可重试；初始化约束与 NewNamed 相同。
func NewDefault(ctx context.Context, cfg elasticSearchx.Config, opts ...elasticSearchx.Option) (*elasticSearchx.Client, error) {
	return elasticSearchx.RegisterDefault(cfg.Name, func() (*elasticSearchx.Client, error) {
		return New(ctx, cfg, opts...)
	})
}
