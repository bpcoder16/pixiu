package v7

import (
	"fmt"
	"io"
	"net/http"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	"github.com/elastic/go-elasticsearch/v7"
)

// New 创建并验证 Elasticsearch 7 连接。
func New(cfg elasticsearchx.Config, opts ...elasticsearchx.Option) (*elasticsearchx.Client, error) {
	transport, err := elasticsearchx.NewTransport(cfg)
	if err != nil {
		return nil, err
	}
	native, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses:    cfg.Addresses,
		Username:     cfg.Username,
		Password:     cfg.Password,
		APIKey:       cfg.APIKey,
		Transport:    noRetryTransport{transport: transport},
		DisableRetry: true,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, err
	}
	return elasticsearchx.Attach(cfg, 7, native, nil, transport, opts...)
}

// NewNamed 创建并按 Config.Name 登记 Elasticsearch 7 客户端；重复名称返回错误。
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

type noRetryTransport struct {
	transport *http.Transport
}

func (t noRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	res, err := t.transport.RoundTrip(req)
	// v7.17.10 SDK 的 EOF 分支未检查 DisableRetry；包装后避开直接相等判断，
	// 同时保留 errors.Is(err, io.EOF)，其他传输错误保持原样。
	if err == io.EOF {
		err = fmt.Errorf("elasticsearchx/v7: transport: %w", err)
	}
	return res, err
}
