package elasticsearchservice

import (
	"errors"

	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
)

// Service 借用 ES 客户端组织通用流程，不创建或关闭客户端，可并发使用。
type Service struct {
	client *elasticsearchx.Client
}

// New 创建 service；应用须在所有调用结束后再关闭传入的客户端。
func New(client *elasticsearchx.Client) (*Service, error) {
	if client == nil {
		return nil, errors.New("elasticsearchservice: nil client")
	}
	return &Service{client: client}, nil
}
