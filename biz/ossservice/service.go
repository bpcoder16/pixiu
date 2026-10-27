package ossservice

import (
	"errors"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
	"github.com/bpcoder16/pixiu/infra/aliyun/stsx"
)

// Service 借用 OSS 客户端组织上传流程，可通过 WithSTSClient 启用凭据签发，可并发使用。
// 客户端由调用方创建和关闭，服务不持有资源关闭责任。
type Service struct {
	oss *ossx.Client
	sts *stsx.Client
}

// Option 在构造时配置服务的可选能力。
type Option func(*Service)

// WithSTSClient 配置凭据签发使用的 STS 客户端；nil 与未配置等价。
// 客户端由调用方持有并关闭，仅 CreateUploadCredentials 要求提供该客户端。
func WithSTSClient(client *stsx.Client) Option {
	return func(service *Service) {
		service.sts = client
	}
}

// New 创建服务，仅要求 OSS 客户端，不发起网络请求。
// 调用方须在所有调用结束后关闭传入的客户端。
func New(ossClient *ossx.Client, options ...Option) (*Service, error) {
	if ossClient == nil {
		return nil, errors.New("ossservice: OSS client is required")
	}
	service := &Service{
		oss: ossClient,
	}
	for _, option := range options {
		if option != nil {
			option(service)
		}
	}
	return service, nil
}
