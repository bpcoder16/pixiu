package stsx

import (
	"net/http"
	"net/url"

	openapi "github.com/alibabacloud-go/darabonba-openapi/v2/client"
	stssdk "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/alibabacloud-go/tea/dara"
	"github.com/aliyun/credentials-go/credentials"
)

type stsStatusKey struct{}

// stsHTTPClient 使用实例私有连接池，避免 SDK 全局客户端缓存共享生命周期。
type stsHTTPClient struct{ client *http.Client }

func (c *stsHTTPClient) Call(req *http.Request, _ *http.Transport) (*http.Response, error) {
	response, err := c.client.Do(req)
	if response != nil {
		if status, ok := req.Context().Value(stsStatusKey{}).(*int); ok {
			*status = response.StatusCode
		}
	}
	return response, err
}

func newSTSClient(region, endpoint string, credential credentials.Credential, httpClient *http.Client) (*stssdk.Client, error) {
	// 默认地址和自定义地址均已在 New 中校验。
	u, _ := url.Parse(endpoint)
	return stssdk.NewClient(&openapi.Config{
		RegionId:   new(region),
		Endpoint:   new(u.Host),
		Protocol:   new(u.Scheme),
		Credential: credential,
		HttpClient: &stsHTTPClient{
			httpClient,
		},
		RetryOptions: &dara.RetryOptions{
			Retryable:   false,
			MaxAttempts: 1,
		},
	})
}
