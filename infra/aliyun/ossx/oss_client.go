package ossx

import (
	"context"
	"errors"
	"net/http"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	osscredentials "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/aliyun/credentials-go/credentials"
)

func newOSSClient(region, endpoint string, credential credentials.Credential, httpClient *http.Client) *oss.Client {
	// 显式构造配置，避免 LoadDefaultConfig 从环境开启请求调试输出。
	sdkConfig := &oss.Config{
		Region: new(region),
		CredentialsProvider: osscredentials.CredentialsProviderFunc(func(ctx context.Context) (osscredentials.Credentials, error) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return osscredentials.Credentials{}, ctxErr
			}
			value, credentialErr := credential.GetCredential()
			if credentialErr != nil {
				return osscredentials.Credentials{}, credentialErr
			}
			if ctxErr := ctx.Err(); ctxErr != nil {
				return osscredentials.Credentials{}, ctxErr
			}
			if value == nil {
				return osscredentials.Credentials{}, errors.New("ossx: empty credential")
			}
			return osscredentials.Credentials{
				AccessKeyID:     oss.ToString(value.AccessKeyId),
				AccessKeySecret: oss.ToString(value.AccessKeySecret),
				SecurityToken:   oss.ToString(value.SecurityToken),
			}, nil
		}),
		HttpClient:       httpClient,
		RetryMaxAttempts: new(1),
		LogLevel:         oss.Ptr[int](oss.LogOff),
	}
	if endpoint != "" {
		sdkConfig.Endpoint = new(endpoint)
	}
	return oss.NewClient(sdkConfig)
}
