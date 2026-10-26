package aliyun

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/aliyun/credentials-go/credentials"
)

// ValidateConfig 校验服务共用的连接参数；默认值及服务特有参数由调用方处理。
// 错误不包含配置原值，由调用方补充服务包前缀。
func ValidateConfig(name, region, endpoint string, requestTimeout, slowThreshold time.Duration) error {
	if strings.TrimSpace(name) == "" || strings.TrimSpace(region) == "" {
		return errors.New("name and region are required")
	}
	if strings.TrimSpace(region) != region {
		return errors.New("region must not have surrounding whitespace")
	}
	if requestTimeout < 0 || slowThreshold < 0 {
		return errors.New("negative timeout or slow threshold")
	}
	return ValidateEndpoint(endpoint)
}

// ValidateEndpoint 校验可选的 HTTP(S) 地址，错误不包含地址原值。
func ValidateEndpoint(endpoint string) error {
	if endpoint != "" {
		u, err := url.Parse(endpoint)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(endpoint, "#") || (u.Path != "" && u.Path != "/") {
			return errors.New("invalid endpoint")
		}
	}
	return nil
}

// ResolveCredential 返回注入凭据或构造独立的固定 AK/SK 凭据，两种来源互斥。
// 不联网、不提前获取动态凭据；注入对象仍由调用方持有。
func ResolveCredential(accessKeyID, accessKeySecret string, credential credentials.Credential) (credentials.Credential, error) {
	if credential != nil {
		if accessKeyID != "" || accessKeySecret != "" {
			return nil, errors.New("credential and access keys are mutually exclusive")
		}
		return credential, nil
	}
	if strings.TrimSpace(accessKeyID) == "" || strings.TrimSpace(accessKeySecret) == "" {
		return nil, errors.New("access key ID and secret are required when credential is not provided")
	}
	credential, err := credentials.NewCredential(new(credentials.Config).
		SetType("access_key").
		SetAccessKeyId(accessKeyID).
		SetAccessKeySecret(accessKeySecret))
	if err != nil {
		return nil, fmt.Errorf("create credential: %w", err)
	}
	return credential, nil
}
