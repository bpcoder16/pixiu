package aliyun

import (
	"crypto/tls"
	"errors"
	"net/http"
)

// NewHTTPClient 创建独立连接池，不联网或修改默认 Transport。
// 调用方负责关闭返回的 Transport；请求期限由各服务的 context 控制。
func NewHTTPClient() (*http.Client, *http.Transport, error) {
	base, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, nil, errors.New("default transport must be *http.Transport")
	}
	transport := base.Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	}
	transport.TLSClientConfig.InsecureSkipVerify = false
	if transport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
		transport.TLSClientConfig.MinVersion = tls.VersionTLS12
	}
	// 自定义 TLS 拨号可绕过 TLSClientConfig；清除后统一遵循本包的 TLS 下限。
	transport.DialTLS = nil
	transport.DialTLSContext = nil
	client := &http.Client{
		Transport: transport,
		// 签名请求不可自动转发至重定向目标。
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	return client, transport, nil
}
