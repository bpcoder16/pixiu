package aliyun

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

func TestHTTPClientIsolationAndTLS(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	base := original.(*http.Transport).Clone()
	base.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS10,
	}
	base.DialTLS = func(string, string) (net.Conn, error) {
		t.Error("不得使用绕过证书校验的 TLS 拨号")
		return nil, errors.New("unexpected DialTLS")
	}
	base.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
		t.Error("不得使用绕过证书校验的 TLS 拨号")
		return nil, errors.New("unexpected DialTLSContext")
	}
	http.DefaultTransport = base
	client, first, err := NewHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	defer first.CloseIdleConnections()
	_, second, err := NewHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	defer second.CloseIdleConnections()
	if client.Transport != first || first == base || first == second || first.TLSClientConfig == base.TLSClientConfig || first.TLSClientConfig == second.TLSClientConfig {
		t.Fatal("客户端必须持有独立连接池和 TLS 配置")
	}
	if first.TLSClientConfig.InsecureSkipVerify || first.TLSClientConfig.MinVersion != tls.VersionTLS12 || first.DialTLS != nil || first.DialTLSContext != nil {
		t.Fatal("TLS 安全下限未生效")
	}
	if !base.TLSClientConfig.InsecureSkipVerify || base.TLSClientConfig.MinVersion != tls.VersionTLS10 || base.DialTLS == nil || base.DialTLSContext == nil {
		t.Fatal("不得修改默认 Transport")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("不可信证书不应通过校验")
	}))
	server.Config.ErrorLog = log.New(io.Discard, "", 0)
	server.StartTLS()
	defer server.Close()
	if response, err := client.Get(server.URL); err == nil {
		_ = response.Body.Close()
		t.Fatal("不得继承跳过证书校验的设置")
	}
	base.TLSClientConfig.MinVersion = tls.VersionTLS13
	_, strict, err := NewHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	defer strict.CloseIdleConnections()
	if strict.TLSClientConfig.MinVersion != tls.VersionTLS13 {
		t.Fatal("不得降低更严格的 TLS 下限")
	}
}

func TestHTTPClientDoesNotRedirect(t *testing.T) {
	var forwarded atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		forwarded.Add(1)
	}))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer server.Close()
	client, transport, err := NewHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	defer transport.CloseIdleConnections()
	response, err := client.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusFound || forwarded.Load() != 0 {
		t.Fatal("不得将签名请求自动转发给重定向目标")
	}
}
