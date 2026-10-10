package elasticsearchx

import (
	"context"
	"crypto/tls"
	"encoding/pem"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewTransportRejectsEmptyHostname(t *testing.T) {
	for _, address := range []string{"http://:9200", "https://:9200"} {
		t.Run(address, func(t *testing.T) {
			transport, err := NewTransport(Config{
				Name:      "search",
				Addresses: []string{address},
			})
			if transport != nil {
				transport.CloseIdleConnections()
			}
			if transport != nil || err == nil || err.Error() != "elasticsearchx: invalid address" {
				t.Fatalf("空主机名未被拒绝: transport=%v err=%v", transport, err)
			}
		})
	}
}

func TestNewTransportIgnoresInheritedTLSDialers(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	for _, mode := range []string{"DialTLS", "DialTLSContext"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			defer server.Close()

			base := original.(*http.Transport).Clone()
			var calls atomic.Int32
			dial := func(network, address string) (net.Conn, error) {
				calls.Add(1)
				// 模拟全局拨号器跳过验证；ES 的本地配置必须仍能拒绝不可信证书。
				return tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, network, address, &tls.Config{
					InsecureSkipVerify: true,
				})
			}
			if mode == "DialTLS" {
				base.DialTLS = dial
			} else {
				base.DialTLSContext = func(_ context.Context, network, address string) (net.Conn, error) {
					return dial(network, address)
				}
			}
			http.DefaultTransport = base
			cfg := Config{
				Name:      "search",
				Addresses: []string{server.URL},
			}
			transport, err := NewTransport(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{
				Transport: transport,
				Timeout:   5 * time.Second,
			}
			response, err := client.Get(server.URL)
			if response != nil {
				response.Body.Close()
			}
			var verificationErr *tls.CertificateVerificationError
			if !errors.As(err, &verificationErr) || calls.Load() != 0 {
				t.Fatalf("证书验证被继承拨号器绕过: err=%v calls=%d", err, calls.Load())
			}

			cfg.CACert = pem.EncodeToMemory(&pem.Block{
				Type:  "CERTIFICATE",
				Bytes: server.Certificate().Raw,
			})
			trusted, err := NewTransport(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer trusted.CloseIdleConnections()
			client.Transport = trusted
			response, err = client.Get(server.URL)
			if err != nil {
				t.Fatalf("配置可信 CA 后无法连接: %v", err)
			}
			response.Body.Close()
			if calls.Load() != 0 || base.DialTLS == nil && base.DialTLSContext == nil {
				t.Fatal("拨号器未被隔离或全局 Transport 被修改")
			}
		})
	}
}

func TestNewTransportTLSVersionPolicy(t *testing.T) {
	original := http.DefaultTransport
	t.Cleanup(func() { http.DefaultTransport = original })
	tests := []struct {
		name          string
		inheritedMin  uint16
		serverVersion uint16
		wantError     bool
	}{
		{
			name:          "拒绝 TLS 1.1",
			inheritedMin:  tls.VersionTLS11,
			serverVersion: tls.VersionTLS11,
			wantError:     true,
		},
		{
			name:          "允许 TLS 1.2",
			inheritedMin:  tls.VersionTLS11,
			serverVersion: tls.VersionTLS12,
		},
		{
			name:          "保留 TLS 1.3 下限",
			inheritedMin:  tls.VersionTLS13,
			serverVersion: tls.VersionTLS12,
			wantError:     true,
		},
		{
			name:          "允许 TLS 1.3",
			inheritedMin:  tls.VersionTLS13,
			serverVersion: tls.VersionTLS13,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				w.WriteHeader(http.StatusOK)
			}))
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.TLS = &tls.Config{
				MinVersion: tt.serverVersion,
				MaxVersion: tt.serverVersion,
			}
			server.StartTLS()
			defer server.Close()
			base := original.(*http.Transport).Clone()
			base.TLSClientConfig = &tls.Config{MinVersion: tt.inheritedMin}
			http.DefaultTransport = base
			transport, err := NewTransport(Config{
				Name:      "search",
				Addresses: []string{server.URL},
				CACert: pem.EncodeToMemory(&pem.Block{
					Type:  "CERTIFICATE",
					Bytes: server.Certificate().Raw,
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer transport.CloseIdleConnections()
			client := &http.Client{
				Transport: transport,
				Timeout:   5 * time.Second,
			}
			response, err := client.Get(server.URL)
			if response != nil {
				response.Body.Close()
			}
			if (err != nil) != tt.wantError {
				t.Fatalf("TLS 版本限制未生效: err=%v wantError=%v", err, tt.wantError)
			}
			if base.TLSClientConfig.MinVersion != tt.inheritedMin {
				t.Fatal("TLS 版本配置修改了全局默认 Transport")
			}
		})
	}
}
