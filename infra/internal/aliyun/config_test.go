package aliyun

import (
	"strings"
	"testing"
	"time"

	"github.com/aliyun/credentials-go/credentials"
)

func TestValidateConfig(t *testing.T) {
	for _, endpoint := range []string{"", "https://example.com", "http://127.0.0.1:8080/"} {
		if err := ValidateConfig("files", "cn-hangzhou", endpoint, 0, 0); err != nil {
			t.Fatalf("合法配置被拒绝: %v", err)
		}
	}
	for _, tc := range []struct {
		name           string
		region         string
		endpoint       string
		requestTimeout time.Duration
		slowThreshold  time.Duration
	}{
		{name: " ", region: "cn-hangzhou"},
		{name: "files", region: ""},
		{name: "files", region: " cn-hangzhou"},
		{name: "files", region: "cn-hangzhou ", endpoint: "https://example.com"},
		{name: "files", region: "cn-hangzhou", requestTimeout: -1},
		{name: "files", region: "cn-hangzhou", slowThreshold: -1},
		{name: "files", region: "cn-hangzhou", endpoint: "https://user:secret@example.com"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://example.com/?token=secret"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://example.com/?"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://example.com/#"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://example.com/path"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://example.com/%zz"},
		{name: "files", region: "cn-hangzhou", endpoint: "ftp://example.com"},
		{name: "files", region: "cn-hangzhou", endpoint: "https://"},
	} {
		err := ValidateConfig(tc.name, tc.region, tc.endpoint, tc.requestTimeout, tc.slowThreshold)
		if err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("应拒绝非法配置且不泄露 Endpoint 凭据: %v", err)
		}
	}
}

// 嵌入接口使任何提前读取凭据的调用失败，构造阶段只能保留原对象。
type borrowedCredential struct{ credentials.Credential }

func TestResolveCredential(t *testing.T) {
	provided := &borrowedCredential{}
	got, err := ResolveCredential("", "", provided)
	if err != nil || got != provided {
		t.Fatalf("注入凭据不应提前读取或替换: %v", err)
	}
	for _, tc := range []struct {
		id         string
		secret     string
		credential credentials.Credential
	}{
		{},
		{id: "test-id"},
		{secret: "test-secret"},
		{id: " ", secret: "test-secret"},
		{id: "test-id", secret: " "},
		{id: "test-id", credential: provided},
		{secret: "test-secret", credential: provided},
		{id: "test-id", secret: "test-secret", credential: provided},
	} {
		_, err := ResolveCredential(tc.id, tc.secret, tc.credential)
		if err == nil || strings.Contains(err.Error(), "test-secret") {
			t.Fatalf("应拒绝不完整或冲突凭据且不泄露密钥: %v", err)
		}
	}
	first, err := ResolveCredential("first-id", "first-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ResolveCredential("second-id", "second-secret", nil)
	if err != nil {
		t.Fatal(err)
	}
	for i, credential := range []credentials.Credential{first, second, first} {
		id, secret := "first-id", "first-secret"
		if i == 1 {
			id, secret = "second-id", "second-secret"
		}
		value, err := credential.GetCredential()
		if err != nil || value == nil || value.AccessKeyId == nil || value.AccessKeySecret == nil || *value.AccessKeyId != id || *value.AccessKeySecret != secret {
			t.Fatalf("独立构造的凭据串号或无法读取: %v", err)
		}
	}
}
