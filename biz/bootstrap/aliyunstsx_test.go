package bootstrap_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/aliyun/stsx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestAliyunSTSConfig(t *testing.T) {
	isolatedLog(t, func() {
		template, err := os.ReadFile("conf.example/aliyunstsx.example.yaml")
		if err != nil {
			t.Fatal(err)
		}

		cfg := loadLogConfig(t, "debug", "log:\n  format: json\n  hookStd: false\n")
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = r.ParseForm()
			if _, exists := r.Form["Policy"]; exists {
				t.Error("empty optional policy should be omitted")
			}
			if r.Form.Get("RoleArn") != "acs:ram::123456789012:role/upload" || r.Form.Get("RoleSessionName") != "config-upload" {
				t.Error("STS file configuration was not applied")
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"Credentials":{"AccessKeyId":"temp-id","AccessKeySecret":"temp-secret","SecurityToken":"temp-token","Expiration":"2030-01-01T00:00:00Z"}}`))
		}))
		defer server.Close()
		content := strings.Replace(string(template), `endpoint: ""`, `endpoint: "`+server.URL+`"`, 1)
		if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "aliyunstsx.upload.yaml"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
		bootstrap.MustRegisterAliyunSTS("upload", true)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		c := stsx.Named("upload")
		if c != stsx.Default() {
			t.Fatal("default STS alias mismatch")
		}
		req := stsx.AssumeRoleRequest{RoleARN: "acs:ram::123456789012:role/upload", SessionName: "config-upload", Duration: 40 * time.Minute}
		if _, err := c.AssumeRole(context.Background(), req); err != nil {
			t.Fatal(err)
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AssumeRole(context.Background(), req); !errors.Is(err, stsx.ErrClosed) {
			t.Fatalf("closed: %v", err)
		}
	})
}

func TestAliyunSTSRejectsMissingAndUnknownConfig(t *testing.T) {
	for _, tc := range []struct{ name, content, want string }{
		{"missing", "", "aliyunstsx.upload.yaml"},
		{"unknown", "region: cn-shanghai\naccessKeyID: test-id\naccessKeySecret: test-secret\nbucket: misplaced-bucket\n", "bucket"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: json\n  hookStd: false\n")
				if tc.content != "" {
					if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "aliyunstsx.upload.yaml"), []byte(tc.content), 0600); err != nil {
						t.Fatal(err)
					}
				}
				bootstrap.MustRegisterAliyunSTS("upload", true)
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				err, ok := value.(error)
				if !ok || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("expected config rejection: %v", value)
				}
			})
		})
	}
}
