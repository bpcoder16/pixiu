package bootstrap

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/infra/configx"
)

func TestNATSConfigNamespace(t *testing.T) {
	for _, tc := range []struct {
		name        string
		declaration string
		want        string
	}{
		{name: "omitted"},
		{
			name:        "null",
			declaration: "namespace: null\n",
		},
		{
			name:        "empty",
			declaration: "namespace: ''\n",
		},
		{
			name:        "configured",
			declaration: "namespace: Test_1-a\n",
			want:        "Test_1-a",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nats.test.yaml")
			if err := os.WriteFile(path, []byte(tc.declaration+"urls: [localhost:4222]\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			file, err := configx.Parse[natsFileConfig](path)
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := file.clientConfig("events")
			if err != nil || cfg.Namespace != tc.want || cfg.Name != "events" {
				t.Fatalf("namespace 应传给客户端且不改变实例名: cfg=%+v, err=%v", cfg, err)
			}
		})
	}
	for _, value := range []string{"42", "true", "{}", "[]"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nats.test.yaml")
			if err := os.WriteFile(path, []byte("namespace: "+value+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := configx.Parse[natsFileConfig](path); err == nil || !strings.Contains(err.Error(), "namespace") {
				t.Fatalf("namespace 类型应严格校验: %v", err)
			}
		})
	}
}
