package configx

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseReturnsIndependentConfigsWithoutRegistration(t *testing.T) {
	for _, tc := range []struct {
		ext     string
		content string
	}{
		{".yaml", "server_name: demo\nhttp:\n  timeout: 5s\norigins: [one]\nlabels:\n  region: cn\n"},
		{".toml", "server_name = 'demo'\norigins = ['one']\n[http]\ntimeout = '5s'\n[labels]\nregion = 'cn'\n"},
		{".json", `{"server_name":"demo","http":{"timeout":"5s"},"origins":["one"],"labels":{"region":"cn"}}`},
	} {
		t.Run(tc.ext, func(t *testing.T) {
			file := configFile(t, tc.ext, tc.content)
			first, err := Parse[serverConfig](file)
			if err != nil {
				t.Fatal(err)
			}
			if first.Name != "demo" || first.HTTP.Timeout != 5*time.Second || first.Missing != "" {
				t.Fatalf("解析内容不正确: %+v", first)
			}
			first.Name = "changed"
			first.Origins[0] = "changed"
			first.Labels["region"] = "changed"
			second, err := Parse[serverConfig](file)
			if err != nil || second == first || second.Name != "demo" ||
				!reflect.DeepEqual(second.Origins, []string{"one"}) || second.Labels["region"] != "cn" {
				t.Fatalf("重复解析应返回独立结果: %+v, %v", second, err)
			}
			if got, err := Get[serverConfig](file); got != nil || err == nil {
				t.Fatalf("解析不应以文件路径注册配置: %+v, %v", got, err)
			}
			// 显式注册后仍可解析同一文件，解析结果的修改不影响注册对象。
			t.Cleanup(func() { configs.Delete(file) })
			if err := Load[serverConfig](file, file); err != nil {
				t.Fatal(err)
			}
			third, err := Parse[serverConfig](file)
			if err != nil {
				t.Fatal(err)
			}
			third.Labels["region"] = "changed"
			if got, err := Get[serverConfig](file); err != nil || got == third || got.Labels["region"] != "cn" {
				t.Fatalf("解析不应读取或修改注册对象: %+v, %v", got, err)
			}
		})
	}
}

func TestParseRejectsInvalidFiles(t *testing.T) {
	for _, tc := range []struct {
		label   string
		ext     string
		content string
	}{
		{"unknown-null", ".yaml", "unknown: null\n"},
		{"weak-type", ".toml", "[http]\nport = '8080'\n"},
		{"syntax", ".json", `{"server_name":`},
		{"unsupported-format", ".json5", `{}`},
	} {
		t.Run(tc.label, func(t *testing.T) {
			file := configFile(t, tc.ext, tc.content)
			if got, err := Parse[serverConfig](file); got != nil || err == nil || !strings.Contains(err.Error(), file) {
				t.Fatalf("无效文件应返回 nil 和包含路径的错误: %+v, %v", got, err)
			}
		})
	}
	file := filepath.Join(t.TempDir(), "missing.yaml")
	if got, err := Parse[serverConfig](file); got != nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("应保留文件不存在错误: %+v, %v", got, err)
	}
	if got, err := Parse[serverConfig](""); got != nil || err == nil {
		t.Fatalf("应拒绝空路径: %+v, %v", got, err)
	}
	if got, err := Parse[*serverConfig](configFile(t, ".json", `{}`)); got != nil || err == nil {
		t.Fatalf("应拒绝非结构体类型: %+v, %v", got, err)
	}
}
