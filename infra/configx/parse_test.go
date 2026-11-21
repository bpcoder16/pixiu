package configx

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseIntegerRanges(t *testing.T) {
	type delta int8
	type numbers struct {
		Delta delta    `mapstructure:"delta"`
		Count uint8    `mapstructure:"count"`
		Large int64    `mapstructure:"large"`
		IDs   []uint64 `mapstructure:"ids"`
	}
	for _, format := range []struct {
		ext     string
		pattern string
	}{
		{".yaml", "%s: %s\n"},
		{".toml", "%s = %s\n"},
		{".json", `{"%s":%s}`},
	} {
		for _, tc := range []struct {
			field string
			value string
			want  *numbers
		}{
			{"delta", "-129", nil},
			{"delta", "128", nil},
			{"count", "-1", nil},
			{"count", "256", nil},
			{"ids", "[-1]", nil},
			{"delta", "-128", &numbers{Delta: math.MinInt8}},
			{"delta", "127", &numbers{Delta: math.MaxInt8}},
			{"count", "255", &numbers{Count: math.MaxUint8}},
			{"large", "-9223372036854775808", &numbers{Large: math.MinInt64}},
			{"large", "9223372036854775807", &numbers{Large: math.MaxInt64}},
			{"ids", "[9007199254740993]", &numbers{IDs: []uint64{9007199254740993}}},
		} {
			t.Run(format.ext+"/"+tc.field+"/"+tc.value, func(t *testing.T) {
				file := configFile(t, format.ext, fmt.Sprintf(format.pattern, tc.field, tc.value))
				got, err := Parse[numbers](file)
				if tc.want == nil {
					if err == nil || got != nil || !strings.Contains(err.Error(), tc.field) {
						t.Fatalf("越界应返回字段错误：got=%+v, err=%v", got, err)
					}
					return
				}
				if err != nil || !reflect.DeepEqual(got, tc.want) {
					t.Fatalf("整数边界解析错误：got=%+v, want=%+v, err=%v", got, tc.want, err)
				}
			})
		}
	}
}

func TestParseMaxUint64(t *testing.T) {
	// TOML 的整数范围止于 MaxInt64；YAML 和 JSON 还能表达到 MaxUint64。
	for _, tc := range []struct{ ext, content string }{
		{".yaml", "value: 18446744073709551615\n"},
		{".json", `{"value":18446744073709551615}`},
	} {
		t.Run(tc.ext, func(t *testing.T) {
			file := configFile(t, tc.ext, tc.content)
			unsigned, err := Parse[struct{ Value uint64 }](file)
			if err != nil || unsigned.Value != math.MaxUint64 {
				t.Fatalf("uint64 最大值应原样保留：got=%+v, err=%v", unsigned, err)
			}
			signed, err := Parse[struct{ Value *int64 }](file)
			if err == nil || signed != nil {
				t.Fatalf("uint64 最大值转为 int64 应报错：got=%+v, err=%v", signed, err)
			}
			narrow, err := Parse[struct{ Value uint32 }](file)
			if err == nil || narrow != nil {
				t.Fatalf("uint64 最大值转为 uint32 应报错：got=%+v, err=%v", narrow, err)
			}
		})
	}
}

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

func TestParseRejectsCaseConflictsInSameObject(t *testing.T) {
	type config struct {
		RunMode string
		Env     map[string]any
		Nodes   []map[string]string
	}
	for _, tc := range []struct {
		name     string
		ext      string
		content  string
		location string
	}{
		{"root", ".yaml", "runMode: private-value\nRunMode: private-value\n", "root"},
		{"nested", ".toml", "[Env]\nrunMode = 'private-value'\nRunMode = 'other'\n", "Env"},
		{"array", ".json", `{"Nodes":[{},{"runMode":"private-value","RunMode":"other"}]}`, "index 1"},
		{"table-array", ".toml", "[[Nodes]]\nrunMode = 'private-value'\nRunMode = 'other'\n", "index 0"},
		{"mixed-yaml-keys", ".yaml", "Env:\n  1: keep\n  runMode: private-value\n  RunMode: other\n", "Env"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file := configFile(t, tc.ext, tc.content)
			got, err := Parse[config](file)
			if got != nil || err == nil {
				t.Fatal("同一对象的大小写冲突应返回 nil 和错误")
			}
			for _, part := range []string{file, tc.location, "runMode", "RunMode"} {
				if !strings.Contains(err.Error(), part) {
					t.Fatalf("冲突错误缺少 %q: %v", part, err)
				}
			}
			if strings.Contains(err.Error(), "private-value") {
				t.Fatalf("冲突错误泄露了配置值: %v", err)
			}
		})
	}
}

func TestLoadCanRetryAfterCaseConflict(t *testing.T) {
	type config struct {
		Nodes []struct{ RunMode string }
		Label string `mapstructure:"nodes.runMode"`
	}
	name := configName(t)
	file := configFile(t, ".json", `{"Nodes":[{"RunMode":"one","runMode":"two"}]}`)
	if err := Load[config](name, file); err == nil {
		t.Fatal("加载应拒绝同层大小写冲突")
	}
	// 两个数组元素各自有独立的键空间，带点的字面键也不参与路径展开。
	const valid = `{"Nodes":[{"RunMode":"one"},{"runMode":"two"}],"nodes.runMode":"literal"}`
	if err := os.WriteFile(file, []byte(valid), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Load[config](name, file); err != nil {
		t.Fatal(err)
	}
	got, err := Get[config](name)
	if err != nil || len(got.Nodes) != 2 || got.Nodes[0].RunMode != "one" || got.Nodes[1].RunMode != "two" || got.Label != "literal" {
		t.Fatalf("修正配置后应能用原名加载: %+v, %v", got, err)
	}
}
