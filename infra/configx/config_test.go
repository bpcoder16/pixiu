package configx

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type serverConfig struct {
	Name string `mapstructure:"server_name"`
	HTTP struct {
		Port    int           `mapstructure:"port"`
		Timeout time.Duration `mapstructure:"timeout"`
	} `mapstructure:"http"`
	Origins []string          `mapstructure:"origins"`
	Labels  map[string]string `mapstructure:"labels"`
	Missing string            `mapstructure:"missing"`
}

func configName(t *testing.T) string {
	t.Helper()
	name := t.Name()
	t.Cleanup(func() {
		configs.Delete(name)
	})
	return name
}

func configFile(t *testing.T, ext, content string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "server"+ext)
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestLoadFormats(t *testing.T) {
	const yaml = "server_name: demo\nhttp:\n  port: 8080\n  timeout: 5s\norigins: [one, two]\nlabels:\n  region: cn\n"
	cases := []struct {
		ext     string
		content string
	}{
		{".yaml", yaml},
		{".yml", yaml},
		{".YAML", yaml},
		{".toml", "server_name = 'demo'\norigins = ['one', 'two']\n[http]\nport = 8080\ntimeout = '5s'\n[labels]\nregion = 'cn'\n"},
		{".JSON", `{"server_name":"demo","http":{"port":8080,"timeout":"5s"},"origins":["one","two"],"labels":{"region":"cn"}}`},
		{".json", `{"server_name":"demo","http":{"port":8080,"timeout":"5s"},"origins":"one,two","labels":{"region":"cn"}}`},
	}
	for _, tc := range cases {
		t.Run(tc.ext, func(t *testing.T) {
			t.Parallel()
			name := configName(t)
			if err := Load[serverConfig](name, configFile(t, tc.ext, tc.content)); err != nil {
				t.Fatal(err)
			}
			got, err := Get[serverConfig](name)
			if err != nil {
				t.Fatal(err)
			}
			if got.Name != "demo" || got.HTTP.Port != 8080 || got.HTTP.Timeout != 5*time.Second ||
				!reflect.DeepEqual(got.Origins, []string{"one", "two"}) ||
				!reflect.DeepEqual(got.Labels, map[string]string{"region": "cn"}) || got.Missing != "" {
				t.Fatalf("配置内容不正确: %+v", got)
			}
		})
	}
}

func TestLoadRejectsWeakTypeConversions(t *testing.T) {
	type typedConfig struct {
		Port    int      `mapstructure:"port"`
		Enabled bool     `mapstructure:"enabled"`
		Name    string   `mapstructure:"name"`
		Alias   *string  `mapstructure:"alias"`
		Origins []string `mapstructure:"origins"`
	}
	formats := []struct {
		ext     string
		pattern string
	}{
		{".yaml", "%s: %s\n"},
		{".toml", "%s = %s\n"},
		{".json", `{"%s":%s}`},
	}
	cases := []struct {
		label string
		key   string
		value string
	}{
		{"string-to-int", "port", `"8080"`},
		{"string-to-bool", "enabled", `"true"`},
		{"bool-to-int", "port", "true"},
		{"int-to-bool", "enabled", "1"},
		{"int-to-string", "name", "123"},
		{"float-to-string", "name", "1.25"},
		{"int-to-string-pointer", "alias", "123"},
		{"int-to-slice", "origins", "123"},
		{"int-in-string-slice", "origins", "[123]"},
	}
	for _, format := range formats {
		for _, tc := range cases {
			t.Run(format.ext+"/"+tc.label, func(t *testing.T) {
				name := configName(t)
				content := fmt.Sprintf(format.pattern, tc.key, tc.value)
				file := configFile(t, format.ext, content)
				if err := Load[typedConfig](name, file); err == nil {
					t.Fatal("应拒绝弱类型转换")
				}
				if got, err := Get[typedConfig](name); got != nil || err == nil {
					t.Fatalf("类型错误后不应注册: %+v, %v", got, err)
				}
			})
		}
	}
}

func TestLoadJSONNumericDurations(t *testing.T) {
	type durationConfig struct {
		Timeout time.Duration   `mapstructure:"timeout"`
		Retry   *time.Duration  `mapstructure:"retry"`
		Delays  []time.Duration `mapstructure:"delays"`
	}
	name := configName(t)
	file := configFile(t, ".json", `{"timeout":1000000000,"retry":2000000000,"delays":[3000000000,"4s"]}`)
	if err := Load[durationConfig](name, file); err != nil {
		t.Fatal(err)
	}
	got, err := Get[durationConfig](name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Timeout != time.Second || got.Retry == nil || *got.Retry != 2*time.Second ||
		!reflect.DeepEqual(got.Delays, []time.Duration{3 * time.Second, 4 * time.Second}) {
		t.Fatalf("数字时长应按纳秒解析，字符串时长应保留原有行为: %+v", got)
	}
}

func TestLoadRejectsUnknownFieldsAndCanRetry(t *testing.T) {
	cases := []struct {
		ext     string
		content string
		valid   string
	}{
		{".yaml", "server_name: demo\nunknown: true\n", "server_name: fixed\n"},
		{".toml", "[http]\nportt = 8080\n", "server_name = 'fixed'\n"},
		{".json", `{"http":{"port":8080,"portt":9090}}`, `{"server_name":"fixed"}`},
		{".yaml", "unknown: null\n", "server_name: fixed\n"},
		{".toml", "[unknown]\n", "server_name = 'fixed'\n"},
		{".json", `{"unknown":{}}`, `{"server_name":"fixed"}`},
		{".json", `{"http":{"unknown":null}}`, `{"server_name":"fixed"}`},
	}
	for _, tc := range cases {
		t.Run(tc.ext, func(t *testing.T) {
			name := configName(t)
			file := configFile(t, tc.ext, tc.content)
			if err := Load[serverConfig](name, file); err == nil || !strings.Contains(err.Error(), file) {
				t.Fatalf("未知字段应返回含文件路径的错误: %v", err)
			}
			if got, err := Get[serverConfig](name); got != nil || err == nil {
				t.Fatalf("失败后不应注册: %+v, %v", got, err)
			}
			if err := os.WriteFile(file, []byte(tc.valid), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := Load[serverConfig](name, file); err != nil {
				t.Fatalf("修正后重试失败: %v", err)
			}
		})
	}
}

func TestLoadFailuresDoNotRegister(t *testing.T) {
	cases := []struct {
		label   string
		ext     string
		content string
	}{
		{"yaml-syntax", ".yaml", "http: ["},
		{"toml-syntax", ".toml", "http = ["},
		{"json-syntax", ".json", `{"http":`},
		{"json-trailing-value", ".json", `{} {}`},
		{"json-trailing-garbage", ".json", `{} invalid`},
		{"json-comment", ".json", "{\"server_name\":\"demo\" // comment\n}"},
		{"field-type", ".yaml", "http:\n  port: invalid\n"},
		{"json5", ".json5", `{"server_name":"demo"}`},
		{"env", ".env", "server_name=demo"},
		{"no-extension", "", `{"server_name":"demo"}`},
	}
	for _, tc := range cases {
		t.Run(tc.label, func(t *testing.T) {
			name := configName(t)
			if err := Load[serverConfig](name, configFile(t, tc.ext, tc.content)); err == nil {
				t.Fatal("应拒绝无效配置")
			}
			if got, err := Get[serverConfig](name); got != nil || err == nil {
				t.Fatalf("失败后不应注册: %+v, %v", got, err)
			}
		})
	}
	name := configName(t)
	file := filepath.Join(t.TempDir(), "missing.yaml")
	if err := Load[serverConfig](name, file); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("应保留文件不存在错误: %v", err)
	}
	if err := Load[serverConfig](name, ""); err == nil {
		t.Fatal("应拒绝空路径")
	}
}

func TestLoadRejectsInvalidNamesAndTypes(t *testing.T) {
	file := configFile(t, ".json", `{}`)
	for _, name := range []string{"", " \t\n"} {
		if err := Load[serverConfig](name, file); err == nil {
			t.Fatalf("应拒绝空名称 %q", name)
		}
	}
	name := configName(t)
	for _, err := range []error{
		Load[*serverConfig](name, file),
		Load[map[string]string](name, file),
		Load[any](name, file),
		Load[string](name, file),
	} {
		if err == nil {
			t.Fatal("应拒绝非结构体类型")
		}
	}
	if err := Load[serverConfig](name, file); err != nil {
		t.Fatalf("类型错误不应占用名称: %v", err)
	}
}

func TestLoadRelativePath(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	if err := os.WriteFile("server.yaml", []byte("server_name: relative\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	name := configName(t)
	if err := Load[serverConfig](name, "./server.yaml"); err != nil {
		t.Fatal(err)
	}
	if got, err := Get[serverConfig](name); err != nil || got.Name != "relative" {
		t.Fatalf("相对路径应基于工作目录: %+v, %v", got, err)
	}
}

func TestRegistrationSharesPointerAndRejectsReplacement(t *testing.T) {
	name := configName(t)
	file := configFile(t, ".json", `{"server_name":"first"}`)
	if err := Load[serverConfig](name, file); err != nil {
		t.Fatal(err)
	}
	first, err := Get[serverConfig](name)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(file, []byte(`{"server_name":"second"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Load[serverConfig](name, file); err == nil {
		t.Fatal("重复加载不应成功")
	}
	if err := Load[struct{}](name, configFile(t, ".json", `{}`)); err == nil {
		t.Fatal("不同类型不应占用同名配置")
	}
	if got, err := Get[struct{}](name); got != nil || err == nil {
		t.Fatalf("应报告类型不匹配: %+v, %v", got, err)
	}
	if err := os.Remove(file); err != nil {
		t.Fatal(err)
	}
	second, err := Get[serverConfig](name)
	if err != nil || second != first || second.Name != "first" {
		t.Fatalf("应返回原来的共享对象: %+v, %v", second, err)
	}
	first.Name = "changed"
	if got := GetOrZero[serverConfig](name); got != first || got.Name != "changed" {
		t.Fatalf("成功获取应返回共享对象: %+v", got)
	}
}

func TestGetOrZeroDoesNotShareOrRegisterFallback(t *testing.T) {
	name := configName(t)
	first := GetOrZero[serverConfig](name)
	if first == nil || !reflect.DeepEqual(*first, serverConfig{}) {
		t.Fatalf("应返回结构体零值: %+v", first)
	}
	first.Name = "changed"
	first.Labels = map[string]string{"key": "value"}
	second := GetOrZero[serverConfig](name)
	if second == first || !reflect.DeepEqual(*second, serverConfig{}) {
		t.Fatalf("兜底对象不应共享: %+v", second)
	}
	if err := Load[struct{ Port int }](name, configFile(t, ".json", `{"port":8080}`)); err != nil {
		t.Fatalf("兜底不应占用名称: %v", err)
	}
	wrong := GetOrZero[serverConfig](name)
	if wrong == nil || !reflect.DeepEqual(*wrong, serverConfig{}) {
		t.Fatalf("类型不匹配应返回零值: %+v", wrong)
	}
	if got, err := Get[struct{ Port int }](name); err != nil || got.Port != 8080 {
		t.Fatalf("类型不匹配不应改变已注册配置: %+v, %v", got, err)
	}
}

func TestConcurrentLoadPublishesOneCompleteConfig(t *testing.T) {
	name := configName(t)
	file := configFile(t, ".json", `{"server_name":"ready","http":{"port":8080}}`)
	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var readers sync.WaitGroup
	for range workers {
		go func() {
			<-start
			results <- Load[serverConfig](name, file)
		}()
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				got, err := Get[serverConfig](name)
				if err == nil && (got.Name != "ready" || got.HTTP.Port != 8080) {
					t.Errorf("读到未完成的配置: %+v", got)
				}
			}
		}()
	}
	close(start)
	successes := 0
	for range workers {
		if err := <-results; err == nil {
			successes++
		}
	}
	readers.Wait()
	if successes != 1 {
		t.Fatalf("成功注册次数 = %d，期望 1", successes)
	}
	if got, err := Get[serverConfig](name); err != nil || got.Name != "ready" {
		t.Fatalf("最终配置不可用: %+v, %v", got, err)
	}
}

func TestLoadJSONPreservesIntegerPrecision(t *testing.T) {
	type numbers struct {
		ID  uint64      `mapstructure:"id"`
		Raw json.Number `mapstructure:"raw"`
	}
	for _, id := range []uint64{9007199254740993, 18446744073709551615} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			name := configName(t)
			if err := Load[numbers](name, configFile(t, ".json", fmt.Sprintf(`{"id":%d,"raw":%d}`, id, id))); err != nil {
				t.Fatal(err)
			}
			got, err := Get[numbers](name)
			if err != nil || got.ID != id || got.Raw.String() != fmt.Sprint(id) {
				t.Fatalf("整数精度丢失: %+v, %v", got, err)
			}
		})
	}
}

func TestLoadPreservesEmptyObjectsAndLiteralKeys(t *testing.T) {
	type sourceConfig struct {
		Optional *struct{ Enabled bool } `mapstructure:"optional"`
		Values   map[string]string       `mapstructure:"values"`
		Literal  string                  `mapstructure:"literal.key"`
	}
	cases := []struct {
		ext     string
		content string
	}{
		{".yaml", "optional: {}\nvalues: {}\nliteral.key: kept\n"},
		{".toml", "\"literal.key\" = 'kept'\n[optional]\n[values]\n"},
		{".json", `{"optional":{},"values":{},"literal.key":"kept"}`},
	}
	for _, tc := range cases {
		t.Run(tc.ext, func(t *testing.T) {
			name := configName(t)
			if err := Load[sourceConfig](name, configFile(t, tc.ext, tc.content)); err != nil {
				t.Fatal(err)
			}
			got, err := Get[sourceConfig](name)
			if err != nil || got.Optional == nil || got.Values == nil || got.Literal != "kept" {
				t.Fatalf("空对象或字面键名被丢弃: %+v, %v", got, err)
			}
		})
	}
}
