package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

const validAliyunOSSConfig = "region: cn-hangzhou\nbucket: assets-bucket\naccessKeyID: test-id\naccessKeySecret: test-secret\n"

func writeAliyunOSSConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "aliyunossx."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireAliyunOSSPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func TestAliyunOSSDeclarationsAndUnregisteredModule(t *testing.T) {
	t.Run("declarations", func(t *testing.T) {
		isolatedLog(t, func() {
			for _, name := range []string{"", " ", "a/b", "../assets", "a.b", "文件"} {
				requireAliyunOSSPanic(t, "invalid AliyunOSSX name", func() { bootstrap.MustRegisterAliyunOSS(name, true) })
			}
			bootstrap.MustRegisterAliyunOSS("assets_1-main", true)
			requireAliyunOSSPanic(t, "already registered", func() { bootstrap.MustRegisterAliyunOSS("assets_1-main", false) })
			requireAliyunOSSPanic(t, "default AliyunOSSX", func() { bootstrap.MustRegisterAliyunOSS("other", true) })
			bootstrap.MustRegisterAliyunOSS("other", false)
			if recoverValue(func() { ossx.Default() }) == nil {
				t.Fatal("声明不得创建客户端")
			}
		})
	})
	t.Run("unregistered", func(t *testing.T) {
		isolatedLog(t, func() {
			cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
			writeAliyunOSSConfig(t, "unused", "invalid: [")
			var resources lifecycle.Stack
			defer resources.Close()
			bootstrap.MustBaseInit(cfg, &resources)
			requireAliyunOSSPanic(t, "initialization has started", func() { bootstrap.MustRegisterAliyunOSS("late", true) })
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := ossx.NewNamed(ossx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
				t.Fatalf("未声明时不应登记模块关闭: %v", err)
			}
		})
	})
}

func TestAliyunOSSValidatesAllFilesBeforeCreating(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "aliyunossx.other.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "identity", content: "name: other\n", want: "name"},
		{name: "default", content: "isDefault: true\n", want: "isdefault"},
		{name: "type", content: "logRequests: enabled\n", want: "logRequests"},
		{name: "duration", content: "requestTimeout: later\n", want: "requestTimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				writeAliyunOSSConfig(t, "assets", validAliyunOSSConfig)
				if tc.name != "missing" {
					writeAliyunOSSConfig(t, "other", tc.content)
				}
				bootstrap.MustRegisterAliyunOSS("assets", true)
				bootstrap.MustRegisterAliyunOSS("other", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireAliyunOSSPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), filepath.Join(env.ConfigDirPath(), "aliyunossx.other.yaml")) {
					t.Fatalf("配置错误缺少实例路径: %v", err)
				}
				if tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失文件错误链丢失: %v", err)
				}
				if recoverValue(func() { ossx.Named("assets") }) == nil {
					t.Fatal("应解析全部文件后才创建首个实例")
				}
			})
		})
	}
}

func TestAliyunOSSPartialInitializationCleanup(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		writeAliyunOSSConfig(t, "assets", validAliyunOSSConfig)
		writeAliyunOSSConfig(t, "other", strings.ReplaceAll(validAliyunOSSConfig, "accessKeyID: test-id", "accessKeyID: ''"))
		bootstrap.MustRegisterAliyunOSS("assets", true)
		bootstrap.MustRegisterAliyunOSS("other", false)
		var resources lifecycle.Stack
		defer resources.Close()
		err := requireAliyunOSSPanic(t, `initialize instance "other"`, func() { bootstrap.MustBaseInit(cfg, &resources) })
		if strings.Contains(err.Error(), "test-secret") {
			t.Fatalf("初始化错误包含密钥: %v", err)
		}
		client := ossx.Named("assets")
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := client.PutObject(context.Background(), ossx.PutObjectRequest{}); !errors.Is(err, ossx.ErrClosed) {
			t.Fatalf("已创建实例未关闭: %v", err)
		}
	})
}

func TestAliyunOSSTemplateInstancesLoggingAndShutdown(t *testing.T) {
	isolatedLog(t, func() {
		template, err := os.ReadFile("conf.example/aliyunossx.example.yaml")
		if err != nil {
			t.Fatal(err)
		}
		cfg := loadLogConfig(t, "debug", "log:\n  format: json\n  hookStd: false\n")
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || string(body) != "data" || r.Method != http.MethodPut {
				t.Errorf("上传正文或方法错误: %q, %s, %v", body, r.Method, err)
			}
			bucket := strings.Split(r.URL.Path, "/")[1]
			if (bucket != "assets-bucket" && bucket != "archive-bucket") || !strings.Contains(r.Header.Get("Authorization"), "Credential="+bucket+"/") {
				t.Errorf("实例 Bucket 或凭据错误: %s, %s", r.URL.Path, r.Header.Get("Authorization"))
			}
			w.Header().Set("x-oss-request-id", "rid")
		}))
		defer server.Close()
		for _, name := range []string{"assets", "archive"} {
			content := strings.NewReplacer(
				"example-assets", name+"-bucket",
				"<部署时填写 AK>", name+"-bucket",
				"<部署时填写 SK>", "test-secret",
				`endpoint: ""`, `endpoint: "`+server.URL+`"`,
				`slowThreshold: "500ms"`, `slowThreshold: "1h"`,
			).Replace(string(template))
			if name == "assets" {
				content = strings.ReplaceAll(content, "logRequests: false", "logRequests: true")
			}
			writeAliyunOSSConfig(t, name, content)
			bootstrap.MustRegisterAliyunOSS(name, name == "assets")
		}
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		if calls.Load() != 0 || ossx.Default() != ossx.Named("assets") {
			t.Fatal("初始化不得发请求，默认与命名实例必须相同")
		}
		assets, archive := ossx.Default(), ossx.Named("archive")
		path := filepath.Join(t.TempDir(), "data.txt")
		if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := archive.PutFile(context.Background(), ossx.PutFileRequest{
			Key:      "archive.txt",
			FilePath: path,
		}); err != nil {
			t.Fatal(err)
		}
		if err := resources.Register(func() error {
			logit.Info(context.Background(), "consumer closing")
			_, err := assets.PutObject(context.Background(), ossx.PutObjectRequest{
				Key:  "assets.txt",
				Body: strings.NewReader("data"),
			})
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("上传次数错误: %d", calls.Load())
		}
		for _, client := range []*ossx.Client{assets, archive} {
			if _, err := client.PutObject(context.Background(), ossx.PutObjectRequest{}); !errors.Is(err, ossx.ErrClosed) {
				t.Fatalf("资源栈未关闭 OSS: %v", err)
			}
		}
		data, err := os.ReadFile(filepath.Join(env.RootDirPath(), "log/bootstrap-test/biz/info.log"))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		if len(lines) != 2 || !strings.Contains(lines[0], "consumer closing") || strings.Contains(string(data), "test-secret") {
			t.Fatalf("日志开关或关闭顺序错误: %s", data)
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(lines[1]), &record); err != nil {
			t.Fatal(err)
		}
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		if record[logit.DownstreamIDKey] != "assets" || details["bucket"] != "assets-bucket" || details["request_id"] != "rid" {
			t.Fatalf("实例日志字段错误: %v", record)
		}
	})
}
