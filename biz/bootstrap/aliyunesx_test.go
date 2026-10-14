package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/aliyun/esx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

const validAliyunESConfig = "region: cn-hangzhou\ninstanceID: es-assets\naccessKeyID: test-id\naccessKeySecret: test-secret\n"

func writeAliyunESConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "aliyunesx."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireAliyunESPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func TestAliyunESDeclarationsAndUnregisteredModule(t *testing.T) {
	t.Run("declarations", func(t *testing.T) {
		isolatedLog(t, func() {
			for _, name := range []string{"", " ", "a/b", "../assets", "a.b", "文件"} {
				requireAliyunESPanic(t, "invalid AliyunESX name", func() { bootstrap.MustRegisterAliyunES(name, true) })
			}
			bootstrap.MustRegisterAliyunES("assets_1-main", true)
			bootstrap.MustRegisterAliyunOSS("assets_1-main", true)
			requireAliyunESPanic(t, "already registered", func() { bootstrap.MustRegisterAliyunES("assets_1-main", false) })
			requireAliyunESPanic(t, "default AliyunESX", func() { bootstrap.MustRegisterAliyunES("other", true) })
			bootstrap.MustRegisterAliyunES("other", false)
			if recoverValue(func() { esx.Default() }) == nil {
				t.Fatal("声明不得创建客户端")
			}
		})
	})
	t.Run("unregistered", func(t *testing.T) {
		isolatedLog(t, func() {
			cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
			writeAliyunESConfig(t, "unused", "invalid: [")
			var resources lifecycle.Stack
			defer resources.Close()
			bootstrap.MustBaseInit(cfg, &resources)
			requireAliyunESPanic(t, "initialization has started", func() { bootstrap.MustRegisterAliyunES("late", true) })
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := esx.NewNamed(esx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
				t.Fatalf("未声明时不应登记模块关闭: %v", err)
			}
		})
	})
}

func TestAliyunESValidatesAllFilesBeforeCreating(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "aliyunesx.other.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "identity", content: "name: other\n", want: "name"},
		{name: "default", content: "isDefault: true\n", want: "isdefault"},
		{name: "type", content: "logRequests: enabled\n", want: "logRequests"},
		{name: "duration", content: "requestTimeout: later\n", want: "requestTimeout"},
		{name: "instance-type", content: "instanceID: [es-a, es-b]\n", want: "instanceID"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
				writeAliyunESConfig(t, "assets", validAliyunESConfig)
				if tc.name != "missing" {
					writeAliyunESConfig(t, "other", tc.content)
				}
				bootstrap.MustRegisterAliyunES("assets", true)
				bootstrap.MustRegisterAliyunES("other", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireAliyunESPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), filepath.Join(env.ConfigDirPath(), "aliyunesx.other.yaml")) {
					t.Fatalf("配置错误缺少实例路径: %v", err)
				}
				if tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失文件错误链丢失: %v", err)
				}
				if recoverValue(func() { esx.Named("assets") }) == nil {
					t.Fatal("应解析全部文件后才创建首个实例")
				}
			})
		})
	}
}

func TestAliyunESPartialInitializationCleanup(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		writeAliyunESConfig(t, "assets", validAliyunESConfig)
		writeAliyunESConfig(t, "other", strings.ReplaceAll(validAliyunESConfig, "accessKeyID: test-id", "accessKeyID: ''"))
		bootstrap.MustRegisterAliyunES("assets", true)
		bootstrap.MustRegisterAliyunES("other", false)
		var resources lifecycle.Stack
		defer resources.Close()
		err := requireAliyunESPanic(t, `initialize instance "other"`, func() { bootstrap.MustBaseInit(cfg, &resources) })
		if strings.Contains(err.Error(), "test-secret") {
			t.Fatalf("初始化错误包含密钥: %v", err)
		}
		client := esx.Named("assets")
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := client.DescribeInstance(context.Background()); !errors.Is(err, esx.ErrClosed) {
			t.Fatalf("已创建实例未关闭: %v", err)
		}
	})
}

func TestAliyunESTemplateInstancesLoggingAndShutdown(t *testing.T) {
	isolatedLog(t, func() {
		template, err := os.ReadFile("conf.example/aliyunesx.example.yaml")
		if err != nil {
			t.Fatal(err)
		}
		cfg := loadLogConfig(t, "debug", "log:\n  format: json\n  hookStd: false\n")
		var calls atomic.Int32
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			instanceID := strings.TrimPrefix(r.URL.Path, "/openapi/instances/")
			if (instanceID != "es-assets" && instanceID != "es-archive") || r.Method != http.MethodGet || !strings.Contains(r.Header.Get("Authorization"), "Credential="+instanceID+",") {
				t.Errorf("实例目标或凭据错误: %s, %s, %s", r.Method, r.URL.Path, r.Header.Get("Authorization"))
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"RequestId":"rid","Result":{"instanceId":%q,"status":"active","esVersion":"8.17"}}`, instanceID)
		}))
		defer server.Close()
		// 先占用端口供配置引用，完成默认时区初始化后再启动 HTTP goroutine。
		endpoint := "http://" + server.Listener.Addr().String()
		for _, name := range []string{"assets", "archive"} {
			content := strings.NewReplacer(
				"es-example", "es-"+name,
				"<部署时填写 AK>", "es-"+name,
				"<部署时填写 SK>", "test-secret",
				`endpoint: ""`, `endpoint: "`+endpoint+`"`,
				`slowThreshold: "200ms"`, `slowThreshold: "1h"`,
			).Replace(string(template))
			if name == "assets" {
				content = strings.ReplaceAll(content, "logRequests: false", "logRequests: true")
			}
			writeAliyunESConfig(t, name, content)
			bootstrap.MustRegisterAliyunES(name, name == "assets")
		}
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		server.Start()
		if calls.Load() != 0 || esx.Default() != esx.Named("assets") {
			t.Fatal("初始化不得发请求，默认与命名实例必须相同")
		}
		assets, archive := esx.Default(), esx.Named("archive")
		if result, err := archive.DescribeInstance(context.Background()); err != nil || result.InstanceID != "es-archive" {
			t.Fatalf("命名实例路由错误: %+v, %v", result, err)
		}
		if err := resources.Register(func() error {
			logit.Info(context.Background(), "consumer closing")
			_, err := assets.DescribeInstance(context.Background())
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if calls.Load() != 2 {
			t.Fatalf("调用次数错误: %d", calls.Load())
		}
		for _, client := range []*esx.Client{assets, archive} {
			if _, err := client.DescribeInstance(context.Background()); !errors.Is(err, esx.ErrClosed) {
				t.Fatalf("资源栈未关闭阿里云 ES: %v", err)
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
		if record[logit.DownstreamTypeKey] != "AliyunES" || record[logit.DownstreamIDKey] != "assets" || details["instance_id"] != "es-assets" || details["request_id"] != "rid" {
			t.Fatalf("实例日志字段错误: %v", record)
		}
	})
}

func TestAliyunESConfigRequestTimeout(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  hookStd: false\n")
		server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()
		endpoint := "http://" + server.Listener.Addr().String()
		writeAliyunESConfig(t, "assets", validAliyunESConfig+"endpoint: "+endpoint+"\nrequestTimeout: 40ms\n")
		bootstrap.MustRegisterAliyunES("assets", false)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		server.Start()
		if recoverValue(func() { esx.Default() }) == nil {
			t.Fatal("仅命名实例不应隐式设置默认实例")
		}
		_, err := esx.Named("assets").DescribeInstance(context.Background())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("配置的超时未生效: %v", err)
		}
	})
}
