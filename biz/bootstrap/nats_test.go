package bootstrap_test

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func writeNATSConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "nats."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func requireNATSPanic(t *testing.T, want string, fn func()) error {
	t.Helper()
	value := recoverValue(fn)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 且包含 %q，实际: %v", want, value)
	}
	return err
}

func TestNATSDeclarationsAndUnregisteredModule(t *testing.T) {
	t.Run("declarations", func(t *testing.T) {
		isolatedLog(t, func() {
			for _, name := range []string{"", " ", "a/b", "../events", "a.b", "消息"} {
				requireNATSPanic(t, "invalid NATS name", func() { bootstrap.MustRegisterNATS(name, true) })
			}
			bootstrap.MustRegisterNATS("events_1-main", true)
			requireNATSPanic(t, "already registered", func() { bootstrap.MustRegisterNATS("events_1-main", false) })
			requireNATSPanic(t, "default NATS", func() { bootstrap.MustRegisterNATS("other", true) })
			bootstrap.MustRegisterNATS("other", false)
			if recoverValue(func() { natsx.Default() }) == nil {
				t.Fatal("声明不得创建客户端")
			}
		})
	})
	t.Run("unregistered", func(t *testing.T) {
		isolatedLog(t, func() {
			cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
			writeNATSConfig(t, "unused", "invalid: [")
			var resources lifecycle.Stack
			defer resources.Close()
			bootstrap.MustBaseInit(cfg, &resources)
			requireNATSPanic(t, "initialization has started", func() { bootstrap.MustRegisterNATS("late", true) })
			if err := resources.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := natsx.NewNamed(natsx.Config{Name: "probe"}); err == nil || strings.Contains(err.Error(), "clients closed") {
				t.Fatalf("未声明时不应登记模块关闭: %v", err)
			}
		})
	})
}

func TestNATSValidatesAllFilesBeforeConnecting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing", want: "nats.other.yaml"},
		{name: "unknown", content: "typo: true\n", want: "typo"},
		{name: "identity", content: "name: other\n", want: "name"},
		{name: "default", content: "isDefault: true\n", want: "isdefault"},
		{name: "nested", content: "jetStream:\n  typo: true\n", want: "typo"},
		{
			name:    "namespace-type",
			content: "namespace: 42\n",
			want:    "namespace",
		},
		{name: "type", content: "urls: [false]\n", want: "urls"},
		{name: "duration", content: "connectTimeout: later\n", want: "connectTimeout"},
		{name: "invalid-url", content: "urls: ['nats://host:%zz']\n", want: "urls"},
		{name: "credentials", content: "urls: ['nats://user:secret@localhost:4222']\n", want: "credentials"},
		{name: "bare-credentials", content: "urls: ['user:secret@localhost:4222']\n", want: "credentials"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				// 首个配置合法但不可连接；必须先发现第二个文件的配置错误。
				writeNATSConfig(t, "events", "urls: [nats://127.0.0.1:1]\n")
				if tc.name != "missing" {
					writeNATSConfig(t, "other", tc.content)
				}
				bootstrap.MustRegisterNATS("events", true)
				bootstrap.MustRegisterNATS("other", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireNATSPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), filepath.Join(env.ConfigDirPath(), "nats.other.yaml")) {
					t.Fatalf("未先检查全部配置或缺少实例路径: %v", err)
				}
				if tc.name == "missing" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失文件错误链丢失: %v", err)
				}
			})
		})
	}
}

func TestNATSClientValidationRunsDuringInitialization(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{
			name:    "namespace",
			content: "urls: [localhost:4222]\nnamespace: bad.namespace\n",
			want:    "natsx: namespace",
		},
		{name: "empty", content: "urls: []\n", want: "natsx: no server URLs"},
		{name: "empty-address", content: "urls: ['']\n", want: "natsx: empty server URL"},
		{name: "password", content: "urls: [localhost:4222]\npassword: secret\n", want: "natsx: password requires username"},
		{name: "connect-timeout", content: "urls: [localhost:4222]\nconnectTimeout: -1s\n", want: "natsx: negative timeout"},
		{name: "drain-timeout", content: "urls: [localhost:4222]\ndrainTimeout: -1s\n", want: "natsx: negative timeout"},
		{name: "close-timeout", content: "urls: [localhost:4222]\ncloseTimeout: -1s\n", want: "natsx: negative timeout"},
		{name: "async-timeout", content: "urls: [localhost:4222]\njetStream:\n  publishAsyncTimeout: -1s\n", want: "natsx: negative JetStream.PublishAsyncTimeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				writeNATSConfig(t, "events", tc.content)
				bootstrap.MustRegisterNATS("events", true)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireNATSPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), `initialize instance "events"`) || errors.Is(err, natsx.ErrConnect) {
					t.Fatalf("通用参数应交给 natsx 在建连前校验: %v", err)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := natsx.NewNamed(natsx.Config{Name: "probe"}); err == nil || !strings.Contains(err.Error(), "clients closed") {
					t.Fatalf("客户端参数校验失败后应执行已登记的 CloseAll: %v", err)
				}
			})
		})
	}
}
