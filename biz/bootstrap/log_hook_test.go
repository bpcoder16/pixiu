//go:build darwin || linux

package bootstrap_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func TestLogHookStd(t *testing.T) {
	const marker = "PIXIU_BOOTSTRAP_HOOK_DIR"
	if dir := os.Getenv(marker); dir != "" {
		setting := os.Getenv("PIXIU_BOOTSTRAP_HOOK_SETTING")
		logYAML := "log:\n  format: json\n  dir: " + dir + "\n"
		if setting != "" {
			logYAML += "  hookStd: " + setting + "\n"
		}
		cfg := loadLogConfig(t, "release", logYAML)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		// 初始化已关闭原始 Writer，复制到 fd 1/2 的句柄仍应持续接收输出。
		fmt.Println("hook stdout message")
		// 直接写 fd 2，不依赖测试框架对 os.Stderr 的绑定。
		if _, err := syscall.Write(2, []byte("hook stderr message\n")); err != nil {
			t.Fatal(err)
		}
		logit.Info(context.Background(), "structured message")
		if err := resources.Register(func() error {
			_, err := fmt.Println("hook resource closing")
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("PIXIU_BOOTSTRAP_HOOK_PANIC") == "true" {
			panic("hook unrecovered panic")
		}
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		fmt.Println("hook stdout after close")
		if _, err := syscall.Write(2, []byte("hook stderr after close\n")); err != nil {
			t.Fatal(err)
		}
		return
	}
	for _, tc := range []struct {
		name     string
		setting  string
		existing bool
		panic    bool
	}{
		{name: "default"},
		{name: "enabled", setting: "true", existing: true},
		{name: "disabled", setting: "false"},
		{name: "panic-after-defer", panic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			stdDir := filepath.Join(dir, "bootstrap-test", "std")
			if tc.existing {
				if err := os.MkdirAll(stdDir, 0o755); err != nil {
					t.Fatal(err)
				}
				for _, stream := range []string{"stdout", "stderr"} {
					path := filepath.Join(stdDir, "bootstrap-test."+stream+".log")
					if err := os.WriteFile(path, []byte("existing "+stream+"\n"), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestLogHookStd$")
			cmd.Env = append(os.Environ(), marker+"="+dir,
				"PIXIU_BOOTSTRAP_HOOK_SETTING="+tc.setting,
				fmt.Sprintf("PIXIU_BOOTSTRAP_HOOK_PANIC=%t", tc.panic))
			var output, errorOutput bytes.Buffer
			cmd.Stdout = &output
			cmd.Stderr = &errorOutput
			err := cmd.Run()
			if tc.panic {
				var exitErr *exec.ExitError
				if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
					t.Fatalf("未捕获 panic 应退出: %v, stdout=%s, stderr=%s", err, output.Bytes(), errorOutput.Bytes())
				}
			} else if err != nil {
				t.Fatalf("日志劫持子进程失败: %v, stdout=%s, stderr=%s", err, output.Bytes(), errorOutput.Bytes())
			}
			if tc.setting == "false" {
				if _, err := os.Stat(stdDir); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("关闭劫持不应创建 std 目录: %v", err)
				}
			}
			for _, stream := range []struct {
				name     string
				original string
				other    string
			}{
				{name: "stdout", original: output.String(), other: "stderr"},
				{name: "stderr", original: errorOutput.String(), other: "stdout"},
			} {
				data := stream.original
				if tc.setting != "false" {
					path := filepath.Join(stdDir, "bootstrap-test."+stream.name+".log")
					content, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					data = string(content)
					if tc.existing && !strings.HasPrefix(data, "existing "+stream.name+"\n") {
						t.Fatalf("劫持应追加到对应文件: %s", data)
					}
					if strings.Contains(stream.original, "hook ") {
						t.Fatalf("劫持后输出仍流向原 %s: %s", stream.name, stream.original)
					}
				}
				messages := []string{"hook " + stream.name + " message\n"}
				if stream.name == "stdout" {
					messages = append(messages, "hook resource closing\n")
				}
				if !tc.panic {
					messages = append(messages, "hook "+stream.name+" after close\n")
				} else if stream.name == "stderr" {
					messages = append(messages, "panic: hook unrecovered panic", "goroutine ")
				}
				for _, message := range messages {
					if !strings.Contains(data, message) {
						t.Fatalf("%s 输出缺失 %q: %s", stream.name, message, data)
					}
				}
				if strings.Contains(data, "hook "+stream.other+" ") || strings.Contains(data, "structured message") {
					t.Fatalf("%s 不应混入其他日志: %s", stream.name, data)
				}
			}
			info, err := os.ReadFile(filepath.Join(dir, "bootstrap-test", "biz", "bootstrap-test.info.log"))
			if err != nil || !strings.Contains(string(info), `"msg":"structured message"`) {
				t.Fatalf("结构化日志应继续单独分流: info=%s, err=%v", info, err)
			}
		})
	}
}

func TestLogHookOpenFailureDoesNotPublishLoggers(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  names: [worker]\n")
				dir := filepath.Join(env.RootDirPath(), "log", env.AppName())
				path := filepath.Join(dir, "std", "bootstrap-test."+stream+".log")
				if err := os.MkdirAll(path, 0o755); err != nil {
					t.Fatal(err)
				}
				previous := logit.Default()
				var resources lifecycle.Stack
				defer resources.Close()
				value := recoverValue(func() { bootstrap.MustBaseInit(cfg, &resources) })
				if value == nil || !strings.Contains(fmt.Sprint(value), path) || logit.Default() != previous || logit.Named("worker") != previous {
					t.Fatalf("劫持文件打开失败应终止初始化且不发布 Logger: %v", value)
				}
				if stream == "stderr" {
					// stderr 打开失败时，stdout 原始 Writer 应关闭，且尚未重定向 fd 1。
					requireLogFileOpen(t, filepath.Join(dir, "std", "bootstrap-test.stdout.log"), false)
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				for _, base := range []string{"bootstrap-test", "bootstrap-test.worker"} {
					for _, suffix := range []string{".info", ".debug", ".wf"} {
						requireLogFileOpen(t, filepath.Join(dir, "biz", base+suffix+".log"), false)
					}
				}
			})
		})
	}
}
