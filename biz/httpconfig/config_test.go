package httpconfig_test

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/infra/env"
)

const validYAML = "env:\n  appName: demo\n  runMode: debug\n  timeLocation: Asia/Shanghai\n  localIP: 192.0.2.10\n"

// 配置注册和环境发布均为进程生命周期状态，通过子进程隔离启动场景。
func isolated(t *testing.T, test func(*testing.T)) {
	t.Helper()
	const marker = "PIXIU_HTTPCONFIG_TEST_PROCESS"
	if os.Getenv(marker) == t.Name() {
		test(t)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("独立进程测试失败: %v\n%s", err, output)
	}
}

func panicValue(fn func()) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func writeConfig(t *testing.T, dir, name, content string) string {
	t.Helper()
	file := filepath.Join(dir, name)
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func requireUninitialized(t *testing.T) {
	t.Helper()
	for _, get := range []func(){
		func() { env.AppName() },
		func() { env.RunMode() },
		func() { env.TimeLocation() },
		func() { env.ConfigDirPath() },
		func() { env.LocalIP() },
		func() { env.RootDirPath() },
	} {
		if value := panicValue(get); value == nil || !strings.Contains(fmt.Sprint(value), "not initialized") {
			t.Fatalf("未初始化的环境读取应 panic: %v", value)
		}
	}
}

func TestEnvironmentBeforeLoad(t *testing.T) {
	isolated(t, requireUninitialized)
}

func TestMustLoadAppConfigFormatsAndPaths(t *testing.T) {
	cases := []struct {
		name    string
		content string
		mode    string
	}{
		{"app.yaml", validYAML, "debug"},
		{"app.toml", "[env]\nappName = 'demo'\nrunMode = 'test'\ntimeLocation = 'Asia/Shanghai'\nlocalIP = '192.0.2.10'\n", "test"},
		{"app.JSON", `{"env":{"appName":"demo","runMode":"release","timeLocation":"Asia/Shanghai","localIP":"192.0.2.10"}}`, "release"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolated(t, func(t *testing.T) {
				dir := t.TempDir()
				file := writeConfig(t, dir, tc.name, tc.content)
				t.Chdir(dir)
				input := file
				if tc.name == "app.yaml" {
					input = "./app.yaml"
				}
				local := time.Local
				workingDir, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				cfg := httpconfig.MustLoadAppConfig(input)
				if env.RootDirPath() != workingDir {
					t.Fatalf("工作目录快照不正确: got %q, want %q", env.RootDirPath(), workingDir)
				}
				if cfg.Env.AppName != "demo" || cfg.Env.RunMode != tc.mode || cfg.Env.TimeLocation != "Asia/Shanghai" || cfg.Env.LocalIP != "192.0.2.10" {
					t.Fatalf("返回的配置不正确: %+v", cfg)
				}
				if env.AppName() != "demo" || env.RunMode() != tc.mode || env.TimeLocation().String() != "Asia/Shanghai" || env.LocalIP() != cfg.Env.LocalIP {
					t.Fatal("全局环境不正确")
				}
				_, offset := time.Date(2026, 10, 9, 0, 0, 0, 0, env.TimeLocation()).Zone()
				if offset != 8*60*60 {
					t.Fatalf("时区未正确解析: %d", offset)
				}
				absolute, err := filepath.Abs(input)
				if err != nil {
					t.Fatal(err)
				}
				if env.ConfigDirPath() != filepath.Dir(absolute) || cfg.Env.ConfigDirPath != env.ConfigDirPath() || !filepath.IsAbs(env.ConfigDirPath()) {
					t.Fatalf("配置目录不正确: %s", env.ConfigDirPath())
				}
				if time.Local != local {
					t.Fatal("加载配置不应修改进程时区")
				}
			})
		})
	}
}

func TestMustLoadAppConfigRejectsInvalidEnvironment(t *testing.T) {
	cases := []struct {
		name    string
		content string
		field   string
	}{
		{"missing-env", "{}", "env.appName"},
		{"blank-name", strings.Replace(validYAML, "demo", "'   '", 1), "env.appName"},
		{"missing-mode", strings.Replace(validYAML, "  runMode: debug\n", "", 1), "env.runMode"},
		{"invalid-mode", strings.Replace(validYAML, "debug", "development", 1), "env.runMode"},
		{"missing-location", strings.Replace(validYAML, "  timeLocation: Asia/Shanghai\n", "", 1), "env.timeLocation"},
		{"invalid-location", strings.Replace(validYAML, "Asia/Shanghai", "Invalid/Timezone", 1), "env.timeLocation"},
		{"invalid-ip", strings.Replace(validYAML, "192.0.2.10", "not-an-ip", 1), "env.localIP"},
		{"unknown-field", validYAML + "  appNmae: wrong\n", "appnmae"},
		{"unknown-null", validYAML + "  extra: null\n", "extra"},
		{"unknown-log-field", validYAML + "log:\n  formatt: json\n", "formatt"},
		{"removed-log-mode", validYAML + "log:\n  mode: file\n", "mode"},
		{"wrong-log-names-type", validYAML + "log:\n  names: [123]\n", "names"},
		{"wrong-type", strings.Replace(validYAML, "demo", "123", 1), "appName"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolated(t, func(t *testing.T) {
				file := writeConfig(t, t.TempDir(), "app.yaml", tc.content)
				local := time.Local
				value := panicValue(func() { httpconfig.MustLoadAppConfig(file) })
				message := fmt.Sprint(value)
				if value == nil || !strings.Contains(message, file) || !strings.Contains(strings.ToLower(message), strings.ToLower(tc.field)) {
					t.Fatalf("错误应包含文件路径及字段: %v", value)
				}
				requireUninitialized(t)
				if time.Local != local {
					t.Fatal("失败不应修改进程时区")
				}
			})
		})
	}
}

func TestMustLoadAppConfigReadFailure(t *testing.T) {
	isolated(t, func(t *testing.T) {
		if value := panicValue(func() { httpconfig.MustLoadAppConfig("") }); value == nil {
			t.Fatal("空路径应 panic")
		}
		file := filepath.Join(t.TempDir(), "app.yaml")
		value := panicValue(func() { httpconfig.MustLoadAppConfig(file) })
		err, ok := value.(error)
		if !ok || !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), file) {
			t.Fatalf("应保留文件不存在错误及路径: %v", value)
		}
		requireUninitialized(t)
	})
}

func TestEnvironmentSnapshotAndDuplicateLoad(t *testing.T) {
	isolated(t, func(t *testing.T) {
		dir := t.TempDir()
		file := writeConfig(t, dir, "app.yaml", validYAML)
		cfg := httpconfig.MustLoadAppConfig(file)
		location, configDir, localIP := env.TimeLocation(), env.ConfigDirPath(), env.LocalIP()
		cfg.Env = env.Config{
			AppName:      "changed",
			RunMode:      "release",
			TimeLocation: "UTC",
		}
		other := writeConfig(t, dir, "other.yaml", strings.Replace(validYAML, "demo", "other", 1))
		for _, path := range []string{file, other} {
			if value := panicValue(func() { httpconfig.MustLoadAppConfig(path) }); value == nil {
				t.Fatal("重复加载应 panic")
			}
		}
		if env.AppName() != "demo" || env.RunMode() != "debug" || env.TimeLocation() != location || env.ConfigDirPath() != configDir || env.LocalIP() != localIP {
			t.Fatal("配置修改或重复加载不应改变环境快照")
		}
	})
}

func TestConcurrentLoadAndRead(t *testing.T) {
	isolated(t, func(t *testing.T) {
		file := writeConfig(t, t.TempDir(), "app.yaml", validYAML)
		const workers = 12
		start := make(chan struct{})
		results := make(chan any, workers)
		var readers sync.WaitGroup
		for range workers {
			go func() {
				<-start
				results <- panicValue(func() { httpconfig.MustLoadAppConfig(file) })
			}()
			readers.Add(1)
			go func() {
				defer readers.Done()
				<-start
				for range 100 {
					value := panicValue(func() {
						if env.AppName() != "demo" || env.RunMode() != "debug" || env.TimeLocation().String() != "Asia/Shanghai" || env.ConfigDirPath() != filepath.Dir(file) || env.LocalIP() != "192.0.2.10" {
							t.Error("读到了不完整的环境")
						}
					})
					if value != nil && !strings.Contains(fmt.Sprint(value), "not initialized") {
						t.Errorf("环境读取异常: %v", value)
					}
				}
			}()
		}
		close(start)
		successes := 0
		for range workers {
			if <-results == nil {
				successes++
			}
		}
		readers.Wait()
		if successes != 1 || env.AppName() != "demo" {
			t.Fatalf("应仅一次成功并发布完整环境，成功次数: %d", successes)
		}
	})
}

func TestExampleTemplate(t *testing.T) {
	isolated(t, func(t *testing.T) {
		cfg := httpconfig.MustLoadAppConfig("conf.example/app.yaml")
		if cfg.Env.AppName != "example-http" || cfg.Env.RunMode != "debug" || cfg.Env.TimeLocation != "Asia/Shanghai" {
			t.Fatalf("模板配置不正确: %+v", cfg)
		}
	})
}

func TestLogConfigurationFormats(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
	}{
		{
			name:    "app.yaml",
			content: validYAML + "log:\n  format: json\n  file: logs\n  names: [worker, request]\n  rotate:\n    every: 24h\n    maxFiles: 7\n",
		},
		{
			name:    "app.toml",
			content: "[env]\nappName = 'demo'\nrunMode = 'debug'\ntimeLocation = 'UTC'\nlocalIP = '192.0.2.10'\n[log]\nformat = 'json'\nfile = 'logs'\nnames = ['worker', 'request']\n[log.rotate]\nevery = '24h'\nmaxFiles = 7\n",
		},
		{
			name:    "app.json",
			content: `{"env":{"appName":"demo","runMode":"debug","timeLocation":"UTC","localIP":"192.0.2.10"},"log":{"format":"json","file":"logs","names":["worker","request"],"rotate":{"every":"24h","maxFiles":7}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolated(t, func(t *testing.T) {
				dir := t.TempDir()
				t.Chdir(dir)
				cfg := httpconfig.MustLoadAppConfig(writeConfig(t, dir, tc.name, tc.content))
				if cfg.Log.Format != "json" || cfg.Log.File != "logs" || !slices.Equal(cfg.Log.Names, []string{"worker", "request"}) || cfg.Log.Rotate.Every != 24*time.Hour || cfg.Log.Rotate.MaxFiles != 7 {
					t.Fatalf("日志配置解析错误: %+v", cfg.Log)
				}
				if _, err := os.Stat("logs"); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("配置加载不应创建日志目录: %v", err)
				}
			})
		})
	}
}

func TestMustLoadDoesNotReplaceExistingEnvironment(t *testing.T) {
	isolated(t, func(t *testing.T) {
		dir := t.TempDir()
		if err := env.Init(env.Config{
			AppName:       "example-cmd",
			RunMode:       env.RunModeTest,
			TimeLocation:  "UTC",
			ConfigDirPath: dir,
			LocalIP:       "192.0.2.20",
		}); err != nil {
			t.Fatal(err)
		}
		file := writeConfig(t, dir, "app.yaml", validYAML)
		value := panicValue(func() { httpconfig.MustLoadAppConfig(file) })
		err, ok := value.(error)
		if !ok || !errors.Is(err, env.ErrAlreadyInitialized) {
			t.Fatalf("应保留环境已初始化错误: %v", value)
		}
		if env.AppName() != "example-cmd" || env.RunMode() != env.RunModeTest || env.TimeLocation().String() != "UTC" || env.ConfigDirPath() != dir || env.LocalIP() != "192.0.2.20" {
			t.Fatal("HTTP 配置加载不应覆盖其他入口发布的环境")
		}
	})
}
