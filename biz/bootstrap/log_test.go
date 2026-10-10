package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

// 环境和默认日志属于进程状态，使用独立进程验证真实配置到文件的调用链。
func isolatedLog(t *testing.T, run func()) {
	t.Helper()
	const marker = "PIXIU_BOOTSTRAP_LOG_TEST"
	if os.Getenv(marker) == t.Name() {
		run()
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^"+regexp.QuoteMeta(t.Name())+"$")
	cmd.Env = append(os.Environ(), marker+"="+t.Name())
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("日志独立进程测试失败: %v\n%s", err, output)
	}
}

func loadLogConfig(t *testing.T, mode, logYAML string) *httpconfig.AppConfig {
	t.Helper()
	t.Chdir(t.TempDir())
	if err := os.Mkdir("conf", 0o755); err != nil {
		t.Fatal(err)
	}
	content := "env:\n  appName: bootstrap-test\n  runMode: " + mode + "\n  timeLocation: Asia/Shanghai\n  localIP: 192.0.2.10\n" + logYAML
	if err := os.WriteFile("conf/app.yaml", []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return httpconfig.MustLoadAppConfig("conf/app.yaml")
}

func recoverValue(fn func()) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestDefaultLogRemainsOpenUntilOtherResourcesClose(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      string
		logYAML   string
		file      string
		json      bool
		initPanic bool
	}{
		{name: "default-file-settings", mode: "debug", logYAML: "log:\n  format: text\n  caller: true\n", file: "log/bootstrap-test.info.log"},
		{
			name:      "json-project-init-panic",
			mode:      "release",
			logYAML:   "log:\n  format: json\n  caller: true\n  dir: output\n  rotate:\n    every: 24h\n    maxFiles: 3\n",
			file:      "output/bootstrap-test.info.log",
			json:      true,
			initPanic: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, tc.mode, tc.logYAML)
				path := filepath.Join(env.RootDirPath(), tc.file)
				if tc.json {
					if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
						t.Fatal(err)
					}
					for _, suffix := range []string{"20000101", "20000102", "20000103"} {
						if err := os.WriteFile(path+"."+suffix, nil, 0o600); err != nil {
							t.Fatal(err)
						}
					}
				}
				// 相对日志路径应固定到加载环境时的目录，而非随工作目录变化。
				t.Chdir(t.TempDir())
				local := time.Local
				var resources lifecycle.Stack
				var closeErr error
				wantCloseErr := errors.New("database close failed")
				wantPanic := errors.New("project init failed")
				value := recoverValue(func() {
					defer func() { closeErr = resources.Close() }()
					bootstrap.MustInit(context.Background(), cfg, &resources)
					if time.Local != local {
						t.Fatal("日志初始化不应修改进程时区")
					}
					logit.Info(context.Background(), "started", logit.Str("application", "demo"))
					if logit.DebugEnabled(context.Background()) != (tc.mode != "release") {
						t.Fatal("默认日志级别应匹配运行模式")
					}
					for _, name := range []string{"database", "worker"} {
						if err := resources.Register(func() error {
							logit.Info(context.Background(), name+" closing")
							if name == "database" {
								return wantCloseErr
							}
							return nil
						}); err != nil {
							t.Fatal(err)
						}
					}
					if tc.initPanic {
						panic(wantPanic)
					}
				})
				if tc.initPanic && value != wantPanic || !tc.initPanic && value != nil {
					t.Fatalf("初始化 panic 未原样传播: %v", value)
				}
				if !errors.Is(closeErr, wantCloseErr) {
					t.Fatalf("应保留组件关闭错误: %v", closeErr)
				}
				target, err := os.Readlink(path)
				if err != nil {
					t.Fatalf("应创建 rotatefile 稳定软链: %v", err)
				}
				digits := 10
				if tc.json {
					digits = 8
				}
				if !regexp.MustCompile(`^` + regexp.QuoteMeta(filepath.Base(path)) + `\.\d{` + fmt.Sprint(digits) + `}$`).MatchString(target) {
					t.Fatalf("轮转周期未生效: %s", target)
				}
				if tc.json {
					files, err := filepath.Glob(path + ".*")
					if err != nil || len(files) != 3 {
						t.Fatalf("应按配置保留 3 个实际文件: %v, %v", files, err)
					}
					if _, err := os.Stat(path + ".20000101"); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("应清理最旧日志: %v", err)
					}
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				lines := strings.Split(strings.TrimSpace(string(data)), "\n")
				messages := []string{"started", "worker closing", "database closing"}
				if len(lines) != len(messages) {
					t.Fatalf("关闭时的日志丢失: %s", data)
				}
				for i, line := range lines {
					if tc.json {
						var record map[string]any
						if err := json.Unmarshal([]byte(line), &record); err != nil || record["msg"] != messages[i] || record["caller"] == nil {
							t.Fatalf("JSON 日志内容错误: %s, %v", line, err)
						}
					} else if !strings.Contains(line, "msg=["+messages[i]+"]") || !strings.Contains(line, "log_test.go:") {
						t.Fatalf("文本日志内容错误: %s", line)
					}
				}
				stats := logit.Default().(logit.WriteErrorStats)
				if stats.WriteErrors() != 0 {
					t.Fatalf("其他组件关闭期间日志写入失败: %v", stats.LastWriteError())
				}
				logit.Info(context.Background(), "after close")
				if !errors.Is(stats.LastWriteError(), os.ErrClosed) {
					t.Fatal("资源栈关闭后日志文件仍然打开")
				}
				if err := resources.Close(); !errors.Is(err, wantCloseErr) {
					t.Fatalf("重复关闭应沿用原结果: %v", err)
				}
			})
		})
	}
}

func TestLogCallerConfiguration(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		for _, tc := range []struct {
			name string
			yaml string
			want bool
		}{
			{name: "enabled", yaml: "  caller: true\n", want: true},
			{name: "disabled", yaml: "  caller: false\n"},
			{name: "omitted"},
		} {
			t.Run(format+"/"+tc.name, func(t *testing.T) {
				isolatedLog(t, func() {
					cfg := loadLogConfig(t, "debug", "log:\n  format: "+format+"\n"+tc.yaml+"  names: [worker]\n")
					var resources lifecycle.Stack
					defer resources.Close()
					bootstrap.MustInit(context.Background(), cfg, &resources)
					for _, name := range []string{"", "worker"} {
						logit.Info(logit.WithLoggerName(context.Background(), name), "caller configuration")
					}
					if err := resources.Close(); err != nil {
						t.Fatal(err)
					}
					for _, base := range []string{"bootstrap-test", "bootstrap-test.worker"} {
						data, err := os.ReadFile(filepath.Join(env.RootDirPath(), "log", base+".info.log"))
						if err != nil {
							t.Fatal(err)
						}
						if !strings.Contains(string(data), "caller configuration") {
							t.Fatalf("应保留日志消息: %s", data)
						}
						if format == "json" {
							var record map[string]any
							if err := json.Unmarshal(data, &record); err != nil {
								t.Fatal(err)
							}
							caller, present := record["caller"]
							if present != tc.want || tc.want && !strings.Contains(fmt.Sprint(caller), "log_test.go:") {
								t.Fatalf("JSON caller 应匹配配置: %s", data)
							}
						} else if strings.Contains(string(data), "log_test.go:") != tc.want {
							t.Fatalf("文本 caller 应匹配配置: %s", data)
						}
					}
				})
			})
		}
	}
}

func TestLogInitFailureDoesNotReplaceDefault(t *testing.T) {
	for _, tc := range []struct {
		name    string
		logYAML string
		closed  bool
	}{
		{name: "format", logYAML: "log:\n  format: other\n"},
		{name: "empty-format", logYAML: "log:\n  format: ''\n"},
		{name: "missing-format"},
		{name: "period", logYAML: "log:\n  format: text\n  rotate:\n    every: 2h\n"},
		{name: "retention", logYAML: "log:\n  format: text\n  rotate:\n    maxFiles: 2\n"},
		{name: "empty-name", logYAML: "log:\n  format: text\n  names: ['']\n"},
		{name: "duplicate-name", logYAML: "log:\n  format: text\n  names: [worker, worker]\n"},
		{name: "path-name", logYAML: "log:\n  format: text\n  names: ['../worker']\n"},
		{name: "path", logYAML: "log:\n  format: text\n  dir: conf/app.yaml\n"},
		{name: "closed-stack", logYAML: "log:\n  format: text\n", closed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", tc.logYAML)
				previous := logit.Default()
				var resources lifecycle.Stack
				defer resources.Close()
				if tc.closed {
					if err := resources.Close(); err != nil {
						t.Fatal(err)
					}
				}
				value := recoverValue(func() { bootstrap.MustInit(context.Background(), cfg, &resources) })
				err, ok := value.(error)
				if !ok || !strings.Contains(err.Error(), "bootstrap:") {
					t.Fatalf("日志初始化失败应 panic 并带上启动上下文: %v", value)
				}
				if tc.closed && !errors.Is(err, lifecycle.ErrClosed) {
					t.Fatalf("应保留关闭栈错误链: %v", err)
				}
				if tc.closed {
					for _, suffix := range []string{".info", ".debug", ".wf"} {
						path := filepath.Join(env.RootDirPath(), "log", env.AppName()+suffix+".log")
						requireLogFileOpen(t, path, false)
					}
				}
				if logit.Default() != previous {
					t.Fatal("失败不应替换默认 Logger")
				}
			})
		})
	}
}

func TestNamedLogsDispatchLevelsAndCloseTogether(t *testing.T) {
	for _, mode := range []string{"debug", "release"} {
		t.Run(mode, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, mode, "log:\n  format: text\n  names: [worker, request, debug, info, wf, worker.debug, '.', '..']\n")
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustInit(context.Background(), cfg, &resources)
				for _, name := range append([]string{""}, cfg.Log.Names...) {
					ctx := logit.WithLoggerName(context.Background(), name)
					logit.Debug(ctx, name+" debug message")
					logit.Info(ctx, name+" info message")
					logit.Warn(ctx, name+" warn message")
					logit.Error(ctx, name+" error message")
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				for _, name := range append([]string{""}, cfg.Log.Names...) {
					base := env.AppName()
					if name != "" {
						base += "." + name
						if logit.Named(name) == logit.Default() {
							t.Fatalf("命名 Logger %q 未注册", name)
						}
					}
					for _, group := range []struct {
						suffix string
						levels []string
					}{
						{suffix: ".info", levels: []string{"info"}},
						{suffix: ".debug", levels: []string{"debug"}},
						{suffix: ".wf", levels: []string{"warn", "error"}},
					} {
						path := filepath.Join(env.RootDirPath(), "log", base+group.suffix+".log")
						data, err := os.ReadFile(path)
						if err != nil {
							t.Fatal(err)
						}
						for _, level := range []string{"debug", "info", "warn", "error"} {
							want := strings.Contains(strings.Join(group.levels, ","), level) && !(mode == "release" && level == "debug")
							if strings.Contains(string(data), name+" "+level+" message") != want {
								t.Fatalf("级别分流错误: %s, %s", path, data)
							}
						}
					}
					ctx := logit.WithLoggerName(context.Background(), name)
					stats := logit.LoggerFromContext(ctx).(logit.WriteErrorStats)
					for _, level := range []logit.Level{logit.InfoLevel, logit.WarnLevel, logit.DebugLevel} {
						if !logit.LoggerFromContext(ctx).Enabled(level) {
							continue
						}
						before := stats.WriteErrors()
						logit.Output(ctx, level, 0, "after close")
						if stats.WriteErrors() != before+1 || !errors.Is(stats.LastWriteError(), os.ErrClosed) {
							t.Fatalf("日志 %q 的 %s 目标未统一关闭: %v", name, level, stats.LastWriteError())
						}
					}
				}
			})
		})
	}
}

func TestFatalLogDispatchesToWFBeforeExit(t *testing.T) {
	const marker = "PIXIU_BOOTSTRAP_FATAL_DIR"
	if dir := os.Getenv(marker); dir != "" {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  dir: "+dir+"\n  names: [worker]\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustInit(context.Background(), cfg, &resources)
		ctx := logit.WithLoggerName(context.Background(), os.Getenv("PIXIU_BOOTSTRAP_FATAL_NAME"))
		logit.Fatal(ctx, "fatal dispatch message")
		t.Fatal("Fatal 应退出进程")
	}
	for _, name := range []string{"", "worker"} {
		t.Run("name="+name, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(os.Args[0], "-test.run=^TestFatalLogDispatchesToWFBeforeExit$")
			cmd.Env = append(os.Environ(), marker+"="+dir, "PIXIU_BOOTSTRAP_FATAL_NAME="+name)
			output, err := cmd.CombinedOutput()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 1 {
				t.Fatalf("Fatal 应以状态 1 退出: %v, %s", err, output)
			}
			for _, loggerName := range []string{"", "worker"} {
				base := "bootstrap-test"
				if loggerName != "" {
					base += "." + loggerName
				}
				for _, suffix := range []string{".info", ".debug", ".wf"} {
					path := filepath.Join(dir, base+suffix+".log")
					data, readErr := os.ReadFile(path)
					if readErr != nil {
						t.Fatal(readErr)
					}
					want := loggerName == name && suffix == ".wf"
					if strings.Contains(string(data), "fatal dispatch message") != want {
						t.Fatalf("Fatal 应只写对应 WF 文件: %s, %s", path, data)
					}
				}
			}
		})
	}
}

func TestPartialNamedLogFailureLeavesCompletedLoggersForCaller(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  names: [worker]\n")
		dir := filepath.Join(env.RootDirPath(), "log")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, env.AppName()+".worker.wf.log"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		previous := logit.Default()
		var resources lifecycle.Stack
		defer resources.Close()
		value := recoverValue(func() { bootstrap.MustInit(context.Background(), cfg, &resources) })
		if value == nil || logit.Default() != previous || logit.Named("worker") != previous {
			t.Fatalf("部分创建失败不应发布默认或命名日志: %v", value)
		}
		// 完整的默认 Logger 交给应用栈；未完成的 worker Logger 自行清理。
		for _, suffix := range []string{".info", ".debug", ".wf"} {
			requireLogFileOpen(t, filepath.Join(dir, env.AppName()+suffix+".log"), true)
		}
		for _, suffix := range []string{".info", ".debug"} {
			requireLogFileOpen(t, filepath.Join(dir, env.AppName()+".worker"+suffix+".log"), false)
		}
		if err := resources.Close(); err != nil {
			t.Fatalf("应用栈应关闭此前已完成的 Logger: %v", err)
		}
		for _, suffix := range []string{".info", ".debug", ".wf"} {
			requireLogFileOpen(t, filepath.Join(dir, env.AppName()+suffix+".log"), false)
		}
	})
}

func TestLogDirectoryUsesAbsolutePathAndCreatesParents(t *testing.T) {
	isolatedLog(t, func() {
		dir := filepath.Join(t.TempDir(), "nested", "logs")
		cfg := loadLogConfig(t, "test", "log:\n  format: text\n  dir: "+dir+"\n")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustInit(context.Background(), cfg, &resources)
		logit.Info(context.Background(), "absolute directory")
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(dir, "bootstrap-test.info.log"))
		if err != nil || !strings.Contains(string(data), "absolute directory") {
			t.Fatalf("绝对目录应自动创建并保存日志: %s, %v", data, err)
		}
		if _, err := os.Stat("log"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("绝对目录不应拼接到启动目录: %v", err)
		}
		probes, err := filepath.Glob(filepath.Join(dir, ".pixiu-log-write-*"))
		if err != nil || len(probes) != 0 {
			t.Fatalf("目录可写检查不应留下临时文件: %v, %v", probes, err)
		}
	})
}

func TestLogDirectoryRejectsMissingWritePermission(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("当前环境无法通过 Unix 权限位限制目录访问")
	}
	isolatedLog(t, func() {
		dir := t.TempDir()
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n  dir: "+dir+"\n")
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatal(err)
		}
		previous := logit.Default()
		var resources lifecycle.Stack
		defer resources.Close()
		value := recoverValue(func() { bootstrap.MustInit(context.Background(), cfg, &resources) })
		err, ok := value.(error)
		if !ok || !errors.Is(err, os.ErrPermission) || !strings.Contains(err.Error(), dir) {
			t.Fatalf("不可写目录应保留权限错误与目录路径: %v", value)
		}
		if logit.Default() != previous {
			t.Fatal("目录不可写时不应替换默认 Logger")
		}
	})
}
