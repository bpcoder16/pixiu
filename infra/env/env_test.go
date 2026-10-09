package env

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/netx"
)

func isolateEnvironment(t *testing.T) {
	t.Helper()
	previous := defaultEnv.Swap(nil)
	t.Cleanup(func() { defaultEnv.Store(previous) })
}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		AppName:       "example-cmd",
		RunMode:       RunModeTest,
		TimeLocation:  "Asia/Shanghai",
		ConfigDirPath: t.TempDir(),
		LocalIP:       "192.0.2.10",
	}
}

func panicValue(fn func()) (value any) {
	defer func() { value = recover() }()
	fn()
	return nil
}

func TestReadBeforeInit(t *testing.T) {
	isolateEnvironment(t)
	for _, get := range []func(){
		func() { AppName() },
		func() { RunMode() },
		func() { TimeLocation() },
		func() { ConfigDirPath() },
		func() { LocalIP() },
		func() { RootDirPath() },
	} {
		if value := panicValue(get); value == nil || !strings.Contains(fmt.Sprint(value), "not initialized") {
			t.Fatalf("初始化前读取应 panic: %v", value)
		}
	}
}

func TestInitValidatesBeforePublishing(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Config)
		field  string
	}{
		{"blank-name", func(c *Config) { c.AppName = " \t" }, "appName"},
		{"missing-mode", func(c *Config) { c.RunMode = "" }, "runMode"},
		{"invalid-mode", func(c *Config) { c.RunMode = "development" }, "runMode"},
		{"missing-location", func(c *Config) { c.TimeLocation = "" }, "timeLocation"},
		{"invalid-location", func(c *Config) { c.TimeLocation = "Invalid/Timezone" }, "timeLocation"},
		{"empty-directory", func(c *Config) { c.ConfigDirPath = "" }, "configDirPath"},
		{"relative-directory", func(c *Config) { c.ConfigDirPath = "./conf" }, "configDirPath"},
		{"invalid-ip", func(c *Config) { c.LocalIP = "not-an-ip" }, "localIP"},
		{"ip-with-port", func(c *Config) { c.LocalIP = "192.0.2.10:8080" }, "localIP"},
		{"blank-ip", func(c *Config) { c.LocalIP = " \t" }, "localIP"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isolateEnvironment(t)
			cfg := testConfig(t)
			tc.change(&cfg)
			if err := Init(cfg); err == nil || !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("应报告无效字段 %s: %v", tc.field, err)
			}
			if value := panicValue(func() { AppName() }); value == nil {
				t.Fatal("校验失败不应发布环境")
			}
			if err := Init(testConfig(t)); err != nil {
				t.Fatalf("env 校验失败不应阻止修正后初始化: %v", err)
			}
		})
	}
}

func TestInitUsesProvidedLocalIP(t *testing.T) {
	for _, ip := range []string{"192.0.2.10", "2001:0DB8::1"} {
		t.Run(ip, func(t *testing.T) {
			isolateEnvironment(t)
			cfg := testConfig(t)
			cfg.LocalIP = ip
			if err := Init(cfg); err != nil {
				t.Fatal(err)
			}
			if got := LocalIP(); got != ip {
				t.Fatalf("应原样保存指定 IP: got %q, want %q", got, ip)
			}
		})
	}
}

func TestInitDetectsLocalIPv4(t *testing.T) {
	isolateEnvironment(t)
	cfg := testConfig(t)
	cfg.LocalIP = ""
	want, detectErr := netx.LocalIPv4()
	err := Init(cfg)
	if detectErr != nil {
		if err == nil || !strings.Contains(err.Error(), "localIP") {
			t.Fatalf("查询失败应报告 localIP 错误: %v", err)
		}
		if value := panicValue(func() { LocalIP() }); value == nil {
			t.Fatal("查询失败不应发布环境")
		}
		if err := Init(testConfig(t)); err != nil {
			t.Fatalf("指定 IP 后应允许重新初始化: %v", err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	got := LocalIP()
	if got != want || net.ParseIP(got).To4() == nil {
		t.Fatalf("应使用 netx 查询的 IPv4: got %q, want %q", got, want)
	}
	if cfg.LocalIP != "" {
		t.Fatal("补齐 IP 不应修改调用方配置")
	}
}

func TestInitPreservesConfigDirPath(t *testing.T) {
	isolateEnvironment(t)
	cfg := testConfig(t)
	if err := os.Mkdir(filepath.Join(cfg.ConfigDirPath, "child"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := cfg.ConfigDirPath + string(os.PathSeparator) + "child" + string(os.PathSeparator) + ".." + string(os.PathSeparator)
	cfg.ConfigDirPath = path
	if err := Init(cfg); err != nil {
		t.Fatalf("存在且可访问的绝对目录应通过校验: %v", err)
	}
	if got := ConfigDirPath(); got != path {
		t.Fatalf("配置目录应原样保存: got %q, want %q", got, path)
	}
}

func TestInitCapturesRootDirPath(t *testing.T) {
	isolateEnvironment(t)
	cfg := testConfig(t)
	t.Chdir(t.TempDir())
	want, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := Init(cfg); err != nil {
		t.Fatal(err)
	}
	if got := RootDirPath(); got != want || !filepath.IsAbs(got) {
		t.Fatalf("应保存初始化时工作目录的绝对路径: got %q, want %q", got, want)
	}
	t.Chdir(t.TempDir())
	if got := RootDirPath(); got != want {
		t.Fatalf("切换工作目录不应改变环境快照: got %q, want %q", got, want)
	}
}

func TestInitRejectsUnavailableWorkingDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows 不允许删除当前工作目录")
	}
	isolateEnvironment(t)
	cfg := testConfig(t)
	dir := t.TempDir()
	t.Chdir(dir)
	// 删除实际工作目录，验证获取失败时不发布环境且允许修正后重试。
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Getwd(); err == nil {
		t.Skip("当前系统仍能返回已删除工作目录的路径，无法构造 Getwd 失败")
	}
	err := Init(cfg)
	if err == nil || !strings.Contains(err.Error(), "rootDirPath") || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("应保留获取工作目录失败的错误: %v", err)
	}
	if value := panicValue(func() { AppName() }); value == nil {
		t.Fatal("获取工作目录失败不应发布环境")
	}
	if err := os.Chdir(cfg.ConfigDirPath); err != nil {
		t.Fatal(err)
	}
	if err := Init(cfg); err != nil {
		t.Fatalf("修正工作目录后应允许重新初始化: %v", err)
	}
}

func TestInitRejectsInvalidConfigDirectory(t *testing.T) {
	for _, name := range []string{"missing", "file", "unreadable", "unsearchable"} {
		t.Run(name, func(t *testing.T) {
			isolateEnvironment(t)
			cfg := testConfig(t)
			var wantError error
			switch name {
			case "missing":
				cfg.ConfigDirPath = filepath.Join(cfg.ConfigDirPath, "missing")
				wantError = os.ErrNotExist
			case "file":
				cfg.ConfigDirPath = filepath.Join(cfg.ConfigDirPath, "app.yaml")
				if err := os.WriteFile(cfg.ConfigDirPath, []byte("env: {}"), 0o600); err != nil {
					t.Fatal(err)
				}
			default:
				if runtime.GOOS == "windows" || os.Geteuid() == 0 {
					t.Skip("当前环境无法通过 Unix 权限位限制目录访问")
				}
				mode := os.FileMode(0o300)
				if name == "unsearchable" {
					mode = 0o600
				}
				t.Cleanup(func() {
					if err := os.Chmod(cfg.ConfigDirPath, 0o700); err != nil {
						t.Error(err)
					}
				})
				if err := os.Chmod(cfg.ConfigDirPath, mode); err != nil {
					t.Fatal(err)
				}
				wantError = os.ErrPermission
			}
			err := Init(cfg)
			if err == nil || !strings.Contains(err.Error(), "configDirPath") {
				t.Fatalf("应拒绝无效配置目录: %v", err)
			}
			if wantError != nil && !errors.Is(err, wantError) {
				t.Fatalf("应保留文件系统错误 %v: %v", wantError, err)
			}
			if value := panicValue(func() { ConfigDirPath() }); value == nil {
				t.Fatal("目录校验失败不应发布环境")
			}
			if err := Init(testConfig(t)); err != nil {
				t.Fatalf("修正目录后应允许重新初始化: %v", err)
			}
		})
	}
}

func TestInitSnapshotsConfigAndKeepsProcessLocation(t *testing.T) {
	isolateEnvironment(t)
	cfg := testConfig(t)
	dir := cfg.ConfigDirPath
	local := time.Local
	if err := Init(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.AppName = "changed"
	cfg.RunMode = RunModeRelease
	cfg.TimeLocation = "UTC"
	cfg.ConfigDirPath = "changed"
	cfg.LocalIP = "192.0.2.20"
	if AppName() != "example-cmd" || RunMode() != RunModeTest || TimeLocation().String() != "Asia/Shanghai" || ConfigDirPath() != dir || LocalIP() != "192.0.2.10" {
		t.Fatal("环境快照不正确或随输入配置改变")
	}
	if time.Local != local {
		t.Fatal("初始化不应修改进程时区")
	}
	if err := Init(cfg); !errors.Is(err, ErrAlreadyInitialized) {
		t.Fatalf("应拒绝其他启动入口重复初始化: %v", err)
	}
	if AppName() != "example-cmd" {
		t.Fatal("重复初始化不应覆盖已有环境")
	}
}

func TestConcurrentInitAndRead(t *testing.T) {
	isolateEnvironment(t)
	cfg := testConfig(t)
	rootDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var readers sync.WaitGroup
	for range workers {
		go func() {
			<-start
			results <- Init(cfg)
		}()
		readers.Add(1)
		go func() {
			defer readers.Done()
			<-start
			for range 100 {
				value := panicValue(func() {
					if AppName() != cfg.AppName || RunMode() != cfg.RunMode || TimeLocation().String() != cfg.TimeLocation || ConfigDirPath() != cfg.ConfigDirPath || LocalIP() != cfg.LocalIP || RootDirPath() != rootDir {
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
		if err := <-results; err == nil {
			successes++
		} else if !errors.Is(err, ErrAlreadyInitialized) {
			t.Errorf("并发初始化错误不正确: %v", err)
		}
	}
	readers.Wait()
	if successes != 1 || AppName() != cfg.AppName {
		t.Fatalf("应仅一次发布成功，成功次数: %d", successes)
	}
}
