package bootstrap_test

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
)

func TestHTTPServiceConfigurationFailsBeforeRunning(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		for _, content := range []string{
			"unknown: true\n",
			"readHeaderTimeout: -1s\n",
			"shutdownTimeout: -1s\n",
			"addr: [invalid]\n",
		} {
			writeHTTPConfig(t, content)
			if service, err := bootstrap.NewHTTPService(http.NewServeMux()); err == nil || service.Run != nil {
				t.Fatalf("无效配置被接受: %q, %v", content, err)
			}
		}
		if err := os.Remove(filepath.Join(env.ConfigDirPath(), "http.yaml")); err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.NewHTTPService(http.NewServeMux()); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("缺失配置未保留错误链: %v", err)
		}
		writeHTTPConfig(t, "{}\n")
		if _, err := bootstrap.NewHTTPService(nil); err == nil {
			t.Fatal("nil handler 被接受")
		}
	})
}

func TestHTTPServiceShutdownBeforeRun(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		// 配置目录来自环境快照，不能随当前工作目录变化。
		t.Chdir(t.TempDir())
		service, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		if err := service.Shutdown(); err != nil {
			t.Fatal(err)
		}
		if err := service.Run(); err != nil {
			t.Fatalf("先关闭后运行应视为正常退出: %v", err)
		}
	})
}

func TestHTTPServicePreservesListenError(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		// 构造阶段不监听，地址占用应在 Run 时返回。
		writeHTTPConfig(t, fmt.Sprintf("addr: %q\n", listener.Addr().String()))
		service, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		defer service.Shutdown()
		var opErr *net.OpError
		if err := service.Run(); !errors.As(err, &opErr) || opErr.Op != "listen" {
			t.Fatalf("监听失败未保留错误链: %v", err)
		}
	})
}

func writeHTTPConfig(t *testing.T, content string) {
	t.Helper()
	file := filepath.Join(env.ConfigDirPath(), "http.yaml")
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func initHTTPConfigDir(t *testing.T) {
	t.Helper()
	if err := env.Init(env.Config{
		AppName:       "http-test",
		RunMode:       env.RunModeTest,
		TimeLocation:  "UTC",
		ConfigDirPath: t.TempDir(),
		LocalIP:       "127.0.0.1",
	}); err != nil {
		t.Fatal(err)
	}
}
