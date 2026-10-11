package bootstrap

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestHTTPServiceConfigurationFailsBeforeRunning(t *testing.T) {
	for _, content := range []string{
		"unknown: true\n",
		"readHeaderTimeout: -1s\n",
		"shutdownTimeout: -1s\n",
		"addr: [invalid]\n",
	} {
		file := writeHTTPConfig(t, content)
		if service, err := NewHTTPService(file, http.NewServeMux()); err == nil || service.Run != nil {
			t.Fatalf("无效配置被接受: %q, %v", content, err)
		}
	}
	if _, err := NewHTTPService(filepath.Join(t.TempDir(), "missing.yaml"), http.NewServeMux()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("缺失配置未保留错误链: %v", err)
	}
	if _, err := NewHTTPService(writeHTTPConfig(t, "{}\n"), nil); err == nil {
		t.Fatal("nil handler 被接受")
	}
}

func TestHTTPServiceShutdownBeforeRun(t *testing.T) {
	service, err := NewHTTPService(writeHTTPConfig(t, "addr: 127.0.0.1:0\n"), http.NewServeMux())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := service.Run(); err != nil {
		t.Fatalf("先关闭后运行应视为正常退出: %v", err)
	}
}

func TestHTTPServicePreservesListenError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	// 构造阶段不监听，地址占用应在 Run 时返回。
	file := writeHTTPConfig(t, fmt.Sprintf("addr: %q\n", listener.Addr().String()))
	service, err := NewHTTPService(file, http.NewServeMux())
	if err != nil {
		t.Fatal(err)
	}
	defer service.Shutdown()
	var opErr *net.OpError
	if err := service.Run(); !errors.As(err, &opErr) || opErr.Op != "listen" {
		t.Fatalf("监听失败未保留错误链: %v", err)
	}
}

func writeHTTPConfig(t *testing.T, content string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "http.yaml")
	if err := os.WriteFile(file, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	return file
}
