package bootstrap_test

import (
	"net/http"
	"os/exec"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 同时检查注册契约与生产依赖，避免仅跳过初始化却仍引入被禁用的 SDK。
func assertBootstrapModuleDisabled(t *testing.T, tag string, register func(), forbidden ...string) {
	t.Helper()
	t.Run("拒绝显式注册", func(t *testing.T) {
		value := recoverValue(register)
		err, ok := value.(error)
		if !ok || !strings.Contains(err.Error(), tag) {
			t.Fatalf("注册时应报告禁用模块的构建标签 %q: %v", tag, value)
		}
	})
	t.Run("生产依赖", func(t *testing.T) {
		cmd := exec.CommandContext(t.Context(), "go", "list", "-deps", "-tags="+tag, ".")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("go list -deps: %v\n%s", err, out)
		}
		for dep := range strings.SplitSeq(string(out), "\n") {
			for _, prefix := range forbidden {
				if strings.HasPrefix(dep, prefix) {
					t.Errorf("标签 %q 下 bootstrap 意外依赖 %s", tag, dep)
				}
			}
		}
	})
}

func TestBootstrapWithoutExternalModules(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "log:\n  format: json\n  hookStd: false\n")
		var resources lifecycle.Stack
		t.Cleanup(func() {
			if err := resources.Close(); err != nil {
				t.Error(err)
			}
		})
		bootstrap.MustBaseInit(cfg, &resources)
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		service, err := bootstrap.NewHTTPService(http.NewServeMux())
		if err != nil {
			t.Fatal(err)
		}
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Fatal(err)
		}
	})
}
