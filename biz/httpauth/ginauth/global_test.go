package ginauth_test

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"github.com/bpcoder16/pixiu/biz/httpauth"
	"github.com/bpcoder16/pixiu/biz/httpauth/ginauth"
	"github.com/gin-gonic/gin"
)

func TestDefaultMiddlewareLifecycle(t *testing.T) {
	// 用独立进程验证未初始化状态，不为测试增加全局重置 API。
	const marker = "PIXIU_GINAUTH_DEFAULT_TEST"
	if os.Getenv(marker) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestDefaultMiddlewareLifecycle$")
		cmd.Env = append(os.Environ(), marker+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("默认中间件测试失败: %v\n%s", err, output)
		}
		return
	}
	gin.SetMode(gin.TestMode)
	mustPanic := func(run func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Error("无效默认中间件配置应 panic")
			}
		}()
		run()
	}
	mustPanic(func() { ginauth.Default() })
	mustPanic(func() { ginauth.SetDefault(nil) })

	newGuard := func(userID string) *ginauth.Middleware {
		registry := httpauth.NewRegistry()
		registry.MustRegister("app", httpauth.AuthenticatorFunc(func(*http.Request) (*httpauth.Principal, error) {
			return &httpauth.Principal{UserID: userID}, nil
		}))
		return ginauth.New(registry)
	}
	first := newGuard("first")
	// New 不隐式安装默认值。
	mustPanic(func() { ginauth.Default() })
	ginauth.SetDefault(first)
	mustPanic(func() { ginauth.SetDefault(nil) })

	independent := newGuard("independent")
	if ginauth.Default() != first {
		t.Fatal("无效设置或 New 不应改变已安装的默认实例")
	}
	handler := func(c *gin.Context) {
		principal, _ := httpauth.PrincipalFromContext(c.Request.Context())
		c.String(http.StatusOK, principal.UserID)
	}
	router := gin.New()
	router.GET("/first", ginauth.Default().Require("app"), handler)
	router.GET("/independent", independent.Require("app"), handler)

	// 仍在路由构造阶段；已有路由保持原先绑定，后续路由使用新的默认实例。
	ginauth.SetDefault(newGuard("second"))
	router.GET("/second", ginauth.Default().Require("app"), handler)
	for _, userID := range []string{"first", "independent", "second"} {
		response := httptest.NewRecorder()
		router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/"+userID, nil))
		if response.Code != http.StatusOK || response.Body.String() != userID {
			t.Fatalf("路由认证实例发生混用: path=%s status=%d body=%q", userID, response.Code, response.Body.String())
		}
	}
}
