package bootstrap_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"
)

func TestGinHTTPServiceAssembly(t *testing.T) {
	for _, mode := range []string{env.RunModeDebug, env.RunModeTest, env.RunModeRelease} {
		t.Run(mode, func(t *testing.T) {
			isolatedLog(t, func() {
				if err := env.Init(env.Config{
					AppName:       "gin-http-test",
					RunMode:       mode,
					TimeLocation:  "UTC",
					ConfigDirPath: t.TempDir(),
					LocalIP:       "127.0.0.1",
				}); err != nil {
					t.Fatal(err)
				}
				writeGinConfig(t, "disableAccessLog: true\n")
				writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
				for _, name := range []string{"app", "admin"} {
					swag.Register(name, &swag.Spec{SwaggerTemplate: `{"swagger":"2.0","info":{"title":"API","version":"1"},"paths":{}}`})
				}
				otherMode := gin.ReleaseMode
				if mode == env.RunModeRelease {
					otherMode = gin.DebugMode
				}
				gin.SetMode(otherMode)
				var engine *gin.Engine
				calls := 0
				service, err := bootstrap.NewGinHTTPService(func(r *gin.Engine) {
					calls++
					if gin.Mode() != mode {
						t.Fatalf("注册路由前未设置 Gin 模式: %s", gin.Mode())
					}
					engine = r
					r.GET("/healthz", func(c *gin.Context) {
						c.Status(http.StatusNoContent)
					})
				},
					ginx.OpenAPIModule{Name: "app", Title: "App"},
					ginx.OpenAPIModule{Name: "admin", Title: "Admin"},
				)
				if err != nil {
					t.Fatal(err)
				}
				defer service.Shutdown()
				if calls != 1 || engine == nil || service.Name != "http" || service.Run == nil || service.Shutdown == nil {
					t.Fatalf("Gin HTTP 服务装配不完整: calls=%d, service=%+v", calls, service)
				}
				for _, path := range []string{"/healthz", "/docs/app", "/openapi/admin.json"} {
					want := http.StatusOK
					if path == "/healthz" {
						want = http.StatusNoContent
					} else if mode == env.RunModeRelease {
						want = http.StatusNotFound
					}
					response := httptest.NewRecorder()
					engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
					if response.Code != want {
						t.Fatalf("%s = %d，预期 %d", path, response.Code, want)
					}
				}
				// release 无需文档，允许省略业务路由回调直接构造服务。
				if mode == env.RunModeRelease {
					empty, err := bootstrap.NewGinHTTPService(nil)
					if err != nil {
						t.Fatal(err)
					}
					if err := empty.Shutdown(); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}

func TestGinHTTPServiceConstructionErrors(t *testing.T) {
	for _, stage := range []string{"gin", "openapi", "http"} {
		t.Run(stage, func(t *testing.T) {
			isolatedLog(t, func() {
				initHTTPConfigDir(t)
				if stage != "gin" {
					writeGinConfig(t, "disableAccessLog: true\n")
				}
				if stage != "http" {
					writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
				}
				if stage == "http" {
					swag.Register(swag.Name, &swag.Spec{SwaggerTemplate: `{"swagger":"2.0","info":{"title":"API","version":"1"},"paths":{}}`})
				}
				calls := 0
				service, err := bootstrap.NewGinHTTPService(func(*gin.Engine) {
					calls++
				})
				if err == nil || service.Run != nil || service.Shutdown != nil {
					t.Fatalf("构造失败不应返回可用服务: service=%+v, err=%v", service, err)
				}
				wantCalls := 0
				if stage == "http" {
					wantCalls = 1
				}
				if calls != wantCalls {
					t.Fatalf("%s 阶段失败时回调次数 = %d，预期 %d", stage, calls, wantCalls)
				}
				if stage != "openapi" && !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("缺失配置未保留错误链: %v", err)
				}
			})
		})
	}
}
