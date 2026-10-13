package bootstrap_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/swaggo/swag"
)

func TestGinEngineOpenAPIModules(t *testing.T) {
	for _, mode := range []string{env.RunModeDebug, env.RunModeTest, env.RunModeRelease} {
		t.Run(mode, func(t *testing.T) {
			isolatedLog(t, func() {
				if err := env.Init(env.Config{
					AppName:       "gin-modules-test",
					RunMode:       mode,
					TimeLocation:  "UTC",
					ConfigDirPath: t.TempDir(),
					LocalIP:       "127.0.0.1",
				}); err != nil {
					t.Fatal(err)
				}
				writeGinConfig(t, "disableAccessLog: true\n")
				modules := []ginx.OpenAPIModule{{Name: "app", Title: "App"}, {Name: "admin", Title: "Admin"}}
				swag.Register("app", &swag.Spec{SwaggerTemplate: `{"swagger":"2.0","info":{"title":"App","version":"1"},"paths":{}}`})
				engine, err := bootstrap.NewGinEngine(modules...)
				if mode == env.RunModeRelease {
					if err != nil || engine == nil {
						t.Fatalf("release 不应要求全部模块文档: %v", err)
					}
				} else if err == nil || engine != nil || !strings.Contains(err.Error(), "admin") {
					t.Fatalf("缺少 Admin 文档应拒绝构造: engine=%v, err=%v", engine, err)
				}
				swag.Register("admin", &swag.Spec{SwaggerTemplate: `{"swagger":"2.0","info":{"title":"Admin","version":"1"},"paths":{}}`})
				engine, err = bootstrap.NewGinEngine(modules...)
				if err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{"/elements", "/elements/app", "/elements/admin", "/openapi/app.json", "/openapi/admin.json", "/elements/assets/openapi.js"} {
					response := httptest.NewRecorder()
					engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
					want := http.StatusOK
					if mode == env.RunModeRelease {
						want = http.StatusNotFound
					}
					if response.Code != want {
						t.Fatalf("%s = %d, want %d", path, response.Code, want)
					}
				}
			})
		})
	}
}
