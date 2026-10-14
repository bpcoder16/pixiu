package ginx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/swaggo/swag"
)

func TestRegisterOpenAPIModules(t *testing.T) {
	isolatedOpenAPI(t, func() {
		router, err := New(Config{DisableAccessLog: true})
		if err != nil {
			t.Fatal(err)
		}
		modules := []OpenAPIModule{{Name: "app", Title: "App"}, {Name: "admin", Title: "Admin <&>"}}
		const app = `{"swagger":"2.0","info":{"title":"App","version":"1"},"paths":{}}`
		const admin = `{"swagger":"2.0","info":{"title":"Admin","version":"1"},"paths":{}}`
		swag.Register("app", &swag.Spec{SwaggerTemplate: app})
		if err := RegisterOpenAPI(&router.RouterGroup, modules...); err == nil || !strings.Contains(err.Error(), "admin") {
			t.Fatalf("应指出缺失的模块文档: %v", err)
		}
		if len(router.Routes()) != 0 {
			t.Fatal("后续模块失败时不应留下部分路由")
		}
		swag.Register("admin", &swag.Spec{SwaggerTemplate: admin})
		for _, prefix := range []string{"", "/internal"} {
			if err := RegisterOpenAPI(router.Group(prefix), modules...); err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct{ path, contains string }{
				{"/openapi/app.json", app},
				{"/openapi/admin.json", admin},
				{"/docs", `data-document-url="` + prefix + `/openapi/app.json"`},
				{"/docs/app", `data-document-url="` + prefix + `/openapi/app.json"`},
				{"/docs/admin", `data-document-url="` + prefix + `/openapi/admin.json"`},
				{"/docs/assets/openapi.js", "apiDescriptionDocument"},
			} {
				response := httptest.NewRecorder()
				router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, prefix+tc.path, nil))
				if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), tc.contains) {
					t.Fatalf("%s%s = %d %s", prefix, tc.path, response.Code, response.Body.String())
				}
				if tc.path == "/docs/admin" {
					for _, want := range []string{
						`href="` + prefix + `/docs/app"`,
						`href="` + prefix + `/docs/admin" aria-current="page"`,
						"Admin &lt;&amp;&gt;",
						`src="` + prefix + `/docs/assets/web-components.min.js"`,
					} {
						if !strings.Contains(response.Body.String(), want) {
							t.Fatalf("页面缺少导航、转义或共享资源: %s", want)
						}
					}
				}
			}
			for _, path := range []string{
				"/docs/missing", "/openapi/missing.json", "/openapi/doc.json",
				"/elements", "/elements/app", "/elements/admin",
				"/elements/assets/web-components.min.js", "/elements/assets/styles.min.css", "/elements/assets/openapi.js",
			} {
				response := httptest.NewRecorder()
				router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, prefix+path, nil))
				if response.Code != http.StatusNotFound {
					t.Fatalf("未注册的文档路径 %s%s = %d，预期 404", prefix, path, response.Code)
				}
			}
		}
	})
}

func TestRegisterOpenAPIRejectsInvalidModules(t *testing.T) {
	for _, modules := range [][]OpenAPIModule{
		{{Name: ""}}, {{Name: "../app"}}, {{Name: "app/admin"}}, {{Name: "app?x"}},
		{{Name: "app"}, {Name: "app"}},
	} {
		router, err := New(Config{DisableAccessLog: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := RegisterOpenAPI(&router.RouterGroup, modules...); err == nil || !strings.Contains(err.Error(), "module name") {
			t.Fatalf("应优先校验模块名: %+v, err=%v", modules, err)
		}
		if len(router.Routes()) != 0 {
			t.Fatal("校验失败不应留下路由")
		}
	}
}
