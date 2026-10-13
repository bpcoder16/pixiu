package ginx

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/swaggo/swag"
)

func isolatedOpenAPI(t *testing.T, run func()) {
	t.Helper()
	// Swag 没有注销入口，独立进程保证缺失文档和重复测试均不受全局状态影响。
	const marker = "PIXIU_OPENAPI_TEST"
	if os.Getenv(marker) != t.Name() {
		cmd := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
		cmd.Env = append(os.Environ(), marker+"="+t.Name())
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("OpenAPI 独立进程测试失败: %v\n%s", err, output)
		}
		return
	}
	run()
}

func TestRegisterOpenAPI(t *testing.T) {
	isolatedOpenAPI(t, func() { testRegisterOpenAPI(t) })
}

func testRegisterOpenAPI(t *testing.T) {
	router, err := New(Config{DisableAccessLog: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterOpenAPI(&router.RouterGroup); err == nil {
		t.Fatal("未生成文档时应拒绝注册")
	}
	if len(router.Routes()) != 0 {
		t.Fatal("注册失败不应留下部分文档路由")
	}
	const document = `{"swagger":"2.0","info":{"title":"test API","version":"1.0"},"basePath":"/v2","paths":{"/ping":{"get":{"responses":{"200":{"description":"OK"}}}}}}`
	swag.Register(swag.Name, &swag.Spec{SwaggerTemplate: document})
	for _, prefix := range []string{"", "/internal"} {
		if err := RegisterOpenAPI(router.Group(prefix)); err != nil {
			t.Fatal(err)
		}
	}
	router.HandleMethodNotAllowed = true
	// 交替访问不同挂载位置，验证文档和资源引用保持各自的路由组前缀。
	for _, prefix := range []string{"/internal", "", "/internal"} {
		for _, tc := range []struct {
			path        string
			contains    string
			contentType string
		}{
			{"/openapi/doc.json", document, "application/json"},
			{"/elements", "<elements-api", "text/html"},
			{"/elements/assets/web-components.min.js", "elements-api", "javascript"},
			{"/elements/assets/styles.min.css", ".sl-elements", "text/css"},
			{"/elements/assets/openapi.js", "apiDescriptionDocument", "javascript"},
		} {
			r := httptest.NewRecorder()
			router.ServeHTTP(r, httptest.NewRequest(http.MethodGet, prefix+tc.path, nil))
			if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), tc.contains) || !strings.Contains(r.Header().Get("Content-Type"), tc.contentType) {
				t.Fatalf("GET %s%s: status=%d, type=%q，正文缺少 %q", prefix, tc.path, r.Code, r.Header().Get("Content-Type"), tc.contains)
			}
			if r.Header().Get(TraceHeader) == "" {
				t.Fatal("文档应经过请求作用域")
			}
			if tc.path == "/openapi/doc.json" && r.Body.String() != document {
				t.Fatal("JSON 端点不应改写原始文档的服务器字段")
			}
			if tc.path == "/elements" {
				body := r.Body.String()
				for _, want := range []string{
					`data-document-url="` + prefix + `/openapi/doc.json"`,
					`src="` + prefix + `/elements/assets/web-components.min.js"`,
					`src="` + prefix + `/elements/assets/openapi.js"`,
					`href="` + prefix + `/elements/assets/styles.min.css"`,
					`router="hash"`,
				} {
					if !strings.Contains(body, want) {
						t.Fatalf("Elements 应引用当前路由组的本地文档和资源并支持页面内导航，缺少 %q: %s", want, body)
					}
				}
				if strings.Contains(body, "https://") || strings.Contains(body, "http://") {
					t.Fatalf("Elements 页面不应依赖外部资源: %s", body)
				}
			}
		}
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/docs", http.StatusNotFound},
		{http.MethodGet, "/docs/", http.StatusNotFound},
		{http.MethodGet, "/docs/assets/api-reference.js", http.StatusNotFound},
		{http.MethodGet, "/docs/assets/missing.js", http.StatusNotFound},
		{http.MethodPost, "/docs", http.StatusNotFound},
		{http.MethodGet, "/internal/docs", http.StatusNotFound},
		{http.MethodGet, "/internal/docs/assets/api-reference.js", http.StatusNotFound},
		{http.MethodGet, "/swagger/index.html", http.StatusNotFound},
		{http.MethodGet, "/swagger/doc.json", http.StatusNotFound},
		{http.MethodGet, "/swagger/swagger-ui.css", http.StatusNotFound},
		{http.MethodGet, "/swagger/swagger-ui-bundle.js", http.StatusNotFound},
		{http.MethodGet, "/internal/swagger/index.html", http.StatusNotFound},
		{http.MethodGet, "/internal/swagger/doc.json", http.StatusNotFound},
		{http.MethodGet, "/openapi/doc.json/", http.StatusNotFound},
		{http.MethodPost, "/openapi/doc.json", http.StatusMethodNotAllowed},
		{http.MethodGet, "/elements/", http.StatusNotFound},
		{http.MethodGet, "/elements/assets/missing.js", http.StatusNotFound},
		{http.MethodPost, "/elements", http.StatusMethodNotAllowed},
		{http.MethodPost, "/elements/assets/web-components.min.js", http.StatusMethodNotAllowed},
		{http.MethodPost, "/elements/assets/styles.min.css", http.StatusMethodNotAllowed},
		{http.MethodPost, "/elements/assets/openapi.js", http.StatusMethodNotAllowed},
	} {
		r := httptest.NewRecorder()
		router.ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.status {
			t.Fatalf("%s %s = %d，预期 %d", tc.method, tc.path, r.Code, tc.status)
		}
	}
}
