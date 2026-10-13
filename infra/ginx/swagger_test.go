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

func TestRegisterSwagger(t *testing.T) {
	// Swag 的默认文档没有注销入口，独立进程保证缺失文档和重复测试均不受全局状态影响。
	const marker = "PIXIU_SWAGGER_TEST"
	if os.Getenv(marker) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRegisterSwagger$")
		cmd.Env = append(os.Environ(), marker+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Swagger 独立进程测试失败: %v\n%s", err, output)
		}
		return
	}
	router, err := New(Config{DisableAccessLog: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := RegisterSwagger(&router.RouterGroup); err == nil {
		t.Fatal("未生成文档时应拒绝注册")
	}
	if len(router.Routes()) != 0 {
		t.Fatal("注册失败不应留下部分文档路由")
	}
	const document = `{"swagger":"2.0","info":{"title":"test API","version":"1.0"},"paths":{}}`
	swag.Register(swag.Name, &swag.Spec{SwaggerTemplate: document})
	for _, prefix := range []string{"", "/internal"} {
		if err := RegisterSwagger(router.Group(prefix)); err != nil {
			t.Fatal(err)
		}
	}
	router.HandleMethodNotAllowed = true
	// 交替访问不同挂载位置，防止 gin-swagger 的可变静态资源 Prefix 被全局共享。
	for _, prefix := range []string{"/internal", "", "/internal"} {
		for _, tc := range []struct {
			path        string
			contains    string
			contentType string
		}{
			{"/swagger/index.html", "Swagger UI", "text/html"},
			{"/swagger/doc.json", document, "application/json"},
			{"/swagger/swagger-ui.css", ".swagger-ui", "text/css"},
			{"/swagger/swagger-ui-bundle.js", "SwaggerUIBundle", "javascript"},
			{"/docs", prefix + "/docs/assets/api-reference.js", "text/html"},
			{"/docs/assets/api-reference.js", "createApiReference", "javascript"},
		} {
			r := httptest.NewRecorder()
			router.ServeHTTP(r, httptest.NewRequest(http.MethodGet, prefix+tc.path, nil))
			if r.Code != http.StatusOK || !strings.Contains(r.Body.String(), tc.contains) || !strings.Contains(r.Header().Get("Content-Type"), tc.contentType) {
				t.Fatalf("GET %s%s: status=%d, type=%q，正文缺少 %q", prefix, tc.path, r.Code, r.Header().Get("Content-Type"), tc.contains)
			}
			if r.Header().Get(TraceHeader) == "" {
				t.Fatal("文档应经过请求作用域")
			}
			if tc.path == "/docs" {
				body := r.Body.String()
				if !strings.Contains(body, `"url":"`+prefix+`/swagger/doc.json"`) || strings.Contains(body, "https://") {
					t.Fatalf("Scalar 应引用当前路由组的本地文档和资源: %s", body)
				}
			}
		}
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{http.MethodGet, "/docs/", http.StatusNotFound},
		{http.MethodGet, "/docs/assets/missing.js", http.StatusNotFound},
		{http.MethodPost, "/docs", http.StatusMethodNotAllowed},
	} {
		r := httptest.NewRecorder()
		router.ServeHTTP(r, httptest.NewRequest(tc.method, tc.path, nil))
		if r.Code != tc.status {
			t.Fatalf("%s %s = %d，预期 %d", tc.method, tc.path, r.Code, tc.status)
		}
	}
}
