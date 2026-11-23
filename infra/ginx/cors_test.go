package ginx

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/net/http/httpguts"
)

func TestCORS(t *testing.T) {
	const web = "https://web.example.com"
	rule := CORSRule{
		PathPrefix:       "/api/web",
		AllowOrigins:     []string{"https://WEB.example.com:443"},
		AllowMethods:     []string{"POST"},
		AllowHeaders:     []string{"Authorization", "Content-Type", "equipmentType"},
		ExposeHeaders:    []string{TraceHeader},
		AllowCredentials: true,
		MaxAge:           10 * time.Minute,
	}
	public := CORSRule{
		PathPrefix:   "/public",
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"GET"},
	}
	mw, err := NewCORS(rule, public)
	if err != nil {
		t.Fatal(err)
	}
	// 构造后修改原始配置不能改变服务中的策略。
	rule.AllowMethods[0] = "DELETE"
	rule.AllowHeaders[0] = "X-Other"
	engine, err := New(Config{
		DisableAccessLog: true,
		Middlewares: []gin.HandlerFunc{
			func(c *gin.Context) { c.Header("Vary", "Accept-Encoding") },
			mw,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine.HandleMethodNotAllowed = true
	engine.POST("/api/web/probe", func(c *gin.Context) { c.AbortWithStatus(http.StatusUnauthorized) })
	engine.GET("/public", func(c *gin.Context) { c.Status(http.StatusOK) })
	for _, tc := range []struct {
		name, method, path, origin, requested, headers string
		status                                         int
		allow                                          string
	}{
		{"预检", "OPTIONS", "/api/web/probe", web, "POST", "equipmentTYPE, Authorization, content-type", 204, web},
		{"拒绝来源", "OPTIONS", "/api/web/probe", "https://other.example.com", "POST", "", 204, ""},
		{"拒绝端口", "OPTIONS", "/api/web/probe", web + ":444", "POST", "", 204, ""},
		{"拒绝方法", "OPTIONS", "/api/web/probe", web, "DELETE", "", 204, ""},
		{"拒绝额外头", "OPTIONS", "/api/web/probe", web, "POST", "authorization,x-other", 204, ""},
		{"认证响应", "POST", "/api/web/probe", web, "", "", 401, web},
		{"无来源", "POST", "/api/web/probe", "", "", "", 401, ""},
		{"普通 OPTIONS", "OPTIONS", "/api/web/probe", web, "", "", 405, ""},
		{"无来源 OPTIONS", "OPTIONS", "/api/web/probe", "", "POST", "", 405, ""},
		{"相似前缀", "OPTIONS", "/api/webhook", web, "POST", "", 404, ""},
		{"公开资源", "GET", "/public", web, "", "", 200, "*"},
		{"无额外请求头", "OPTIONS", "/public", web, "GET", "", 204, "*"},
		{"空清单不放行 JSON 头", "OPTIONS", "/public", web, "GET", "content-type", 204, ""},
		{"请求头空列表", "OPTIONS", "/public", web, "GET", " ", 204, "*"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.requested != "" {
				req.Header.Set("Access-Control-Request-Method", tc.requested)
			}
			if tc.headers != "" {
				req.Header.Set("Access-Control-Request-Headers", tc.headers)
			}
			w := httptest.NewRecorder()
			engine.ServeHTTP(w, req)
			h := w.Result().Header
			if w.Code != tc.status || h.Get("Access-Control-Allow-Origin") != tc.allow {
				t.Fatalf("响应 %d %v", w.Code, h)
			}
			if (h.Get("Access-Control-Allow-Credentials") == "true") != (tc.allow == web) {
				t.Fatalf("凭据响应 %v", h)
			}
			vary := h.Values("Vary")
			if !httpguts.HeaderValuesContainsToken(vary, "Accept-Encoding") {
				t.Fatalf("丢失已有 Vary: %v", vary)
			}
			inScope := tc.path != "/api/webhook"
			if httpguts.HeaderValuesContainsToken(vary, "Origin") != inScope {
				t.Fatalf("Origin 缓存维度: %v", vary)
			}
			if inScope && tc.origin != "" && tc.requested != "" {
				for _, key := range []string{"Access-Control-Request-Method", "Access-Control-Request-Headers"} {
					if !httpguts.HeaderValuesContainsToken(vary, key) {
						t.Fatalf("缺少预检缓存维度 %s: %v", key, vary)
					}
				}
			}
			if tc.name == "预检" {
				if w.Body.Len() != 0 || h.Get("Access-Control-Max-Age") != "600" || h.Get("Access-Control-Allow-Methods") != "POST" {
					t.Fatalf("预检结果 %s %v", w.Body, h)
				}
				for _, key := range []string{"authorization", "Content-Type", "equipmentType"} {
					if !httpguts.HeaderValuesContainsToken(h.Values("Access-Control-Allow-Headers"), key) {
						t.Fatalf("未允许 %s: %v", key, h)
					}
				}
			} else if h.Get("Access-Control-Max-Age") != "" {
				t.Fatal(h)
			}
			if (h.Get("Access-Control-Expose-Headers") == TraceHeader) != (tc.name == "认证响应") {
				t.Fatalf("暴露响应头 %v", h)
			}
		})
	}
}

func TestCORSUsesSameScopeAsRouter(t *testing.T) {
	mw, err := NewCORS(CORSRule{
		PathPrefix:   "/web",
		AllowOrigins: []string{"https://web.example.com"},
		AllowMethods: []string{"GET"},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := gin.New()
	engine.RemoveExtraSlash = true
	engine.Use(mw)
	engine.GET("/admin/info", func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/web/../admin/info", nil)
	req.Header.Set("Origin", "https://web.example.com")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusOK || w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("路由规范化后超出规则范围，得到 %d %v", w.Code, w.Header())
	}
}

func TestCORSPreservesVaryWildcard(t *testing.T) {
	mw, err := NewCORS(CORSRule{
		PathPrefix:   "/",
		AllowOrigins: []string{"*"},
		AllowMethods: []string{"GET"},
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := gin.New()
	engine.Use(func(c *gin.Context) { c.Header("Vary", "*") }, mw)
	req := httptest.NewRequest(http.MethodOptions, "/items", nil)
	req.Header.Set("Origin", "https://web.example.com")
	req.Header.Set("Access-Control-Request-Method", "GET")
	w := httptest.NewRecorder()
	engine.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent || strings.Join(w.Header().Values("Vary"), ",") != "*" {
		t.Fatalf("预检覆盖已有通配缓存策略: %d %v", w.Code, w.Header())
	}
}

func TestCORSInvalidRules(t *testing.T) {
	valid := CORSRule{
		PathPrefix:   "/api",
		AllowOrigins: []string{"http://localhost:5173"},
		AllowMethods: []string{"GET"},
	}
	for _, tc := range []struct {
		field  string
		change func(*CORSRule)
	}{
		{"pathPrefix", func(r *CORSRule) { r.PathPrefix = "api" }},
		{"allowOrigins", func(r *CORSRule) { r.AllowOrigins = nil }},
		{"allowOrigins", func(r *CORSRule) { r.AllowOrigins = []string{"https://example.com/path"} }},
		{"allowOrigins", func(r *CORSRule) { r.AllowOrigins = []string{"https://*.example.com"} }},
		{"allowOrigins", func(r *CORSRule) { r.AllowOrigins = []string{"*", "https://example.com"} }},
		{"allowCredentials", func(r *CORSRule) {
			r.AllowOrigins = []string{"*"}
			r.AllowCredentials = true
		}},
		{"allowMethods", func(r *CORSRule) { r.AllowMethods = nil }},
		{"allowMethods", func(r *CORSRule) { r.AllowMethods = []string{"BAD METHOD"} }},
		{"allowHeaders", func(r *CORSRule) { r.AllowHeaders = []string{"X-A\nB"} }},
		{"exposeHeaders", func(r *CORSRule) { r.ExposeHeaders = []string{"*"} }},
		{"maxAge", func(r *CORSRule) { r.MaxAge = time.Millisecond }},
		{"maxAge", func(r *CORSRule) { r.MaxAge = -time.Second }},
	} {
		rule := valid
		tc.change(&rule)
		if _, err := NewCORS(rule); err == nil || !strings.Contains(err.Error(), tc.field) {
			t.Errorf("字段 %s 应报告配置错误，得到 %v", tc.field, err)
		}
	}
	for _, prefix := range []string{"/", "/api", "/api/web"} {
		other := valid
		other.PathPrefix = prefix
		if _, err := NewCORS(valid, other); err == nil {
			t.Errorf("未拒绝重叠范围 %q", prefix)
		}
	}
}
