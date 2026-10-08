package ginx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/gin-gonic/gin"
)

func TestUnmatchedResponseObservation(t *testing.T) {
	for _, fallback := range []struct {
		name   string
		method string
		path   string
		status int
		body   string
	}{
		{
			name:   "404",
			method: http.MethodGet,
			path:   "/missing",
			status: http.StatusNotFound,
			body:   "404 page not found",
		},
		{
			name:   "405",
			method: http.MethodPost,
			path:   "/items",
			status: http.StatusMethodNotAllowed,
			body:   "405 method not allowed",
		},
	} {
		for _, outcome := range []string{"默认正文", "自定义正文", "修改状态", "panic", "中断"} {
			t.Run(fallback.name+"/"+outcome, func(t *testing.T) {
				output := captureAccessLogs(t)
				writer := httptest.NewRecorder()
				var results []RequestResult
				router, err := New(Config{
					LogResponseInfo: true,
					Observe: func(ctx context.Context, result RequestResult) {
						results = append(results, result)
						if id, _ := logit.LogIDFromContext(ctx); id != "trace" {
							t.Errorf("Observe 缺少请求日志 ID: %q", id)
						}
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				router.HandleMethodNotAllowed = true
				router.GET("/items", func(c *gin.Context) { c.Status(http.StatusNoContent) })
				status, body := fallback.status, fallback.body
				var handler gin.HandlerFunc
				switch outcome {
				case "自定义正文":
					body = "custom"
					handler = func(c *gin.Context) { c.String(status, body) }
				case "修改状态":
					// 改成另一种错误状态也不应补写默认正文,与 Gin 的回退条件一致。
					status, body = http.StatusMethodNotAllowed, ""
					if fallback.status == http.StatusMethodNotAllowed {
						status = http.StatusNotFound
					}
					handler = func(c *gin.Context) { c.Status(status) }
				case "panic":
					status, body = http.StatusInternalServerError, ""
					handler = func(*gin.Context) { panic("路由错误处理异常") }
				case "中断":
					status, body = 0, ""
					handler = func(*gin.Context) { panic(http.ErrAbortHandler) }
				}
				if handler != nil {
					router.NoRoute(handler)
					router.NoMethod(handler)
				}
				req := httptest.NewRequest(fallback.method, fallback.path, nil)
				req.Header.Set(TraceHeader, "trace")
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					router.ServeHTTP(writer, req)
				}()
				if outcome == "中断" {
					if recovered != http.ErrAbortHandler {
						t.Fatalf("未保留标准库中断: %v", recovered)
					}
				} else if recovered != nil || writer.Code != status {
					t.Fatalf("响应状态异常: status=%d panic=%v", writer.Code, recovered)
				}
				if writer.Body.String() != body || writer.Header().Get(TraceHeader) != "trace" {
					t.Fatalf("响应正文或日志 ID 变化: body=%q headers=%v", writer.Body.String(), writer.Header())
				}
				if len(results) != 1 {
					t.Fatalf("Observe 次数: %d", len(results))
				}
				result := results[0]
				if result.Status != status || result.Bytes != len(body) || result.Route != "unmatched" ||
					result.Panicked != (outcome == "panic") || result.Aborted != (outcome == "中断") {
					t.Errorf("Observe 结果不准确: %+v", result)
				}
				var accessCount int
				for _, record := range accessLogRecords(t, output) {
					if record["msg"] != "HTTPRequest" {
						continue
					}
					accessCount++
					info := record["response_info"].(map[string]any)
					if record["status"] != float64(status) || record["bytes"] != float64(len(body)) || info["body"] != body {
						t.Errorf("访问日志与响应不一致: %v", record)
					}
					if outcome == "默认正文" {
						headers := info["headers"].(map[string]any)
						contentType, _ := headers["Content-Type"].([]any)
						if len(contentType) != 1 || contentType[0] != gin.MIMEPlain {
							t.Errorf("未记录默认错误响应 Content-Type: %v", headers)
						}
					}
				}
				if accessCount != 1 {
					t.Errorf("访问日志次数: %d", accessCount)
				}
			})
		}
	}
}

func TestTrailingSlashAndExplicitRedirectObservation(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			for _, explicit := range []bool{false, true} {
				// 默认不匹配路径应返回 404;显式注册的重定向仍保留 301/307。
				output := captureAccessLogs(t)
				var results []RequestResult
				router, err := New(Config{
					DisableAccessLog: disabled,
					LogResponseInfo:  true,
					Observe: func(_ context.Context, result RequestResult) {
						results = append(results, result)
					},
				})
				if err != nil {
					t.Fatal(err)
				}
				router.Handle(method, "/items", func(c *gin.Context) { c.Status(http.StatusNoContent) })
				status, route := http.StatusNotFound, "unmatched"
				if explicit {
					status, route = http.StatusMovedPermanently, "/items/"
					if method == http.MethodPost {
						status = http.StatusTemporaryRedirect
					}
					router.Handle(method, route, func(c *gin.Context) { c.Redirect(status, "/items") })
				}
				writer := httptest.NewRecorder()
				router.ServeHTTP(writer, httptest.NewRequest(method, "/items/", nil))
				if writer.Code != status || writer.Header().Get(TraceHeader) == "" {
					t.Errorf("关闭日志=%t 方法=%s 显式重定向=%t: status=%d headers=%v", disabled, method, explicit, writer.Code, writer.Header())
				}
				if len(results) != 1 || results[0].Status != status || results[0].Route != route || results[0].Bytes != writer.Body.Len() {
					t.Errorf("重定向策略或 Observe 结果不准确: %+v", results)
				}
				records := accessLogRecords(t, output)
				if disabled {
					if len(records) != 0 {
						t.Errorf("关闭访问日志后仍输出: %v", records)
					}
					continue
				}
				if len(records) != 1 {
					t.Fatalf("访问日志次数: %d", len(records))
				}
				info := records[0]["response_info"].(map[string]any)
				if records[0]["status"] != float64(status) || records[0]["bytes"] != float64(writer.Body.Len()) || info["body"] != writer.Body.String() {
					t.Errorf("访问日志与响应不一致: %v", records[0])
				}
			}
		}
	}
}
