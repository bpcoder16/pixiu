package ginx

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestRegisterIgnoredRoute(t *testing.T) {
	output := captureAccessLogs(t)
	var observations int
	config := Config{
		LogRequestInfo:  true,
		LogResponseInfo: true,
		Observe: func(context.Context, RequestResult) {
			observations++
		},
	}
	router, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	router.HandleMethodNotAllowed = true
	group := router.Group("/static", func(c *gin.Context) {
		c.Header("X-Group", "active")
		c.Next()
	})
	const body = "request-body"
	handler := func(c *gin.Context) {
		data, err := io.ReadAll(c.Request.Body)
		if err != nil || string(data) != body {
			t.Errorf("业务读取正文变化: %q, %v", data, err)
		}
		c.String(http.StatusOK, "asset-body")
	}
	RegisterIgnoredRoute(group, http.MethodGet, "/app.js", func(c *gin.Context) {
		// 忽略必须在结果观察预读正文之前生效，原 Body 应原样到达业务。
		input, ok := c.Request.Body.(*accessLogBody)
		if !ok || input.reads != 0 {
			t.Error("忽略路由仍预读或替换了请求正文")
		}
		c.Header("X-Route", "active")
		c.Next()
	}, handler)
	group.POST("/app.js", handler)
	group.GET("/other.css", handler)
	other, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	other.GET("/static/app.js", handler)
	for _, tc := range []struct {
		name   string
		engine *gin.Engine
		method string
		path   string
		status int
		ignore bool
	}{
		{
			name:   "组前缀及查询参数",
			engine: router,
			method: http.MethodGet,
			path:   "/static/app.js?v=1",
			status: http.StatusOK,
			ignore: true,
		},
		{
			name:   "同一路径不同方法",
			engine: router,
			method: http.MethodPost,
			path:   "/static/app.js",
			status: http.StatusOK,
		},
		{
			name:   "未登记的静态资源",
			engine: router,
			method: http.MethodGet,
			path:   "/static/other.css",
			status: http.StatusOK,
		},
		{
			name:   "未匹配的资源",
			engine: router,
			method: http.MethodGet,
			path:   "/static/missing.js",
			status: http.StatusNotFound,
		},
		{
			name:   "未登记的方法",
			engine: router,
			method: http.MethodDelete,
			path:   "/static/app.js",
			status: http.StatusMethodNotAllowed,
		},
		{
			name:   "其他 Engine 同名路由",
			engine: other,
			method: http.MethodGet,
			path:   "/static/app.js",
			status: http.StatusOK,
		},
		{
			name:   "再次忽略不受上下文复用影响",
			engine: router,
			method: http.MethodGet,
			path:   "/static/app.js",
			status: http.StatusOK,
			ignore: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			output.Reset()
			observations = 0
			input := &accessLogBody{Reader: strings.NewReader(body)}
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req.Body = input
			req.ContentLength = int64(len(body))
			req.Header.Set(TraceHeader, "ignored-route-trace")
			writer := httptest.NewRecorder()
			tc.engine.ServeHTTP(writer, req)
			if writer.Code != tc.status || writer.Header().Get(TraceHeader) != "ignored-route-trace" {
				t.Fatalf("路由结果或请求作用域变化: status=%d, headers=%v", writer.Code, writer.Header())
			}
			if tc.status == http.StatusOK && writer.Body.String() != "asset-body" {
				t.Fatalf("响应正文变化: %q", writer.Body.String())
			}
			if tc.engine == router && tc.status == http.StatusOK && writer.Header().Get("X-Group") != "active" {
				t.Fatal("原组中间件未执行")
			}
			records := accessLogRecords(t, output)
			if tc.ignore {
				if len(records) != 0 || observations != 0 || writer.Header().Get("X-Route") != "active" {
					t.Fatalf("忽略失败或路由中间件未执行: records=%d, Observe=%d", len(records), observations)
				}
				return
			}
			if len(records) != 1 || observations != 1 {
				t.Fatalf("未登记路由的观察变化: records=%d, Observe=%d", len(records), observations)
			}
			record := records[0]
			if record["msg"] != "HTTPRequest" || record["status"] != float64(tc.status) || record["request_info"] == nil || record["response_info"] == nil {
				t.Fatalf("未登记路由的日志详情变化: %v", record)
			}
		})
	}
}

func TestRegisterIgnoredRouteRecovery(t *testing.T) {
	for _, outcome := range []string{"error", "panic", "committed-panic", "abort"} {
		t.Run(outcome, func(t *testing.T) {
			output := captureAccessLogs(t)
			var observations int
			router, err := New(Config{
				LogRequestInfo:  true,
				LogResponseInfo: true,
				Observe: func(context.Context, RequestResult) {
					observations++
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			RegisterIgnoredRoute(&router.RouterGroup, http.MethodGet, "/asset.js", func(c *gin.Context) {
				switch outcome {
				case "error":
					c.String(http.StatusInternalServerError, "failure")
				case "panic":
					panic("静态资源异常")
				case "committed-panic":
					c.String(http.StatusAccepted, "committed")
					panic("提交后异常")
				case "abort":
					panic(http.ErrAbortHandler)
				}
			})
			writer := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/asset.js", nil)
			req.Header.Set(TraceHeader, "ignored-panic-trace")
			var recovered any
			func() {
				defer func() { recovered = recover() }()
				router.ServeHTTP(writer, req)
			}()
			if outcome == "abort" {
				if recovered != http.ErrAbortHandler {
					t.Fatalf("HTTP 中断语义变化: %v", recovered)
				}
			} else if recovered != nil {
				t.Fatalf("Recovery 未捕获 panic: %v", recovered)
			}
			if observations != 0 {
				t.Fatalf("忽略路由仍调用 Observe: %d", observations)
			}
			records := accessLogRecords(t, output)
			if outcome == "panic" || outcome == "committed-panic" {
				if len(records) != 1 {
					t.Fatalf("独立 panic 日志数量变化: %d", len(records))
				}
				record := records[0]
				stack, _ := record["stack"].(string)
				if record["msg"] != "HTTPPanic" || record["level"] != "ERROR" || stack == "" || record["logId"] != "ignored-panic-trace" {
					t.Fatalf("独立 panic 日志变化: %v", records)
				}
			} else if len(records) != 0 {
				t.Fatalf("忽略路由仍写入访问日志: %v", records)
			}
			if outcome == "committed-panic" {
				if writer.Code != http.StatusAccepted || writer.Body.String() != "committed" {
					t.Fatal("已提交的响应被 Recovery 改写")
				}
			} else if outcome != "abort" && writer.Code != http.StatusInternalServerError {
				t.Fatalf("错误响应状态变化: %d", writer.Code)
			}
		})
	}
}
