package ginx

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/gin-gonic/gin"
)

func TestResponseController(t *testing.T) {
	r, err := New(Config{DisableAccessLog: true})
	if err != nil {
		t.Fatal(err)
	}
	results := make(chan []error, 1)
	r.POST("/", func(c *gin.Context) {
		ctrl := http.NewResponseController(c.Writer)
		results <- []error{
			ctrl.SetReadDeadline(time.Now().Add(time.Second)),
			ctrl.SetWriteDeadline(time.Now().Add(time.Second)),
			ctrl.EnableFullDuplex(),
		}
		c.Status(http.StatusNoContent)
	})
	server := httptest.NewServer(r)
	defer server.Close()
	resp, err := server.Client().Post(server.URL, "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	for i, err := range <-results {
		if err != nil {
			t.Errorf("ResponseController 操作 %d: %v", i, err)
		}
	}
}

func TestRecoveryAndRequestScopes(t *testing.T) {
	var records []RequestResult
	var output bytes.Buffer
	old := logit.Default()
	logit.SetDefault(logit.MustNew(logit.OptWriter(logit.NewWriter(&output)), logit.OptEncoder(logit.DefaultJSONEncoder)))
	t.Cleanup(func() { logit.SetDefault(old) })
	r, err := New(Config{Observe: func(ctx context.Context, result RequestResult) { records = append(records, result) }})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/panic", func(c *gin.Context) { panic("boom") })
	r.GET("/committed", func(c *gin.Context) {
		c.String(202, "kept")
		panic("boom")
	})
	r.GET("/items/:id", func(c *gin.Context) {
		logit.AddField(c.Request.Context(), logit.Str("item", c.Param("id")))
		c.String(200, "ok")
	})
	parent := logit.WithContext(context.Background())
	for _, path := range []string{"/panic", "/committed", "/items/one", "/items/two"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil).WithContext(parent)
		req.Header.Set("Authorization", "secret")
		req.Header.Set("PIXIU-Log-Id", "trace")
		r.ServeHTTP(w, req)
		if w.Header().Get("PIXIU-Log-Id") != "trace" {
			t.Fatal("缺少追踪响应头")
		}
		if req.Header.Get("Authorization") != "secret" {
			t.Fatal("日志修改请求头")
		}
	}
	if len(records) != 4 || records[0].Status != 500 || !records[0].Panicked || records[1].Status != 202 {
		t.Fatalf("结果: %+v", records)
	}
	if records[2].Route != "/items/:id" {
		t.Fatal(records[2].Route)
	}
	if strings.Contains(output.String(), "secret") {
		t.Fatal("日志泄漏 Header")
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	last := lines[len(lines)-1]
	if strings.Contains(last, "one") || !strings.Contains(last, "two") {
		t.Fatalf("字段串请求: %s", last)
	}
}

func TestTrustedProxies(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		cfg := Config{DisableAccessLog: true}
		if trusted {
			cfg.TrustedProxies = []string{"127.0.0.1"}
		}
		r, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		r.GET("/", func(c *gin.Context) { c.String(200, c.ClientIP()) })
		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("X-Forwarded-For", "198.51.100.1")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		want := "127.0.0.1"
		if trusted {
			want = "198.51.100.1"
		}
		if w.Body.String() != want {
			t.Fatalf("响应: %s %v", w.Body.String(), w.Header())
		}
	}
}

func TestTraceHeaderPropagation(t *testing.T) {
	for _, tt := range []struct {
		name    string
		missing bool
		id      string
	}{
		{
			name:    "缺失时生成",
			missing: true,
		},
		{
			name: "空值时生成",
		},
		{
			name: "普通值原样保留",
			id:   "trace-id",
		},
		{
			name: "长值原样保留",
			id:   strings.Repeat("x", 129),
		},
		{
			name: "空白值原样保留",
			id:   " \t ",
		},
		{
			name: "控制字符原样保留",
			id:   "trace\x00\r\n\x1f\x7f",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, err := New(Config{DisableAccessLog: true})
			if err != nil {
				t.Fatal(err)
			}
			var contextID string
			r.GET("/", func(c *gin.Context) {
				contextID, _ = logit.LogIDFromContext(c.Request.Context())
				c.Status(http.StatusNoContent)
			})
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if !tt.missing {
				req.Header.Set(TraceHeader, tt.id)
			}
			// 直接调用 Handler,验证 ginx 对收到的 Header 值的处理,不涉及 HTTP 传输层校验。
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			responseID := w.Header().Get(TraceHeader)
			if responseID == "" {
				t.Fatal("未补充日志 ID")
			}
			if tt.id != "" && responseID != tt.id {
				t.Fatalf("非空日志 ID 被修改: got %q, want %q", responseID, tt.id)
			}
			if contextID != responseID {
				t.Fatalf("请求 meta 与响应 Header 的日志 ID 不一致: %q != %q", contextID, responseID)
			}
		})
	}
}

func TestAbortHandlerAndObserverIsolation(t *testing.T) {
	var result RequestResult
	r, err := New(Config{
		DisableAccessLog: true,
		Observe: func(ctx context.Context, r RequestResult) {
			result = r
			panic("observer")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.GET("/", func(c *gin.Context) { panic(http.ErrAbortHandler) })
	func() {
		defer func() {
			if value := recover(); value != http.ErrAbortHandler {
				t.Errorf("未保留标准库中断: %v", value)
			}
		}()
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}()
	if !result.Aborted || result.Status != 0 || result.Panicked {
		t.Fatalf("中断结果不准确: %+v", result)
	}
}
