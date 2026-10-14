package bootstrap_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"
)

func TestGinEngineOpenAPIRunModes(t *testing.T) {
	for _, mode := range []string{env.RunModeDebug, env.RunModeTest, env.RunModeRelease} {
		t.Run(mode, func(t *testing.T) {
			isolatedLog(t, func() {
				dir := t.TempDir()
				if err := env.Init(env.Config{
					AppName:       "bootstrap-gin-test",
					RunMode:       mode,
					TimeLocation:  "UTC",
					ConfigDirPath: dir,
					LocalIP:       "127.0.0.1",
				}); err != nil {
					t.Fatal(err)
				}
				// 故意与 env 不同，文档开关只取决于应用环境，构造也不得改写 Gin 模式。
				ginMode := gin.ReleaseMode
				if mode == env.RunModeRelease {
					ginMode = gin.DebugMode
				}
				gin.SetMode(ginMode)
				writeGinConfig(t, "disableAccessLog: true\n")
				engine, err := bootstrap.NewGinEngine()
				if mode != env.RunModeRelease {
					if err == nil || engine != nil {
						t.Fatalf("无生成文档时应构造失败: engine=%v, err=%v", engine, err)
					}
					if !strings.Contains(err.Error(), "swag init -o ./openapi") || !strings.Contains(err.Error(), "blank import") {
						t.Fatalf("缺少生成或导入文档包的提示: %v", err)
					}
					if errors.Unwrap(err) == nil {
						t.Fatalf("未保留文档注册错误链: %v", err)
					}
				} else if err != nil || engine == nil || len(engine.Routes()) != 0 {
					t.Fatalf("release 无文档时应构造无路由的 Engine: engine=%v, err=%v", engine, err)
				}
				// 旧配置即使非法也不影响注册，文档开关只由 RunMode 决定。
				if err := os.WriteFile(filepath.Join(dir, "swagger.yaml"), []byte("enabled: ["), 0600); err != nil {
					t.Fatal(err)
				}
				const document = `{"swagger":"2.0","info":{"title":"test API","version":"1.0"},"paths":{}}`
				swag.Register(swag.Name, &swag.Spec{SwaggerTemplate: document})
				engine, err = bootstrap.NewGinEngine()
				if err != nil {
					t.Fatal(err)
				}
				if gin.Mode() != ginMode {
					t.Fatalf("构造改写了 Gin 模式: %s", gin.Mode())
				}
				wantStatus := http.StatusOK
				if mode == env.RunModeRelease {
					wantStatus = http.StatusNotFound
				}
				for _, path := range []string{
					"/openapi/doc.json", "/docs",
					"/docs/assets/web-components.min.js", "/docs/assets/styles.min.css", "/docs/assets/openapi.js",
				} {
					r := httptest.NewRecorder()
					engine.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
					if r.Code != wantStatus {
						t.Fatalf("%s = %d，预期 %d", path, r.Code, wantStatus)
					}
					if path == "/openapi/doc.json" && wantStatus == http.StatusOK && r.Body.String() != document {
						t.Fatalf("返回了错误的文档内容: %s", r.Body.String())
					}
				}
				for _, path := range []string{
					"/elements", "/elements/assets/web-components.min.js", "/elements/assets/styles.min.css", "/elements/assets/openapi.js",
					"/docs/assets/api-reference.js", "/swagger/index.html", "/swagger/doc.json", "/swagger/swagger-ui.css", "/swagger/swagger-ui-bundle.js",
				} {
					r := httptest.NewRecorder()
					engine.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
					if r.Code != http.StatusNotFound {
						t.Fatalf("已移除的文档入口 %s = %d，预期 404", path, r.Code)
					}
				}
			})
		})
	}
}

func TestGinEngineRejectsInvalidConfiguration(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		if engine, err := bootstrap.NewGinEngine(); engine != nil || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("缺失 gin.yaml 应保留错误链: engine=%v, err=%v", engine, err)
		}
		for _, content := range []string{
			"unknown: true\n",
			"gin:\n  disableAccessLog: true\n",
			"logRequestInfo: 'true'\n",
			"trustedProxies: [123]\n",
			"trustedProxies: [not-an-ip]\n",
			"observe: null\n",
			"middlewares: []\n",
		} {
			writeGinConfig(t, content)
			engine, err := bootstrap.NewGinEngine()
			if err == nil || engine != nil || !strings.Contains(err.Error(), "gin.yaml") {
				t.Fatalf("非法配置应拒绝并标明文件: %q, engine=%v, err=%v", content, engine, err)
			}
		}
	})
}

func TestGinEngineReadsConfigurationAtConstruction(t *testing.T) {
	isolatedLog(t, func() {
		swag.Register(swag.Name, &swag.Spec{SwaggerTemplate: `{"swagger":"2.0","info":{"title":"test API","version":"1.0"},"paths":{}}`})
		template, err := os.ReadFile("conf.example/gin.yaml")
		if err != nil {
			t.Fatal(err)
		}
		initHTTPConfigDir(t)
		// 构造只使用环境中的配置目录，不依赖之后的工作目录。
		t.Chdir(t.TempDir())
		for _, tc := range []struct {
			name         string
			content      string
			clientIP     string
			named        bool
			disabled     bool
			requestInfo  bool
			responseInfo bool
		}{
			{name: "零值默认", content: "{}\n", clientIP: "127.0.0.1"},
			{
				name:         "配置模板",
				content:      string(template),
				clientIP:     "127.0.0.1",
				requestInfo:  true,
				responseInfo: true,
			},
			{
				name:        "命名日志与可信代理和请求详情",
				content:     "trustedProxies: [127.0.0.1]\nloggerName: bootstrap-gin-test\nlogRequestInfo: true\n",
				clientIP:    "198.51.100.7",
				named:       true,
				requestInfo: true,
			},
			{
				name:         "仅响应详情",
				content:      "logResponseInfo: true\n",
				clientIP:     "127.0.0.1",
				responseInfo: true,
			},
			{
				name:     "关闭访问日志覆盖详情开关",
				content:  "disableAccessLog: true\nlogRequestInfo: true\nlogResponseInfo: true\n",
				disabled: true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var defaultOutput, namedOutput bytes.Buffer
				defaultLogger := logit.MustNew(logit.OptWriter(logit.NewWriter(&defaultOutput)), logit.OptEncoder(logit.DefaultJSONEncoder))
				namedLogger := logit.MustNew(logit.OptWriter(logit.NewWriter(&namedOutput)), logit.OptEncoder(logit.DefaultJSONEncoder))
				previous, previousNamed := logit.Default(), logit.Named("bootstrap-gin-test")
				logit.SetDefault(defaultLogger)
				logit.SetNamed("bootstrap-gin-test", namedLogger)
				t.Cleanup(func() {
					logit.SetDefault(previous)
					logit.SetNamed("bootstrap-gin-test", previousNamed)
					_ = logit.Close(defaultLogger)
					_ = logit.Close(namedLogger)
				})
				writeGinConfig(t, tc.content)
				engine, err := bootstrap.NewGinEngine()
				if err != nil {
					t.Fatal(err)
				}
				engine.POST("/probe", func(c *gin.Context) {
					body, err := io.ReadAll(c.Request.Body)
					if err != nil {
						t.Error(err)
					}
					c.String(http.StatusOK, string(body))
				})
				// 构造后改变文件不应影响现有 Engine，也不应在请求期间重新解析。
				writeGinConfig(t, "unknown: true\n")
				request := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader("probe-body"))
				request.RemoteAddr = "127.0.0.1:1234"
				request.Header.Set("X-Forwarded-For", "198.51.100.7")
				request.Header.Set(ginx.TraceHeader, "probe-trace")
				writer := httptest.NewRecorder()
				engine.ServeHTTP(writer, request)
				if writer.Code != http.StatusOK || writer.Body.String() != "probe-body" || writer.Header().Get(ginx.TraceHeader) != "probe-trace" {
					t.Fatalf("配置不应改变正文读取或日志 ID: %d, %q, %v", writer.Code, writer.Body.String(), writer.Header())
				}
				if tc.disabled {
					if defaultOutput.Len() != 0 || namedOutput.Len() != 0 {
						t.Fatal("关闭访问日志后仍有输出")
					}
					return
				}
				output, unused := &defaultOutput, &namedOutput
				if tc.named {
					output, unused = &namedOutput, &defaultOutput
				}
				if unused.Len() != 0 {
					t.Fatal("请求日志写入了错误的 Logger")
				}
				var record map[string]any
				if err := json.Unmarshal(output.Bytes(), &record); err != nil {
					t.Fatalf("每个请求应写出一条 JSON 日志: %v, %s", err, output.String())
				}
				if record["client_ip"] != tc.clientIP || record["status"] != float64(http.StatusOK) || record["logId"] != "probe-trace" {
					t.Fatalf("请求观察字段错误: %v", record)
				}
				requestInfo, hasRequestInfo := record["request_info"].(map[string]any)
				responseInfo, hasResponseInfo := record["response_info"].(map[string]any)
				if hasRequestInfo != tc.requestInfo || hasResponseInfo != tc.responseInfo {
					t.Fatalf("详情开关未生效: %v", record)
				}
				if hasRequestInfo && requestInfo["body"] != "probe-body" {
					t.Fatalf("请求详情错误: %v", requestInfo)
				}
				if hasResponseInfo && responseInfo["body"] != writer.Body.String() {
					t.Fatalf("响应详情错误: %v", responseInfo)
				}
			})
		}
	})
}

func writeGinConfig(t *testing.T, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "gin.yaml"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}
