package ginx

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/gin-gonic/gin"
)

const TraceHeader = "PIXIU-Log-Id"

// Config 只配置当前 Engine;回调须并发安全并快速完成。
type Config struct {
	// TrustedProxies 指定可信代理的 IP 或 CIDR;为空时不信任任何代理。
	TrustedProxies []string

	// LoggerName 指定请求使用的命名 Logger;为空时沿用请求 context 的 Logger 路由。
	LoggerName string

	// DisableAccessLog 关闭 HTTP 访问日志,不影响 Observe 回调或 panic 错误日志。
	DisableAccessLog bool

	// LogRequestInfo 在访问日志中记录请求 method、Header 和 body,默认关闭。
	// 开启后完整预读并还原 body,不截断或脱敏;DisableAccessLog 为 true 时不生效。
	// 进入观察中间件时请求 Logger 未启用 Info 则跳过采集,后续开启也不补采。
	LogRequestInfo bool

	// LogResponseInfo 在访问日志中记录响应 Header 和 body,默认关闭。
	// 开启后随写出复制 body,不截断或脱敏;DisableAccessLog 为 true 时不生效。
	// 进入观察中间件时请求 Logger 未启用 Info 则跳过采集,后续开启也不补采。
	LogResponseInfo bool

	// Middlewares 在内置请求作用域、结果观察和 Recovery 之后按顺序注册;元素不能为 nil。
	Middlewares []gin.HandlerFunc

	// Observe 在请求处理结束时同步接收结果;为 nil 时不调用,成功 Hijack 的请求跳过观察。
	// 回调须并发安全并快速完成;回调 panic 会被恢复并记录错误日志。
	Observe func(context.Context, RequestResult)
}

// RequestResult 提供一次普通 HTTP 请求的结果,Route 使用模板或 unmatched。
type RequestResult struct {
	// Method 是 HTTP 请求方法,如 GET、POST。
	Method string

	// Route 是匹配的路由模板,如 /items/:id;未匹配时为 unmatched。
	Route string

	// Status 是 Gin Writer 记录的 HTTP 状态码;因 http.ErrAbortHandler 中断且未写出响应时为 0。
	Status int

	// Bytes 是已写入的响应体字节数,不含响应头;未写入时为 0。
	Bytes int

	// ClientIP 是 Gin 根据连接对端和可信代理等配置解析的客户端 IP。
	ClientIP string

	// Duration 是从进入结果观察中间件到采集结果时的耗时,不含后续日志和 Observe 回调。
	Duration time.Duration

	// Panicked 表示内置 Recovery 捕获了普通 panic,不含 http.ErrAbortHandler。
	Panicked bool

	// Aborted 表示捕获到 http.ErrAbortHandler;普通 c.Abort() 不会设置此标记。
	Aborted bool
}

type panicKey struct{}
type abortKey struct{}

// New 创建独立 Engine,默认关闭自动重定向,不改变 Gin 全局模式、Validator 或注册管理端点。
func New(cfg Config) (*gin.Engine, error) {
	for _, middleware := range cfg.Middlewares {
		if middleware == nil {
			return nil, errors.New("ginx: nil middleware")
		}
	}
	r := gin.New()
	// Gin 自动重定向在中间件之前返回;默认让不匹配路径进入可观测的 404 流程。
	r.RedirectTrailingSlash = false
	r.RedirectFixedPath = false
	if err := r.SetTrustedProxies(append([]string(nil), cfg.TrustedProxies...)); err != nil {
		return nil, err
	}
	r.Use(requestContext(cfg.LoggerName), observe(cfg), recovery())
	r.Use(cfg.Middlewares...)
	return r, nil
}

func requestContext(loggerName string) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := logit.NewContextScope(c.Request.Context())
		ctx = logit.WithStart(ctx)
		if loggerName != "" {
			ctx = logit.WithLoggerName(ctx, loggerName)
		}
		id := c.GetHeader(TraceHeader)
		if id == "" {
			id = logit.NewLogID()
		}
		logit.AddMeta(ctx, logit.Str(logit.LogId, id))
		c.Request = c.Request.WithContext(ctx)
		c.Header(TraceHeader, id)
		c.Next()
	}
}

// responseWriter 观察 Hijack,并按配置复制响应详情,其余 Gin writer 能力保持透传。
type responseWriter struct {
	gin.ResponseWriter
	hijacked bool
	body     *bytes.Buffer
	headers  http.Header
}

// Unwrap 让 ResponseController 可继续访问底层 deadline 和全双工控制。
func (w *responseWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.Hijack()
	if err == nil {
		w.hijacked = true
	}
	return conn, rw, err
}

func observe(cfg Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		started := time.Now()
		initialStatus := c.Writer.Status()
		writer := &responseWriter{ResponseWriter: c.Writer}
		var request *requestInfo
		// 按入口状态控制本次访问日志,中途启用 Info 不补记。
		accessLogEnabled := !cfg.DisableAccessLog && logit.InfoEnabled(c.Request.Context())
		if accessLogEnabled {
			if cfg.LogRequestInfo {
				request = captureRequestInfo(c.Request)
			}
			if cfg.LogResponseInfo {
				writer.body = &bytes.Buffer{}
			}
		}
		c.Writer = writer
		defer func() {
			if writer.hijacked {
				return
			}
			route := c.FullPath()
			if route == "" {
				route = "unmatched"
			}
			panicked, _ := c.Request.Context().Value(panicKey{}).(bool)
			aborted, _ := c.Request.Context().Value(abortKey{}).(bool)
			result := RequestResult{
				Method:   c.Request.Method,
				Route:    route,
				Status:   c.Writer.Status(),
				Bytes:    max(c.Writer.Size(), 0),
				ClientIP: c.ClientIP(),
				Duration: time.Since(started),
				Panicked: panicked,
				Aborted:  aborted,
			}
			if aborted && !c.Writer.Written() {
				result.Status = 0
			}
			ctx := c.Request.Context()
			if accessLogEnabled {
				logResult(ctx, result, request, writer)
			}
			if cfg.Observe != nil {
				callObserver(ctx, cfg.Observe, result)
			}
		}()
		c.Next()
		// Gin 在中间件退出后才补写默认 404/405;提前完成相同回退,让结果包含完整响应。
		// 保留业务已写出的响应及状态变更;中断重新 panic 时不会执行到这里。
		if !c.Writer.Written() && c.Writer.Status() == initialStatus {
			var body string
			switch initialStatus {
			case http.StatusNotFound:
				body = "404 page not found"
			case http.StatusMethodNotAllowed:
				body = "405 method not allowed"
			default:
				return
			}
			c.Header("Content-Type", gin.MIMEPlain)
			_, _ = c.Writer.Write([]byte(body))
		}
	}
}

func logResult(ctx context.Context, result RequestResult, request *requestInfo, writer *responseWriter) {
	fields := make([]logit.Field, 0, 10)
	fields = append(fields,
		logit.Str("method", result.Method),
		logit.Str("route", result.Route),
		logit.Int("status", result.Status),
		logit.Int("bytes", result.Bytes),
		logit.Str("client_ip", result.ClientIP),
		logit.Bool("panicked", result.Panicked),
		logit.Bool("aborted", result.Aborted),
	)
	if request != nil {
		fields = append(fields, logit.Any("request_info", request))
	}
	if writer.body != nil {
		writer.captureHeaders()
		body, encoding := bodyForLog(writer.body.Bytes())
		fields = append(fields, logit.Any("response_info", responseInfo{
			Headers:      writer.headers,
			Body:         body,
			BodyEncoding: encoding,
		}))
	}
	logit.InfoDuration(ctx, "HTTPRequest", fields...)
}

func callObserver(ctx context.Context, fn func(context.Context, RequestResult), result RequestResult) {
	defer func() {
		if value := recover(); value != nil {
			logit.Error(ctx, "HTTPObserverPanic", logit.Any("panic", value), logit.Str("stack", string(debug.Stack())))
		}
	}()
	fn(ctx, result)
}

func recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if value := recover(); value != nil {
				if value == http.ErrAbortHandler {
					c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), abortKey{}, true))
					panic(value)
				}
				ctx := context.WithValue(c.Request.Context(), panicKey{}, true)
				c.Request = c.Request.WithContext(ctx)
				logit.Error(ctx, "HTTPPanic", logit.Any("panic", value), logit.Str("stack", string(debug.Stack())))
				c.Abort()
				if !c.Writer.Written() {
					c.Status(http.StatusInternalServerError)
				}
			}
		}()
		c.Next()
	}
}
