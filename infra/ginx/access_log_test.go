package ginx

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

func captureAccessLogs(t *testing.T, options ...logit.Option) *bytes.Buffer {
	t.Helper()
	var output bytes.Buffer
	opts := []logit.Option{
		logit.OptWriter(logit.NewWriter(&output)),
		logit.OptEncoder(logit.DefaultJSONEncoder),
	}
	logger := logit.MustNew(append(opts, options...)...)
	previous := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(previous)
		_ = logit.Close(logger)
	})
	return &output
}

func TestAccessLogDetailSwitches(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		for _, requestInfo := range []bool{false, true} {
			for _, responseInfo := range []bool{false, true} {
				t.Run(fmt.Sprintf("关闭日志=%t/请求=%t/响应=%t", disabled, requestInfo, responseInfo), func(t *testing.T) {
					output := captureAccessLogs(t)
					const body = `{"password":"secret"}`
					input := &accessLogBody{Reader: strings.NewReader(body)}
					var observations int
					router, err := New(Config{
						DisableAccessLog: disabled,
						LogRequestInfo:   requestInfo,
						LogResponseInfo:  responseInfo,
						Observe: func(context.Context, RequestResult) {
							observations++
						},
					})
					if err != nil {
						t.Fatal(err)
					}
					router.POST("/items/:id", func(c *gin.Context) {
						if (input.reads > 0) != (!disabled && requestInfo) {
							t.Errorf("详情开关与请求预读不一致: reads=%d", input.reads)
						}
						if !disabled && requestInfo {
							got, err := io.ReadAll(c.Request.Body)
							if err != nil || string(got) != body {
								t.Errorf("业务读取正文变化: %q %v", got, err)
							}
						}
						c.Request.Header.Set("Authorization", "业务修改")
						c.Header("X-Reply", "secret-response")
						c.Status(http.StatusCreated)
						_, _ = c.Writer.WriteString("first-")
						_, _ = c.Writer.Write([]byte("second"))
						c.Header("X-Reply", "提交后修改")
					})
					req := httptest.NewRequest(http.MethodPost, "/items/42?token=secret", nil)
					req.Host = "example.test"
					req.Body = input
					req.ContentLength = int64(len(body))
					req.Header.Set("Authorization", "Bearer secret")
					req.Header.Add("X-Multi", "one")
					req.Header.Add("X-Multi", "two")
					writer := httptest.NewRecorder()
					router.ServeHTTP(writer, req)
					if writer.Code != http.StatusCreated || writer.Body.String() != "first-second" || observations != 1 {
						t.Fatalf("请求结果变化: %d %q, Observe=%d", writer.Code, writer.Body.String(), observations)
					}
					records := accessLogRecords(t, output)
					if disabled {
						if len(records) != 0 || input.reads != 0 {
							t.Fatalf("关闭访问日志后仍记录或预读: logs=%v reads=%d", records, input.reads)
						}
						return
					}
					if len(records) != 1 {
						t.Fatalf("日志条数: %d", len(records))
					}
					record := records[0]
					request, hasRequest := record["request_info"].(map[string]any)
					response, hasResponse := record["response_info"].(map[string]any)
					if hasRequest != requestInfo || hasResponse != responseInfo {
						t.Fatalf("详情开关未独立生效: %v", record)
					}
					if requestInfo {
						headers := request["headers"].(map[string]any)
						if _, hasHost := headers["Host"]; hasHost {
							t.Fatalf("请求详情不应补充 Host: %v", headers)
						}
						if _, hasURL := request["url"]; hasURL {
							t.Fatalf("请求详情不应包含 URL: %v", request)
						}
						if request["method"] != http.MethodPost ||
							request["body"] != body || request["body_encoding"] != "utf-8" ||
							headers["Authorization"].([]any)[0] != "Bearer secret" ||
							len(headers["X-Multi"].([]any)) != 2 {
							t.Fatalf("请求详情丢失原始信息: %v", request)
						}
					}
					if responseInfo {
						headers := response["headers"].(map[string]any)
						if _, hasStatus := response["status"]; hasStatus {
							t.Fatalf("响应详情不应包含 status: %v", response)
						}
						if record["status"] != float64(http.StatusCreated) || response["body"] != "first-second" ||
							response["body_encoding"] != "utf-8" || headers["X-Reply"].([]any)[0] != "secret-response" {
							t.Fatalf("响应详情不准确: %v", response)
						}
					}
				})
			}
		}
	}
}

func TestAccessLogRequestReadError(t *testing.T) {
	output := captureAccessLogs(t)
	failure := errors.New("读取中断")
	input := &accessLogBody{
		Reader: strings.NewReader("partial"),
		err:    failure,
	}
	router, err := New(Config{LogRequestInfo: true})
	if err != nil {
		t.Fatal(err)
	}
	router.POST("/", func(c *gin.Context) {
		defer c.Request.Body.Close()
		body, err := io.ReadAll(c.Request.Body)
		if !errors.Is(err, failure) || string(body) != "partial" {
			t.Errorf("预读吞掉正文或错误: %q %v", body, err)
		}
		c.Status(http.StatusBadRequest)
	})
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = input
	router.ServeHTTP(httptest.NewRecorder(), req)
	if !input.closed {
		t.Fatal("未关闭原始 Body")
	}
	request := accessLogRecords(t, output)[0]["request_info"].(map[string]any)
	if request["body"] != "partial" || request["body_error"] != failure.Error() {
		t.Fatalf("未标记不完整正文: %v", request)
	}
}

// 模拟可观察读取和关闭的 Body,错误只返回一次以验证预读后的错误还原。
type accessLogBody struct {
	io.Reader
	reads  int
	closed bool
	err    error
}

func (b *accessLogBody) Read(p []byte) (int, error) {
	b.reads++
	n, err := b.Reader.Read(p)
	if err == io.EOF && b.err != nil {
		err = b.err
		b.err = nil
	}
	return n, err
}

func (b *accessLogBody) Close() error {
	b.closed = true
	return nil
}

func TestAccessLogBodiesNotTruncated(t *testing.T) {
	for _, body := range [][]byte{[]byte(strings.Repeat("正文", 128*1024)), {0xff, 0xfe, 0, 1}} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			output := captureAccessLogs(t)
			router, err := New(Config{
				LogRequestInfo:  true,
				LogResponseInfo: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			router.POST("/", func(c *gin.Context) {
				if _, err := io.Copy(c.Writer, c.Request.Body); err != nil {
					t.Error(err)
				}
			})
			writer := httptest.NewRecorder()
			router.ServeHTTP(writer, httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body)))
			if !bytes.Equal(writer.Body.Bytes(), body) {
				t.Fatal("业务响应正文被修改")
			}
			record := accessLogRecords(t, output)[0]
			for _, key := range []string{"request_info", "response_info"} {
				info := record[key].(map[string]any)
				got := []byte(info["body"].(string))
				if info["body_encoding"] == "base64" {
					got, err = base64.StdEncoding.DecodeString(string(got))
					if err != nil {
						t.Fatal(err)
					}
				}
				if !bytes.Equal(got, body) {
					t.Fatalf("%s 正文未完整保留", key)
				}
			}
		})
	}
}

func TestAccessLogStreamingResponse(t *testing.T) {
	output := captureAccessLogs(t)
	continueResponse := make(chan struct{})
	observed := make(chan struct{})
	errorsSeen := make(chan error, 3)
	router, err := New(Config{
		LogRequestInfo:  true,
		LogResponseInfo: true,
		Observe: func(context.Context, RequestResult) {
			close(observed)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	router.POST("/", func(c *gin.Context) {
		ctrl := http.NewResponseController(c.Writer)
		errorsSeen <- ctrl.SetReadDeadline(time.Now().Add(3 * time.Second))
		errorsSeen <- ctrl.SetWriteDeadline(time.Now().Add(3 * time.Second))
		errorsSeen <- ctrl.EnableFullDuplex()
		c.Header("X-Reply", "first")
		c.Writer.Flush()
		c.Header("X-Reply", "late")
		_, _ = c.Writer.WriteString("first\n")
		c.Writer.Flush()
		select {
		case <-continueResponse:
		case <-c.Request.Context().Done():
			return
		}
		_, _ = c.Writer.Write([]byte("second\n"))
	})
	server := httptest.NewServer(router)
	defer server.Close()
	client := server.Client()
	client.Timeout = 3 * time.Second
	resp, err := client.Post(server.URL, "text/plain", strings.NewReader("request"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	close(continueResponse)
	if err != nil || first != "first\n" {
		t.Fatalf("Flush 未立即发送首段: %q %v", first, err)
	}
	rest, err := io.ReadAll(reader)
	if err != nil || string(rest) != "second\n" {
		t.Fatalf("后续响应: %q %v", rest, err)
	}
	select {
	case <-observed:
	case <-time.After(3 * time.Second):
		t.Fatal("未收到请求结果")
	}
	for range 3 {
		if err := <-errorsSeen; err != nil {
			t.Errorf("ResponseController 能力丢失: %v", err)
		}
	}
	record := accessLogRecords(t, output)[0]
	response := record["response_info"].(map[string]any)
	if response["body"] != "first\nsecond\n" || response["headers"].(map[string]any)["X-Reply"].([]any)[0] != "first" {
		t.Fatalf("流式响应详情不准确: %v", response)
	}
}

func TestAccessLogHijackStillSkipped(t *testing.T) {
	output := captureAccessLogs(t)
	var observations int
	finished := make(chan struct{})
	upgradeErrors := make(chan error, 1)
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
	router.GET("/", func(c *gin.Context) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		upgradeErrors <- err
		if err == nil {
			_ = conn.Close()
		}
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		router.ServeHTTP(w, req)
		close(finished)
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("升级后 HTTP 中间件未退出")
	}
	if err := <-upgradeErrors; err != nil {
		t.Fatal(err)
	}
	if observations != 0 || output.Len() != 0 {
		t.Fatalf("Hijack 后仍输出 HTTP 结果: Observe=%d logs=%s", observations, output.String())
	}
}

func accessLogRecords(t *testing.T, output *bytes.Buffer) []map[string]any {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var records []map[string]any
	for {
		var record map[string]any
		if err := decoder.Decode(&record); err == io.EOF {
			return records
		} else if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
}

func TestAccessLogAlwaysInfoDuration(t *testing.T) {
	for _, outcome := range []string{"success", "client-error", "server-error", "panic", "committed-panic", "abort"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				output := captureAccessLogs(t)
				router, err := New(Config{})
				if err != nil {
					t.Fatal(err)
				}
				router.GET("/", func(c *gin.Context) {
					time.Sleep(5 * time.Millisecond)
					logit.AddDownstreamDurationAuto(c.Request.Context(), "db", 2*time.Millisecond)
					switch outcome {
					case "client-error":
						c.Status(http.StatusBadRequest)
					case "server-error":
						c.Status(http.StatusInternalServerError)
					case "panic":
						panic("业务异常")
					case "committed-panic":
						c.String(http.StatusAccepted, "已提交")
						panic("提交后异常")
					case "abort":
						panic(http.ErrAbortHandler)
					default:
						c.Status(http.StatusNoContent)
					}
				})
				var recovered any
				func() {
					defer func() { recovered = recover() }()
					router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
				}()
				if outcome == "abort" {
					if recovered != http.ErrAbortHandler {
						t.Fatalf("未保留 HTTP 中断: %v", recovered)
					}
				} else if recovered != nil {
					t.Fatalf("意外 panic: %v", recovered)
				}
				var accessCount int
				for _, record := range accessLogRecords(t, output) {
					if record["msg"] == "HTTPPanic" {
						if record["level"] != "ERROR" {
							t.Fatalf("独立 panic 日志级别变化: %v", record)
						}
						continue
					}
					accessCount++
					if record["msg"] != "HTTPRequest" || record["level"] != "INFO" {
						t.Fatalf("访问日志未固定为 Info: %v", record)
					}
					if _, hasDuration := record["duration_ms"]; hasDuration {
						t.Fatalf("访问日志不应单独输出 duration_ms: %v", record)
					}
					for field, want := range map[string]float64{
						"db_1_duration_ms":  2,
						"self_duration_ms":  3,
						"total_duration_ms": 5,
					} {
						if record[field] != want {
							t.Errorf("%s = %v, want %v", field, record[field], want)
						}
					}
				}
				if accessCount != 1 {
					t.Fatalf("访问日志条数: %d", accessCount)
				}
			})
		})
	}
}

func TestAccessLogInfoFiltering(t *testing.T) {
	output := captureAccessLogs(t, logit.OptMinLevel(logit.WarnLevel))
	input := &accessLogBody{Reader: strings.NewReader("不应预读的正文")}
	var observed bool
	router, err := New(Config{
		LogRequestInfo:  true,
		LogResponseInfo: true,
		Observe: func(context.Context, RequestResult) {
			observed = true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	router.GET("/", func(c *gin.Context) {
		if c.Request.Body != input {
			t.Error("Info 被过滤时仍替换了请求 Body")
		}
		c.String(http.StatusInternalServerError, "failure")
	})
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Body = input
	router.ServeHTTP(httptest.NewRecorder(), req)
	if input.reads != 0 {
		t.Errorf("Info 被过滤时仍预读请求正文: reads=%d", input.reads)
	}
	if output.Len() != 0 || !observed {
		t.Fatalf("Info 过滤未生效或影响了 Observe: logs=%s observed=%t", output.String(), observed)
	}
}

func TestAccessLogLoggerChangedDuringRequest(t *testing.T) {
	name := t.Name()
	previous := logit.Named(name)
	logger := logit.MustNew(
		logit.OptMinLevel(logit.WarnLevel),
		logit.OptWriter(logit.NewWriter(io.Discard)),
	)
	logit.SetNamed(name, logger)
	t.Cleanup(func() {
		logit.SetNamed(name, previous)
		_ = logit.Close(logger)
	})
	output := captureAccessLogs(t)
	input := &accessLogBody{Reader: strings.NewReader("不应补采的正文")}
	router, err := New(Config{
		LoggerName:      name,
		LogRequestInfo:  true,
		LogResponseInfo: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	router.POST("/", func(c *gin.Context) {
		// 入口命名 Logger 过滤 Info,业务切换到启用 Info 的默认 Logger。
		ctx := logit.WithLoggerName(c.Request.Context(), "")
		c.Request = c.Request.WithContext(ctx)
		c.String(http.StatusOK, "ok")
	})
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Body = input
	router.ServeHTTP(httptest.NewRecorder(), req)
	if input.reads != 0 {
		t.Errorf("入口 Info 被过滤时仍预读请求正文: reads=%d", input.reads)
	}
	if records := accessLogRecords(t, output); len(records) != 0 {
		t.Fatalf("入口未启用 Info 时,中途更换 Logger 后仍输出访问日志: %v", records)
	}
}

func TestAccessLogPartialResponseWrite(t *testing.T) {
	for _, writeString := range []bool{false, true} {
		t.Run(fmt.Sprint(writeString), func(t *testing.T) {
			output := captureAccessLogs(t)
			router, err := New(Config{LogResponseInfo: true})
			if err != nil {
				t.Fatal(err)
			}
			router.GET("/", func(c *gin.Context) {
				c.Header("X-Reply", "submitted")
				c.Writer.WriteHeaderNow()
				c.Header("X-Reply", "late")
				var n int
				var err error
				if writeString {
					n, err = c.Writer.WriteString("abcdef")
				} else {
					n, err = c.Writer.Write([]byte("abcdef"))
				}
				if n != 3 || err != io.ErrShortWrite {
					t.Errorf("部分写入结果变化: %d %v", n, err)
				}
			})
			writer := &partialAccessLogWriter{recorder: httptest.NewRecorder()}
			router.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/", nil))
			record := accessLogRecords(t, output)[0]
			response := record["response_info"].(map[string]any)
			if record["bytes"] != float64(3) || response["body"] != "abc" ||
				response["headers"].(map[string]any)["X-Reply"].([]any)[0] != "submitted" {
				t.Fatalf("记录了未写入的正文或未提交的 Header: %v", record)
			}
		})
	}
}

type partialAccessLogWriter struct {
	recorder *httptest.ResponseRecorder
}

func (w *partialAccessLogWriter) Header() http.Header {
	return w.recorder.Header()
}

func (w *partialAccessLogWriter) WriteHeader(status int) {
	w.recorder.WriteHeader(status)
}

func (w *partialAccessLogWriter) Write(p []byte) (int, error) {
	n, _ := w.recorder.Write(p[:min(len(p), 3)])
	return n, io.ErrShortWrite
}
