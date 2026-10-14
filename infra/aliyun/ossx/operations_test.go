package ossx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/credentials-go/credentials"
	"github.com/bpcoder16/pixiu/logit"
)

// 嵌入接口让旧的分次取值方法一旦被调用就失败，确保每次使用完整凭据快照。
type rotatingCredential struct {
	credentials.Credential
	version atomic.Int64
}

func (c *rotatingCredential) GetCredential() (*credentials.CredentialModel, error) {
	id, secret, token := "oss-account-one", "secret", "token-one"
	if c.version.Load() > 0 {
		id, token = "oss-account-two", "token-two"
	}
	typ := "sts"
	return &credentials.CredentialModel{
		AccessKeyId:     &id,
		AccessKeySecret: &secret,
		SecurityToken:   &token,
		Type:            &typ,
	}, nil
}

func testConfig(endpoint string) Config {
	return Config{
		Name:          "files",
		Bucket:        "dict-bucket",
		Region:        "cn-hangzhou",
		Endpoint:      endpoint,
		Credential:    &rotatingCredential{},
		SlowThreshold: time.Hour,
	}
}

func newTestClient(t *testing.T, cfg Config, opts ...Option) *Client {
	t.Helper()
	c, err := New(cfg, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

type borrowedReader struct {
	io.Reader
	closed bool
}

func (r *borrowedReader) Close() error {
	r.closed = true
	return nil
}

func TestUploadAndDynamicCredentials(t *testing.T) {
	const content = "125th,125周年\n龙纹折 => dragon,龙纹,龙纹折\n"
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != content || r.Method != http.MethodPut || r.URL.Path != "/dict-bucket/synonyms/词库 +.txt" || r.Header.Get("Content-Type") != "text/plain; charset=utf-8" {
			t.Errorf("unexpected upload: %s %s body=%q err=%v", r.Method, r.URL, body, err)
		}
		id, token := "oss-account-one", "token-one"
		if n == 2 {
			id, token = "oss-account-two", "token-two"
		}
		if !strings.Contains(r.Header.Get("Authorization"), id) || r.Header.Get("x-oss-security-token") != token {
			t.Error("credentials were not refreshed")
		}
		w.Header().Set("x-oss-request-id", "request-oss")
		w.Header().Set("ETag", `"opaque-etag"`)
		w.Header().Set("x-oss-version-id", "v2")
	}))
	defer server.Close()
	cfg := testConfig(server.URL)
	c := newTestClient(t, cfg)
	if calls.Load() != 0 {
		t.Fatal("New performed a request")
	}
	reader := &borrowedReader{Reader: strings.NewReader(content)}
	result, err := c.PutObject(context.Background(), PutObjectRequest{
		Key:         "synonyms/词库 +.txt",
		Body:        reader,
		ContentType: "text/plain; charset=utf-8",
	})
	if err != nil || result.RequestID != "request-oss" || result.ETag != `"opaque-etag"` || result.VersionID != "v2" || reader.closed {
		t.Fatalf("result=%+v err=%v closed=%v", result, err, reader.closed)
	}
	cfg.Credential.(*rotatingCredential).version.Store(1)
	path := filepath.Join(t.TempDir(), "dict.txt")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutFile(context.Background(), PutFileRequest{
		Key:         "synonyms/词库 +.txt",
		FilePath:    path,
		ContentType: "text/plain; charset=utf-8",
	}); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestStaticCredentialsAndFixedBuckets(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil || string(body) != "data" {
			t.Errorf("上传内容错误: %q, %v", body, err)
		}
		bucket := strings.Split(r.URL.Path, "/")[1]
		if (bucket != "assets-bucket" && bucket != "archive-bucket") || !strings.Contains(r.Header.Get("Authorization"), "Credential="+bucket+"/") {
			t.Errorf("Bucket 与账号未保持隔离: %s", r.URL.Path)
		}
		if r.Header.Get("x-oss-security-token") != "" {
			t.Error("固定 AK/SK 不应携带 STS Token")
		}
		w.Header().Set("x-oss-request-id", bucket)
	}))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "data.txt")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	clients := make([]*Client, 0, 2)
	for _, bucket := range []string{"assets-bucket", "archive-bucket"} {
		clients = append(clients, newTestClient(t, Config{
			Name:            bucket,
			Region:          "cn-hangzhou",
			Bucket:          bucket,
			AccessKeyID:     bucket,
			AccessKeySecret: "test-secret",
			Endpoint:        server.URL,
		}))
	}
	if calls.Load() != 0 {
		t.Fatal("初始化不应发起请求")
	}
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Go(func() {
			for range 3 {
				if _, err := c.PutObject(context.Background(), PutObjectRequest{
					Key:  "object.txt",
					Body: strings.NewReader("data"),
				}); err != nil {
					t.Error(err)
				}
				if _, err := c.PutFile(context.Background(), PutFileRequest{
					Key:      "file.txt",
					FilePath: path,
				}); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if calls.Load() != 12 {
		t.Fatalf("上传次数错误: %d", calls.Load())
	}
}

func TestServiceErrorNoRetryAndCRC(t *testing.T) {
	for _, status := range []int{403, 503, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			buf := captureLogs(t)
			var calls atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("x-oss-request-id", "failed-id")
				w.Header().Set("x-oss-hash-crc64ecma", "1")
				w.WriteHeader(status)
				if status != 200 {
					_, _ = io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>secret-payload</Message><RequestId>failed-id</RequestId></Error>`)
				}
			}))
			defer s.Close()
			c := newTestClient(t, testConfig(s.URL), OptLogRequests(true))
			_, err := c.PutObject(context.Background(), PutObjectRequest{
				Key:  "test.txt",
				Body: strings.NewReader("data"),
			})
			if err == nil || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
			if status != 200 {
				var sdkErr *oss.ServiceError
				if !errors.As(err, &sdkErr) || sdkErr.StatusCode != status {
					t.Fatalf("lost SDK error: %v", err)
				}
			} else if !strings.Contains(err.Error(), "crc is inconsistent") {
				t.Fatalf("未保留 CRC 校验错误: %v", err)
			}
			var record map[string]any
			if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			details := record[logit.DownstreamDetailsKey].(map[string]any)
			if details["status"] != float64(status) || details["request_id"] != "failed-id" {
				t.Fatalf("失败日志丢失响应信息: %v", record)
			}
			if status == 200 && (record["level"] != "ERROR" || details["error_code"] != "client_error") {
				t.Fatalf("CRC 校验失败未记录为客户端错误: %v", record)
			}
			if strings.Contains(buf.String(), "secret-payload") || strings.Contains(buf.String(), "crc is inconsistent") {
				t.Fatalf("日志包含响应正文或原始错误: %s", buf.String())
			}
		})
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, callerTimeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "config", true: "caller"}[callerTimeout], func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				<-r.Context().Done()
			}))
			defer s.Close()
			cfg := testConfig(s.URL)
			cfg.RequestTimeout = 40 * time.Millisecond
			ctx := context.Background()
			if callerTimeout {
				cfg.RequestTimeout = time.Hour
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 40*time.Millisecond)
				defer cancel()
			}
			c := newTestClient(t, cfg)
			_, err := c.PutObject(ctx, PutObjectRequest{
				Key:  "test.txt",
				Body: strings.NewReader("data"),
			})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline: %v", err)
			}
		})
	}
}

func TestErrorResponseBodyTimeoutPreservesErrorChain(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-oss-request-id", "timeout-id")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusForbidden)
		// 先发送错误响应头，再阻塞正文，覆盖 SDK 已开始解析服务错误的超时路径。
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer s.Close()
	cfg := testConfig(s.URL)
	cfg.RequestTimeout = 200 * time.Millisecond
	c := newTestClient(t, cfg)
	_, err := c.PutObject(context.Background(), PutObjectRequest{
		Key:  "test.txt",
		Body: strings.NewReader("data"),
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("错误响应正文超时应保留 context 原因: %v", err)
	}
	var sdkErr *oss.ServiceError
	if !errors.As(err, &sdkErr) || sdkErr.StatusCode != http.StatusForbidden || sdkErr.RequestID != "timeout-id" {
		t.Fatalf("补充超时原因后仍须保留 SDK 错误及响应信息: %v", err)
	}
}
