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
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/bpcoder16/pixiu/logit"
)

func TestLocallyRejectedOperationsHaveNoDuration(t *testing.T) {
	buf := captureLogs(t)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("rejected operation reached server") }))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL), OptLogRequests(true))
	ctx := logit.WithStart(context.Background())
	for _, req := range []PutObjectRequest{
		{},
		{Key: "key"},
	} {
		if _, err := c.PutObject(ctx, req); err == nil {
			t.Fatal("invalid upload accepted")
		}
	}
	for _, req := range []PutFileRequest{
		{Key: "key"},
		{FilePath: filepath.Join(t.TempDir(), "absent.txt")},
		{
			Key:      "key",
			FilePath: filepath.Join(t.TempDir(), "absent.txt"),
		},
	} {
		_, err := c.PutFile(ctx, req)
		pathErr, ok := errors.AsType[*os.PathError](err)
		if !ok || pathErr.Path != req.FilePath || !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("应保留 os.Open 错误链: %v", err)
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = c.PutObject(cancelled, PutObjectRequest{
		Key:  "key",
		Body: strings.NewReader("data"),
	})
	_ = c.Close()
	_, _ = c.PutObject(ctx, PutObjectRequest{
		Key:  "key",
		Body: strings.NewReader("data"),
	})
	if buf.Len() != 0 {
		t.Fatalf("rejected operation logged: %s", buf.String())
	}
	logit.InfoDuration(ctx, "done")
	if strings.Contains(buf.String(), "AliyunOSS_files_") {
		t.Fatalf("rejected operation recorded duration: %s", buf.String())
	}
}

func TestSDKKeyValidation(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("SDK 应在发送请求前拒绝无效 Key")
	}))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL), OptLogRequests(true))
	path := filepath.Join(t.TempDir(), "data.txt")
	if err := os.WriteFile(path, []byte("data"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		invoke func(context.Context) error
	}{
		{"PutObject", func(ctx context.Context) error {
			_, err := c.PutObject(ctx, PutObjectRequest{Body: strings.NewReader("data")})
			return err
		}},
		{"PutFile", func(ctx context.Context) error {
			_, err := c.PutFile(ctx, PutFileRequest{FilePath: path})
			return err
		}},
		{"HeadObject", func(ctx context.Context) error {
			_, err := c.HeadObject(ctx, "")
			return err
		}},
		{"PresignGetObject", func(ctx context.Context) error {
			_, err := c.PresignGetObject(ctx, "", time.Minute)
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf := captureLogs(t)
			ctx := logit.WithStart(context.Background())
			err := tc.invoke(ctx)
			sdkErr, ok := errors.AsType[oss.InvalidParamError](err)
			if !ok || !strings.HasSuffix(sdkErr.Field(), ".Key") {
				t.Fatalf("应保留 SDK 的 Key 校验错误: %v", err)
			}
			logged := tc.name != "PresignGetObject"
			if logged {
				var record map[string]any
				if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				details := record[logit.DownstreamDetailsKey].(map[string]any)
				if record["level"] != "ERROR" || details["operation"] != tc.name || details["error_code"] != "client_error" {
					t.Fatalf("SDK 参数错误未记录为客户端错误: %v", record)
				}
			} else if buf.Len() != 0 {
				t.Fatalf("本地签名不应记录下游日志: %s", buf.String())
			}
			buf.Reset()
			logit.InfoDuration(ctx, "done")
			if strings.Contains(buf.String(), "AliyunOSS_files_1_duration_ms") != logged {
				t.Fatalf("SDK 参数错误的耗时登记不正确: %s", buf.String())
			}
		})
	}
}

func TestSDKKeyValidationClosesOwnedFile(t *testing.T) {
	c := newTestClient(t, testConfig("http://127.0.0.1:1"))
	file, err := os.CreateTemp(t.TempDir(), "upload")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	_, err = c.put(context.Background(), "PutFile", PutObjectRequest{Body: file}, file)
	if _, ok := errors.AsType[oss.InvalidParamError](err); !ok {
		t.Fatalf("应保留 SDK 参数错误: %v", err)
	}
	if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("SDK 拒绝 Key 后未关闭自有文件: %v", err)
	}
}

type failingReader struct{ err error }

func (r failingReader) Read([]byte) (int, error) { return 0, r.err }

func TestReaderErrorAndOwnership(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL))
	want := errors.New("read failed")
	body := &borrowedReader{Reader: failingReader{err: want}}
	_, err := c.PutObject(context.Background(), PutObjectRequest{
		Key:  "key",
		Body: body,
	})
	if !errors.Is(err, want) || body.closed {
		t.Fatalf("reader error/ownership: err=%v closed=%v", err, body.closed)
	}
}
