package ossservice_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/bpcoder16/pixiu/biz/ossservice"
	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
)

func newMultipartUploadService(t *testing.T, handler http.HandlerFunc) *ossservice.Service {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := ossx.New(ossx.Config{
		Name:            "uploads",
		Region:          "cn-shanghai",
		Bucket:          "upload-bucket",
		Endpoint:        server.URL,
		AccessKeyID:     "oss-id",
		AccessKeySecret: "oss-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	service, err := ossservice.New(client)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func multipartFile(t *testing.T, content string, onDisk bool) (*multipart.Form, *multipart.FileHeader) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreatePart(textproto.MIMEHeader{
		"Content-Disposition": {`form-data; name="file"; filename="client.json"`},
		"Content-Type":        {"application/x-client-only"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(part, content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	maxMemory := int64(len(content))
	if onDisk {
		maxMemory = 0
	}
	form, err := multipart.NewReader(&body, writer.Boundary()).ReadForm(maxMemory)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := form.RemoveAll(); err != nil {
			t.Error(err)
		}
	})
	return form, form.File["file"][0]
}

func TestUploadMultipartWithoutSTS(t *testing.T) {
	const content = "multipart file content\n"
	const key = "uploads/文件 +#.bin"
	for _, onDisk := range []bool{false, true} {
		t.Run(fmt.Sprintf("onDisk=%t", onDisk), func(t *testing.T) {
			var calls atomic.Int32
			service := newMultipartUploadService(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(r.Body)
				if err != nil || string(body) != content || r.Method != http.MethodPut || r.URL.Path != "/upload-bucket/"+key {
					t.Errorf("unexpected upload: %s %s body=%q err=%v", r.Method, r.URL.Path, body, err)
				}
				if r.Header.Get("Content-Type") == "application/x-client-only" {
					t.Error("multipart content type must not be forwarded")
				}
				w.Header().Set("x-oss-request-id", "upload-request")
				w.Header().Set("ETag", `"opaque-etag"`)
				w.Header().Set("x-oss-version-id", "v1")
			})
			form, file := multipartFile(t, content, onDisk)
			// 每次独立打开文件；临时表单仍由 HTTP 调用方持有，上传方法不能提前删除它。
			for range 2 {
				result, err := service.UploadMultipart(context.Background(), key, file)
				if err != nil || result.RequestID != "upload-request" || result.ETag != `"opaque-etag"` || result.VersionID != "v1" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
			if calls.Load() != 2 {
				t.Fatal("unexpected STS request or upload retry")
			}
			if err := form.RemoveAll(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestUploadMultipartLocalFailuresDoNotUpload(t *testing.T) {
	var calls atomic.Int32
	service := newMultipartUploadService(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	_, file := multipartFile(t, "content", false)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		key  string
		file *multipart.FileHeader
		want error
	}{
		{name: "nil file", ctx: context.Background(), key: "file.bin"},
		{name: "empty key", ctx: context.Background(), file: file},
		{name: "nil context", key: "file.bin", file: file},
		{name: "canceled context", ctx: canceled, key: "file.bin", file: file, want: context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := service.UploadMultipart(tc.ctx, tc.key, tc.file)
			if err == nil || result != (ossx.PutObjectResult{}) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("invalid upload result=%+v err=%v", result, err)
			}
		})
	}
	form, missing := multipartFile(t, "disk content", true)
	if err := form.RemoveAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := service.UploadMultipart(context.Background(), "file.bin", missing); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("file open error not preserved: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("invalid upload made a network request")
	}
}

func TestUploadMultipartPreservesOSSErrorWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	service := newMultipartUploadService(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("x-oss-request-id", "failed-upload")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `<Error><Code>ServiceUnavailable</Code><Message>unavailable</Message><RequestId>failed-upload</RequestId></Error>`)
	})
	form, file := multipartFile(t, "disk content", true)
	_, err := service.UploadMultipart(context.Background(), "file.bin", file)
	var serviceErr *oss.ServiceError
	if !errors.As(err, &serviceErr) || serviceErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("OSS error chain not preserved: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("failed upload retried")
	}
	if err := form.RemoveAll(); err != nil {
		t.Fatal(err)
	}
}
