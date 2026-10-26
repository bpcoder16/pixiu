package ossx

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
)

func TestHeadAndPresign(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "HEAD" {
			t.Errorf("method=%s", r.Method)
		}
		switch {
		case strings.HasSuffix(r.URL.Path, "missing.json"):
			w.WriteHeader(404)
		case strings.HasSuffix(r.URL.Path, "denied.json"):
			w.WriteHeader(403)
		default:
			w.Header().Set("Content-Length", "12")
			w.Header().Set("Content-Type", "application/json")
		}
	}))
	defer server.Close()
	c := newTestClient(t, testConfig(server.URL))
	info, err := c.HeadObject(context.Background(), "config/a.json")
	if err != nil || info.ContentLength != 12 || info.ContentType != "application/json" {
		t.Fatalf("head=%+v err=%v", info, err)
	}
	if _, err = c.HeadObject(context.Background(), "missing.json"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("missing: %v", err)
	}
	if _, err = c.HeadObject(context.Background(), "denied.json"); err == nil || errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("denied: %v", err)
	}
	signed, err := c.PresignGetObject(context.Background(), "配置/a +.json", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(signed)
	if err != nil || u.Path != "/dict-bucket/配置/a +.json" || u.Query().Get("x-oss-signature") == "" || u.Query().Get("x-oss-expires") != "300" {
		t.Fatalf("invalid presigned URL: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatal("presign made a network request")
	}
	if _, err = c.PresignGetObject(context.Background(), "a", 0); err == nil {
		t.Fatal("zero expiry accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.HeadObject(ctx, "a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	_ = c.Close()
	if _, err = c.PresignGetObject(context.Background(), "a", time.Minute); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestHeadNoSuchBucket(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-oss-err", base64.StdEncoding.EncodeToString([]byte(`<Error><Code>NoSuchBucket</Code><RequestId>review-request</RequestId></Error>`)))
		w.WriteHeader(404)
	}))
	defer server.Close()
	c := newTestClient(t, testConfig(server.URL))
	_, err := c.HeadObject(context.Background(), "config.json")
	serviceErr, ok := errors.AsType[*oss.ServiceError](err)
	if !ok || serviceErr.Code != "NoSuchBucket" {
		t.Fatal("fixture did not reproduce NoSuchBucket")
	}
	if errors.Is(err, ErrObjectNotFound) {
		t.Error("NoSuchBucket was incorrectly mapped to ErrObjectNotFound")
	}
}
