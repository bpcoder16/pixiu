package ossservice_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/ossservice"
	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
)

func TestMD5FromURLHashesResponseContent(t *testing.T) {
	for _, tc := range []struct {
		name    string
		content string
		want    string
	}{
		{name: "empty", want: "d41d8cd98f00b204e9800998ecf8427e"},
		{name: "config JSON", content: "{}", want: "99914b932bd37a50b983c5e7c90ae93b"},
		{name: "raw text", content: "message digest", want: "f96b697d7cb7938d525a2f31aaf161d0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.RawQuery != "x-oss-signature=test-signature&x-oss-expires=1800" {
					t.Error("GET method or signed query changed")
				}
				// ETag 和内容不同，且分块返回，确保读取完整正文计算摘要。
				w.Header().Set("ETag", `"not-the-content-md5"`)
				w.WriteHeader(http.StatusOK)
				w.(http.Flusher).Flush()
				for _, value := range []byte(tc.content) {
					_, _ = w.Write([]byte{value})
					w.(http.Flusher).Flush()
				}
			}))
			defer server.Close()
			got, err := ossservice.MD5FromURL(context.Background(), server.URL+"/file?x-oss-signature=test-signature&x-oss-expires=1800")
			if err != nil || got != tc.want {
				t.Fatalf("MD5=%q err=%v, want %q", got, err, tc.want)
			}
		})
	}
}

func TestMD5FromURLPreservesRedirectBehavior(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/file?x-oss-signature=redirect-token", http.StatusFound)
			return
		}
		if r.URL.Path != "/file" || r.URL.Query().Get("x-oss-signature") != "redirect-token" {
			t.Error("redirect destination changed")
		}
		_, _ = io.WriteString(w, "abc")
	}))
	defer server.Close()
	got, err := ossservice.MD5FromURL(context.Background(), server.URL+"/redirect")
	if err != nil || got != "900150983cd24fb0d6963f7d28e17f72" || calls.Load() != 2 {
		t.Fatalf("redirect result=%q calls=%d err=%v", got, calls.Load(), err)
	}
}

func assertMD5ReadFailure(t *testing.T, ctx context.Context, fileURL string, want error) {
	t.Helper()
	got, err := ossservice.MD5FromURL(ctx, fileURL)
	if got != "" || !errors.Is(err, want) {
		t.Fatalf("MD5=%q err=%v, want empty digest and %v", got, err, want)
	}
	var urlErr *url.Error
	if errors.As(err, &urlErr) || strings.Contains(err.Error(), "secret-signature") || strings.Contains(err.Error(), "private-payload") {
		t.Fatal("read failure exposed a URL or response payload")
	}
}

func TestMD5FromURLRejectsHTTPFailuresWithoutRetry(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusForbidden, http.StatusServiceUnavailable, http.StatusNoContent, http.StatusPartialContent} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "private-payload")
			}))
			defer server.Close()
			want := ossservice.ErrFileRead
			if status == http.StatusNotFound {
				want = ossx.ErrObjectNotFound
			}
			assertMD5ReadFailure(t, context.Background(), server.URL+"/file?x-oss-signature=secret-signature", want)
			if calls.Load() != 1 {
				t.Fatal("failed HTTP response was retried")
			}
		})
	}
}

func TestMD5FromURLSanitizesRequestAndBodyErrors(t *testing.T) {
	assertMD5ReadFailure(t, context.Background(), "http://example.invalid/%zz?x-oss-signature=secret-signature", ossservice.ErrFileRead)
	assertMD5ReadFailure(t, nil, "http://example.invalid/file?x-oss-signature=secret-signature", ossservice.ErrFileRead)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = io.WriteString(w, "partial-content")
	}))
	defer server.Close()
	fileURL := server.URL + "/file?x-oss-signature=secret-signature"
	assertMD5ReadFailure(t, context.Background(), fileURL, ossservice.ErrFileRead)
	server.Close()
	assertMD5ReadFailure(t, context.Background(), fileURL, ossservice.ErrFileRead)
}

func TestMD5FromURLPreservesContextCancellation(t *testing.T) {
	for _, sendHeaders := range []bool{false, true} {
		t.Run(fmt.Sprintf("headers=%t", sendHeaders), func(t *testing.T) {
			started := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if sendHeaders {
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, "partial-content")
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				got, err := ossservice.MD5FromURL(ctx, server.URL+"/file?x-oss-signature=secret-signature")
				if got != "" {
					t.Error("canceled download returned a partial digest")
				}
				done <- err
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("download did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || !errors.Is(err, ossservice.ErrFileRead) || strings.Contains(err.Error(), "secret-signature") {
					t.Fatalf("cancellation or sanitized error contract lost: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("download did not stop after cancellation")
			}
		})
	}
}

func TestMD5FromURLPreservesDeadlineAndClosesErrorResponse(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}))
		defer server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		assertMD5ReadFailure(t, ctx, server.URL+"/file?x-oss-signature=secret-signature", context.DeadlineExceeded)
	})
	t.Run("close non-200 response", func(t *testing.T) {
		closed := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusForbidden)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
			close(closed)
		}))
		defer server.Close()
		defer server.CloseClientConnections()
		assertMD5ReadFailure(t, context.Background(), server.URL+"/file?x-oss-signature=secret-signature", ossservice.ErrFileRead)
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("error response body was not closed")
		}
	})
}
