package ossx

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
)

func TestRejectedOperationsHaveNoDuration(t *testing.T) {
	buf := captureLogs(t)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("rejected operation reached server") }))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL), OptLogRequests(true))
	ctx := logit.WithStart(context.Background())
	for _, req := range []PutObjectRequest{
		{},
		{Body: strings.NewReader("data")},
		{Key: "key"},
	} {
		if _, err := c.PutObject(ctx, req); err == nil {
			t.Fatal("invalid upload accepted")
		}
	}
	if _, err := c.PutFile(ctx, PutFileRequest{
		FilePath: filepath.Join(t.TempDir(), "absent.txt"),
	}); err == nil || strings.Contains(err.Error(), "open file") {
		t.Fatalf("缺少 Key 应在打开文件前拒绝: %v", err)
	}
	if _, err := c.PutFile(ctx, PutFileRequest{
		Key:      "key",
		FilePath: filepath.Join(t.TempDir(), "absent.txt"),
	}); err == nil {
		t.Fatal("missing file accepted")
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
