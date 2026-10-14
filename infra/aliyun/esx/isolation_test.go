package esx

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestIndependentAccountsAndConcurrentCalls(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if r.Method == "PUT" {
			if !strings.Contains(r.Header.Get("Authorization"), "oss-account") || strings.Contains(r.Header.Get("Authorization"), "es-account") {
				t.Error("OSS used ES credentials")
			}
			w.Header().Set("x-oss-request-id", "oss-id")
			return
		}
		if !strings.Contains(r.Header.Get("Authorization"), "es-account") || strings.Contains(r.Header.Get("Authorization"), "oss-account") {
			t.Error("ES used OSS credentials")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"RequestId":"es-id","Result":{"instanceId":"es-test","status":"active","esVersion":"7.10"}}`)
	}))
	defer s.Close()
	files, err := ossx.New(ossx.Config{
		Name:            "shared-name",
		Bucket:          "dict-bucket",
		Region:          "cn-shanghai",
		Endpoint:        s.URL,
		AccessKeyID:     "oss-account",
		AccessKeySecret: "oss-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = files.Close() })
	cfg := testConfig(s.URL)
	cfg.Name = "shared-name"
	cfg.Credential = nil
	cfg.AccessKeyID = "es-account"
	cfg.AccessKeySecret = "es-secret"
	c := newTestClient(t, cfg)
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := c.DescribeInstance(context.Background()); err != nil {
				t.Error(err)
			}
			if _, err := files.PutObject(context.Background(), ossx.PutObjectRequest{
				Key:  "synonyms.txt",
				Body: strings.NewReader("a,b"),
			}); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	var stack lifecycle.Stack
	if err := stack.Register(files.Close); err != nil {
		t.Fatal(err)
	}
	if err := stack.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeInstance(context.Background()); err != nil {
		t.Fatalf("closing OSS affected ES: %v", err)
	}
}
