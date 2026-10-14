package ossx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/logit"
)

func TestConfigValidation(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Name = " " },
		func(c *Config) { c.Region = "" },
		func(c *Config) { c.Credential = nil },
		func(c *Config) { c.RequestTimeout = -1 },
		func(c *Config) { c.SlowThreshold = -1 },
		func(c *Config) { c.Endpoint = "https://user:secret@host" },
		func(c *Config) { c.Endpoint = "https://host/?secret=1" },
		func(c *Config) { c.Endpoint = "https://host/path" },
		func(c *Config) { c.Endpoint = "https://host/#secret" },
		func(c *Config) { c.Endpoint = "ftp://host" },
		func(c *Config) { c.Endpoint = "https://" },
	} {
		cfg := testConfig("")
		mutate(&cfg)
		c, err := New(cfg)
		if err == nil {
			_ = c.Close()
			t.Errorf("invalid configuration accepted")
		}
	}
	c := newTestClient(t, testConfig(""))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.PutObject(ctx, PutObjectRequest{
		Key:  "test.txt",
		Body: strings.NewReader("secret-content"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancel: %v", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := c.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	ctx = context.Background()
	_, err = c.PutObject(ctx, PutObjectRequest{
		Key:  "test.txt",
		Body: strings.NewReader("secret-content"),
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestRegionValidation(t *testing.T) {
	for _, endpoint := range []string{"", "http://127.0.0.1:1"} {
		for _, region := range []string{"CN-Hangzhou", "cn_hangzhou", "cn/hangzhou"} {
			t.Run(endpoint+"/"+region, func(t *testing.T) {
				cfg := testConfig(endpoint)
				cfg.Region = region
				c, err := New(cfg)
				if err == nil {
					_ = c.Close()
					t.Fatal("非法 Region 应在初始化时拒绝，自定义 Endpoint 不应跳过校验")
				}
				var paramErr oss.InvalidParamError
				if !errors.As(err, &paramErr) || paramErr.Field() != "Region" {
					t.Fatalf("应返回指向 Region 的 SDK 参数错误: %v", err)
				}
			})
		}
	}
}

func TestBucketAndCredentialValidation(t *testing.T) {
	for _, bucket := range []string{"", "INVALID BUCKET", "ab", "-bucket"} {
		t.Run("bucket="+bucket, func(t *testing.T) {
			cfg := testConfig("")
			cfg.Bucket = bucket
			c, err := New(cfg)
			if err == nil {
				_ = c.Close()
				t.Fatal("非法 Bucket 应在初始化时拒绝")
			}
			var paramErr oss.InvalidParamError
			if !errors.As(err, &paramErr) {
				t.Fatalf("未保留 SDK 参数错误: %v", err)
			}
		})
	}
	for _, tc := range []struct {
		name   string
		id     string
		secret string
		inject bool
	}{
		{name: "missing"},
		{name: "id-only", id: "test-id"},
		{name: "secret-only", secret: "test-secret"},
		{name: "blank-id", id: " ", secret: "test-secret"},
		{name: "blank-secret", id: "test-id", secret: " "},
		{name: "conflict-id", id: "test-id", inject: true},
		{name: "conflict-secret", secret: "test-secret", inject: true},
		{name: "conflict-pair", id: "test-id", secret: "test-secret", inject: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig("")
			cfg.AccessKeyID = tc.id
			cfg.AccessKeySecret = tc.secret
			if !tc.inject {
				cfg.Credential = nil
			}
			c, err := New(cfg)
			if err == nil {
				_ = c.Close()
				t.Fatal("非法或冲突的凭据应被拒绝")
			}
			if strings.Contains(err.Error(), "test-secret") {
				t.Fatalf("配置错误包含密钥: %v", err)
			}
		})
	}
}

func TestNamedLifecycle(t *testing.T) {
	old := namedClients
	namedClients = named.New[*Client]("ossx")
	t.Cleanup(func() {
		_ = CloseAll()
		namedClients = old
	})
	independent := newTestClient(t, testConfig(""))
	cfg := testConfig("")
	c, err := NewDefault(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if Default() != c || Named(cfg.Name) != c {
		t.Fatal("default alias mismatch")
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("duplicate accepted")
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	if _, err := NewNamed(cfg); err == nil {
		t.Fatal("registry reopened")
	}
	ctx := context.Background()
	_, err = c.PutObject(ctx, PutObjectRequest{
		Key:  "test.txt",
		Body: strings.NewReader("secret-content"),
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("registered client not closed: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	c = independent
	_, err = c.PutObject(ctx, PutObjectRequest{
		Key:  "test.txt",
		Body: strings.NewReader("secret-content"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("independent client closed: %v", err)
	}
}

func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
	old := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() {
		logit.SetDefault(old)
		_ = logit.Close(logger)
	})
	return buf
}

func TestLoggingAndDuration(t *testing.T) {
	for _, tt := range []struct {
		name    string
		status  int
		enabled bool
		slow    bool
		level   string
	}{
		{"disabled", 200, false, false, ""},
		{"success", 200, true, false, "INFO"},
		{"slow", 200, true, true, "WARN"},
		{"client_error", 403, true, false, "WARN"},
		{"server_error", 503, true, false, "ERROR"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := captureLogs(t)
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("x-oss-request-id", "rid")
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, `<Error><Code>Denied</Code><Message>secret-message</Message><RequestId>rid</RequestId></Error>`)
			}))
			defer s.Close()
			cfg := testConfig(s.URL)
			if tt.slow {
				cfg.SlowThreshold = time.Nanosecond
			}
			c := newTestClient(t, cfg, OptLogRequests(tt.enabled))
			ctx := logit.WithStart(context.Background())
			_, err := c.PutObject(ctx, PutObjectRequest{
				Key:  "test.txt",
				Body: strings.NewReader("secret-content"),
			})
			if (err != nil) != (tt.status >= 400) {
				t.Fatalf("unexpected error: %v", err)
			}
			if !tt.enabled && buf.Len() != 0 {
				t.Fatal("default logged")
			}
			if tt.enabled {
				var record map[string]any
				if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				details := record[logit.DownstreamDetailsKey].(map[string]any)
				if record["level"] != tt.level || record["msg"] != "AliyunOSS" || record[logit.DownstreamTypeKey] != "AliyunOSS" || details["request_id"] != "rid" || details["status"] != float64(tt.status) || details["bucket"] != cfg.Bucket {
					t.Fatalf("unexpected record: %v", record)
				}
				if strings.Contains(buf.String(), "secret") {
					t.Fatalf("sensitive log: %s", buf.String())
				}
			}
			buf.Reset()
			logit.InfoDuration(ctx, "done")
			if !strings.Contains(buf.String(), "AliyunOSS_"+cfg.Name+"_1_duration_ms") || strings.Contains(buf.String(), "AliyunOSS_"+cfg.Name+"_2_duration_ms") {
				t.Fatalf("duration missing/duplicated: %s", buf.String())
			}
		})
	}
}

func TestClientDefaults(t *testing.T) {
	cfg := testConfig("")
	cfg.SlowThreshold = 0
	c := newTestClient(t, cfg)
	if c.requestTimeout != 60*time.Second || c.slowThreshold != 500*time.Millisecond {
		t.Fatalf("默认超时或慢阈值发生变化: %v, %v", c.requestTimeout, c.slowThreshold)
	}
}
