package stsx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/bpcoder16/pixiu/logit"
)

const successResponse = `{"RequestId":"sts-request","Credentials":{"AccessKeyId":"temporary-id","AccessKeySecret":"temporary-secret","SecurityToken":"temporary-token","Expiration":"2030-01-01T00:00:00Z"}}`

func testConfig(endpoint string) Config {
	return Config{Name: "upload", Region: "cn-shanghai", Endpoint: endpoint, AccessKeyID: "test-id", AccessKeySecret: "test-secret"}
}
func testRequest() AssumeRoleRequest {
	return AssumeRoleRequest{RoleARN: "acs:ram::123456789012:role/upload", Duration: 40 * time.Minute, Policy: `{"Version":"1","Statement":[{"Effect":"Allow","Action":["example:Read"],"Resource":["example-resource"]}]}`}
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
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	logger := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
	old := logit.Default()
	logit.SetDefault(logger)
	t.Cleanup(func() { logit.SetDefault(old); _ = logit.Close(logger) })
	return buf
}

func TestAssumeRoleAndValidation(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = r.ParseForm()
		req := testRequest()
		session := r.Form.Get("RoleSessionName")
		if r.Form.Get("RoleArn") != req.RoleARN || r.Form.Get("DurationSeconds") != "2400" || r.Form.Get("Policy") != req.Policy || (session != "sts-session" && session != "config-upload") {
			t.Error("AssumeRole parameters not preserved")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, successResponse)
	}))
	defer server.Close()
	c := newTestClient(t, testConfig(server.URL))
	for _, session := range []string{"", "config-upload"} {
		req := testRequest()
		req.SessionName = session
		token, err := c.AssumeRole(context.Background(), req)
		if err != nil || token.AccessKeyID != "temporary-id" || token.AccessKeySecret != "temporary-secret" || token.SecurityToken != "temporary-token" || token.Expiration.IsZero() {
			t.Fatalf("invalid credentials: %v", err)
		}
	}
	for _, mutate := range []func(*AssumeRoleRequest){
		func(r *AssumeRoleRequest) { r.RoleARN = "" },
		func(r *AssumeRoleRequest) { r.RoleARN = "invalid" },
		func(r *AssumeRoleRequest) { r.RoleARN += " " },
		func(r *AssumeRoleRequest) { r.SessionName = "a" },
		func(r *AssumeRoleRequest) { r.SessionName = "invalid+name" },
		func(r *AssumeRoleRequest) { r.Duration = time.Second },
		func(r *AssumeRoleRequest) { r.Duration = 13 * time.Hour },
		func(r *AssumeRoleRequest) { r.Duration += time.Nanosecond },
		func(r *AssumeRoleRequest) { r.Policy = "invalid json" },
		func(r *AssumeRoleRequest) { r.Policy = strings.Repeat(" ", 2049) },
	} {
		req := testRequest()
		mutate(&req)
		if _, err := c.AssumeRole(context.Background(), req); err == nil {
			t.Error("invalid request accepted")
		}
	}
	if calls.Load() != 2 {
		t.Fatal("invalid input made network calls")
	}
}

func TestAssumeRoleFailureLogs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		enabled bool
		level   string
	}{
		{"disabled", 403, false, ""}, {"denied", 403, true, "WARN"}, {"unavailable", 503, true, "ERROR"}, {"success", 200, true, "INFO"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := captureLogs(t)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				if tc.status == 200 {
					_, _ = io.WriteString(w, successResponse)
					return
				}
				_, _ = io.WriteString(w, `{"Code":"AccessDenied","Message":"sensitive-temporary-secret","RequestId":"review-sts-id"}`)
			}))
			defer server.Close()
			c := newTestClient(t, testConfig(server.URL), OptLogRequests(tc.enabled))
			ctx := logit.WithStart(context.Background())
			_, err := c.AssumeRole(ctx, testRequest())
			if tc.status >= 400 {
				sdkErr, ok := errors.AsType[*tea.SDKError](err)
				if !ok || tea.StringValue(sdkErr.Code) != "AccessDenied" {
					t.Fatalf("lost SDK error: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 {
				t.Fatal("unexpected retry")
			}
			if !tc.enabled && logs.Len() != 0 {
				t.Fatal("logging disabled")
			}
			if tc.enabled {
				var record map[string]any
				if err := json.Unmarshal(logs.Bytes(), &record); err != nil {
					t.Fatal(err)
				}
				details := record[logit.DownstreamDetailsKey].(map[string]any)
				code, rid := "AccessDenied", "review-sts-id"
				if tc.status == 200 {
					code, rid = "", "sts-request"
				}
				if details["error_code"] != code || details["request_id"] != rid || details["status"] != float64(tc.status) || record["level"] != tc.level || record["msg"] != "AliyunSTS" {
					t.Errorf("incorrect metadata: %v", record)
				}
				for _, secret := range []string{"temporary-secret", "temporary-token", "test-secret", "example-resource"} {
					if strings.Contains(logs.String(), secret) {
						t.Fatal("sensitive log")
					}
				}
			}
			logs.Reset()
			logit.InfoDuration(ctx, "done")
			if !strings.Contains(logs.String(), "AliyunSTS_upload_1_duration_ms") {
				t.Fatal("missing downstream duration")
			}
		})
	}
}

func TestAssumeRoleTimeoutAndClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body); <-r.Context().Done() }))
	defer server.Close()
	cfg := testConfig(server.URL)
	cfg.RequestTimeout = 40 * time.Millisecond
	c := newTestClient(t, cfg)
	if _, err := c.AssumeRole(context.Background(), testRequest()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.AssumeRole(ctx, testRequest()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if _, err := c.AssumeRole(nil, testRequest()); err == nil {
		t.Fatal("nil context accepted")
	}
	_ = c.Close()
	_ = c.Close()
	if _, err := c.AssumeRole(context.Background(), testRequest()); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestAssumeRoleInvalidResponse(t *testing.T) {
	for _, body := range []string{`{}`, `{"Credentials":{}}`, strings.Replace(successResponse, "2030-01-01T00:00:00Z", "2000-01-01T00:00:00Z", 1), strings.Replace(successResponse, "temporary-token", "", 1)} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer server.Close()
			c := newTestClient(t, testConfig(server.URL))
			if _, err := c.AssumeRole(context.Background(), testRequest()); err == nil {
				t.Fatal("invalid credentials accepted")
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Name = " " }, func(c *Config) { c.Region = "" },
		func(c *Config) { c.Region = "cn%invalid" },
		func(c *Config) { c.Region = "cn/invalid" }, func(c *Config) { c.RequestTimeout = -1 }, func(c *Config) { c.SlowThreshold = -1 },
		func(c *Config) { c.AccessKeyID = "" }, func(c *Config) { c.AccessKeySecret = "" },
		func(c *Config) { c.Endpoint = "https://user:secret@host" }, func(c *Config) { c.Endpoint = "https://host/path" }, func(c *Config) { c.Endpoint = "https://host/?secret=1" }, func(c *Config) { c.Endpoint = "https://host/#secret" }, func(c *Config) { c.Endpoint = "ftp://host" },
	} {
		cfg := testConfig("")
		mutate(&cfg)
		c, err := New(cfg)
		if err == nil {
			_ = c.Close()
			t.Error("invalid configuration accepted")
		}
	}
	c := newTestClient(t, testConfig(""))
	if c.requestTimeout != 60*time.Second || c.slowThreshold != 500*time.Millisecond {
		t.Fatal("incorrect defaults")
	}
}
