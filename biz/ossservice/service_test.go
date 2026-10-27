package ossservice_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/bpcoder16/pixiu/biz/ossservice"
	"github.com/bpcoder16/pixiu/infra/aliyun/ossx"
	"github.com/bpcoder16/pixiu/infra/aliyun/stsx"
)

func newClients(t *testing.T, handler http.HandlerFunc) (*ossx.Client, *stsx.Client) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	ossClient, err := ossx.New(ossx.Config{
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
	t.Cleanup(func() { _ = ossClient.Close() })
	stsClient, err := stsx.New(stsx.Config{
		Name:            "upload-sts",
		Region:          "cn-shanghai",
		Endpoint:        server.URL,
		AccessKeyID:     "sts-id",
		AccessKeySecret: "sts-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stsClient.Close() })
	return ossClient, stsClient
}

func uploadOptions() ossservice.UploadCredentialsOptions {
	return ossservice.UploadCredentialsOptions{
		RoleARN:  "acs:ram::123456789012:role/upload",
		Duration: 40 * time.Minute,
	}
}

func TestCreateUploadCredentialsScopesObjectsAndPreservesOptions(t *testing.T) {
	requests := make(chan url.Values, 2)
	expiration := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	ossClient, stsClient := newClients(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		requests <- r.Form
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"Credentials":{"AccessKeyId":"temporary-id","AccessKeySecret":"temporary-secret","SecurityToken":"temporary-token","Expiration":%q}}`, expiration.Format(time.RFC3339))
	})
	service, err := ossservice.New(ossClient, ossservice.WithSTSClient(stsClient))
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"config/a.json", "config/文件 & #%.json"}
	wantPolicy := map[string]any{
		"Version": "1",
		"Statement": []any{map[string]any{
			"Effect": "Allow",
			"Action": []any{"oss:PutObject"},
			"Resource": []any{
				"acs:oss:*:*:upload-bucket/" + keys[0],
				"acs:oss:*:*:upload-bucket/" + keys[1],
			},
		}},
	}
	for _, session := range []string{"", "config-upload"} {
		options := uploadOptions()
		options.SessionName = session
		got, err := service.CreateUploadCredentials(context.Background(), keys, options)
		if err != nil {
			t.Fatal(err)
		}
		if got.Info != ossClient.Info() || got.Credentials.AccessKeyID != "temporary-id" ||
			got.Credentials.AccessKeySecret != "temporary-secret" || got.Credentials.SecurityToken != "temporary-token" ||
			!got.Credentials.Expiration.Equal(expiration) {
			t.Fatal("bucket information or credentials changed")
		}
		form := <-requests
		if session == "" {
			session = "sts-session"
		}
		if form.Get("RoleArn") != options.RoleARN || form.Get("RoleSessionName") != session || form.Get("DurationSeconds") != "2400" {
			t.Fatal("STS role, session or duration changed")
		}
		var policy map[string]any
		if err := json.Unmarshal([]byte(form.Get("Policy")), &policy); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(policy, wantPolicy) {
			t.Fatalf("unexpected upload authorization: %v", policy)
		}
	}
}

func TestCreateUploadCredentialsRejectsUnsafePolicyBeforeSTS(t *testing.T) {
	var calls atomic.Int32
	ossClient, stsClient := newClients(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	service, err := ossservice.New(ossClient, ossservice.WithSTSClient(stsClient))
	if err != nil {
		t.Fatal(err)
	}
	var oversized []string
	for i := range 30 {
		oversized = append(oversized, fmt.Sprintf("config/%s-%d.json", strings.Repeat("a", 70), i))
	}
	for _, tc := range []struct {
		name string
		keys []string
	}{
		{name: "no keys"},
		{name: "empty key", keys: []string{""}},
		{name: "star wildcard", keys: []string{"config/*"}},
		{name: "question wildcard", keys: []string{"config/?.json"}},
		{name: "mixed valid and invalid", keys: []string{"config/a.json", ""}},
		{name: "policy exceeds 2048 bytes", keys: oversized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := service.CreateUploadCredentials(context.Background(), tc.keys, uploadOptions())
			if err == nil || got != (ossservice.UploadCredentialsResult{}) {
				t.Fatal("unsafe policy must fail without a result")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("invalid policy reached STS")
	}
}

func TestCreateUploadCredentialsPreservesErrorsWithoutRetry(t *testing.T) {
	var calls atomic.Int32
	ossClient, stsClient := newClients(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, `{"Code":"ServiceUnavailable","Message":"unavailable","RequestId":"test-request"}`)
	})
	service, err := ossservice.New(ossClient, ossservice.WithSTSClient(stsClient))
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{"config/a.json"}
	got, err := service.CreateUploadCredentials(context.Background(), keys, uploadOptions())
	var sdkErr *tea.SDKError
	if !errors.As(err, &sdkErr) || tea.StringValue(sdkErr.Code) != "ServiceUnavailable" || got != (ossservice.UploadCredentialsResult{}) {
		t.Fatal("STS error chain or empty result contract lost")
	}
	if calls.Load() != 1 {
		t.Fatal("STS request retried")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	expired, cancelDeadline := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	for _, tc := range []struct {
		ctx  context.Context
		want error
	}{
		{ctx: canceled, want: context.Canceled},
		{ctx: expired, want: context.DeadlineExceeded},
	} {
		got, err := service.CreateUploadCredentials(tc.ctx, keys, uploadOptions())
		if !errors.Is(err, tc.want) || got != (ossservice.UploadCredentialsResult{}) {
			t.Fatalf("context error not preserved: %v", err)
		}
	}
	if _, err := service.CreateUploadCredentials(nil, keys, uploadOptions()); err == nil {
		t.Fatal("nil context accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("invalid context reached STS")
	}
}

func TestNewRequiresOSS(t *testing.T) {
	if service, err := ossservice.New(nil); service != nil || err == nil {
		t.Fatal("missing OSS client accepted")
	}
}

func TestCreateUploadCredentialsRequiresSTS(t *testing.T) {
	ossClient, err := ossx.New(ossx.Config{
		Name:            "uploads",
		Region:          "cn-shanghai",
		Bucket:          "upload-bucket",
		AccessKeyID:     "oss-id",
		AccessKeySecret: "oss-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ossClient.Close() })
	for _, tc := range []struct {
		name    string
		options []ossservice.Option
	}{
		{name: "STS omitted"},
		{name: "nil option", options: []ossservice.Option{nil}},
		{name: "nil STS client", options: []ossservice.Option{ossservice.WithSTSClient(nil)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service, err := ossservice.New(ossClient, tc.options...)
			if err != nil || service == nil {
				t.Fatalf("constructing service must not require STS: %v", err)
			}
			result, err := service.CreateUploadCredentials(context.Background(), []string{"config/a.json"}, uploadOptions())
			if !errors.Is(err, ossservice.ErrSTSNotConfigured) || result != (ossservice.UploadCredentialsResult{}) {
				t.Fatalf("issuing credentials without STS must return ErrSTSNotConfigured and no result: %v", err)
			}
		})
	}
}
