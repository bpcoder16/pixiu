package esx

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/credentials-go/credentials"
)

type rotatingCredential struct {
	credentials.Credential
	version atomic.Int64
}

func (c *rotatingCredential) GetCredential() (*credentials.CredentialModel, error) {
	id, secret, token, typ := "es-account-one", "secret", "token-one", "sts"
	if c.version.Load() > 0 {
		id, token = "es-account-two", "token-two"
	}
	return &credentials.CredentialModel{
		AccessKeyId:     &id,
		AccessKeySecret: &secret,
		SecurityToken:   &token,
		Type:            &typ,
	}, nil
}

func testConfig(endpoint string) Config {
	return Config{
		Name:          "search",
		InstanceID:    "es-test",
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

const dictResponse = `{"RequestId":"es-request","Result":[{"name":"current.txt","fileSize":10,"sourceType":"ORIGIN","type":"SYNONYMS","downloadUrl":"https://example.test/?Signature=secret"}]}`

func TestOperationsAndDynamicCredentials(t *testing.T) {
	var calls atomic.Int64
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		id, token := "es-account-one", "token-one"
		if n > 1 {
			id, token = "es-account-two", "token-two"
		}
		if !strings.Contains(r.Header.Get("Authorization"), id) || r.Header.Get("x-acs-security-token") != token {
			t.Errorf("unexpected credentials: %v", r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		switch n {
		case 1:
			if r.Method != "GET" || r.URL.Path != "/openapi/instances/es-test/dicts" || r.URL.RawQuery != "analyzerType=SYNONYMS" {
				t.Errorf("unexpected list: %s %s", r.Method, r.URL)
			}
			_, _ = io.WriteString(w, dictResponse)
		case 2:
			if r.Method != "PUT" || r.URL.Path != "/openapi/instances/es-test/synonymsDict" || r.URL.Query().Get("clientToken") != "release-1" {
				t.Errorf("unexpected update: %s %s", r.Method, r.URL)
			}
			var got any
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Error(err)
			}
			var want any
			_ = json.Unmarshal([]byte(`[{"name":"keep.txt","sourceType":"ORIGIN","type":"SYNONYMS"},{"name":"current.txt","sourceType":"OSS","type":"SYNONYMS","ossObject":{"bucketName":"dict-bucket","key":"release/词库.txt"}}]`), &want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("unexpected full inventory body: %#v", got)
			}
			_, _ = io.WriteString(w, dictResponse)
		case 3:
			if r.Method != "GET" || r.URL.Path != "/openapi/instances/es-test" || r.URL.RawQuery != "" {
				t.Errorf("unexpected describe: %s %s", r.Method, r.URL)
			}
			_, _ = io.WriteString(w, `{"RequestId":"describe-id","Result":{"instanceId":"es-test","status":"activating","esVersion":"8.17"}}`)
		default:
			t.Error("unexpected extra request")
		}
	}))
	defer s.Close()
	cfg := testConfig(s.URL)
	c := newTestClient(t, cfg)
	if calls.Load() != 0 {
		t.Fatal("New performed request")
	}
	list, err := c.ListSynonymsDicts(context.Background())
	if err != nil || list.RequestID != "es-request" || len(list.Dicts) != 1 || list.Dicts[0].Name != "current.txt" || list.Dicts[0].FileSize != 10 || !strings.Contains(list.Dicts[0].DownloadURL, "Signature") {
		t.Fatalf("list=%+v err=%v", list, err)
	}
	// 修改调用方配置不得改变已构造客户端的目标。
	cfg.InstanceID = "es-other"
	cfg.Credential.(*rotatingCredential).version.Store(1)
	updated, err := c.UpdateSynonymsDicts(context.Background(), UpdateSynonymsDictsRequest{
		ClientToken: "release-1",
		Dicts:       []SynonymsDict{KeepSynonymsDict("keep.txt"), SynonymsDictFromOSS("current.txt", "dict-bucket", "release/词库.txt")},
	})
	if err != nil || updated.RequestID != "es-request" || len(updated.Dicts) != 1 {
		t.Fatalf("update=%+v err=%v", updated, err)
	}
	desc, err := c.DescribeInstance(context.Background())
	if err != nil || desc.RequestID != "describe-id" || desc.InstanceID != "es-test" || desc.Status != "activating" || desc.Version != "8.17" {
		t.Fatalf("describe=%+v err=%v", desc, err)
	}
}

func TestUpdateValidationBeforeRequest(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("invalid request reached SDK") }))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL))
	for _, req := range []UpdateSynonymsDictsRequest{
		{},
		{
			ClientToken: "token",
		},
		{
			Dicts: []SynonymsDict{KeepSynonymsDict("ok.txt")},
		},
		{
			ClientToken: strings.Repeat("a", 65),
			Dicts:       []SynonymsDict{KeepSynonymsDict("ok.txt")},
		},
		{
			ClientToken: "中文",
			Dicts:       []SynonymsDict{KeepSynonymsDict("ok.txt")},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{{}},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{KeepSynonymsDict("same.txt"), KeepSynonymsDict("same.txt")},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{KeepSynonymsDict("../bad.txt")},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{KeepSynonymsDict(strings.Repeat("a", 27) + ".txt")},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{SynonymsDictFromOSS("ok.txt", "", "key")},
		},
		{
			ClientToken: "token",
			Dicts:       []SynonymsDict{SynonymsDictFromOSS("ok.txt", "bucket", "")},
		},
	} {
		if _, err := c.UpdateSynonymsDicts(context.Background(), req); err == nil {
			t.Errorf("accepted invalid request: %+v", req)
		}
	}
}

func TestErrorsNoRetryAndIncompleteResponses(t *testing.T) {
	for _, status := range []int{403, 503, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int64
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"RequestId":"failure-id","Code":"AccessDenied","Message":"secret-payload"}`)
			}))
			defer s.Close()
			c := newTestClient(t, testConfig(s.URL))
			_, err := c.DescribeInstance(context.Background())
			if err == nil || calls.Load() != 1 {
				t.Fatalf("err=%v calls=%d", err, calls.Load())
			}
			if status != 200 {
				var sdkErr *tea.SDKError
				if !errors.As(err, &sdkErr) || tea.IntValue(sdkErr.StatusCode) != status {
					t.Fatalf("lost SDK error: %v", err)
				}
			}
		})
	}
}

func TestCancellationAndTimeout(t *testing.T) {
	for _, callerTimeout := range []bool{false, true} {
		t.Run(map[bool]string{false: "config", true: "caller"}[callerTimeout], func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
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
			_, err := c.DescribeInstance(ctx)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("lost deadline: %v", err)
			}
		})
	}
}

func TestResponseBodyCancellation(t *testing.T) {
	for _, operation := range []string{"list", "update", "describe"} {
		for _, status := range []int{http.StatusOK, http.StatusForbidden} {
			for _, callerCancel := range []bool{false, true} {
				name := operation + "/" + http.StatusText(status) + "/timeout"
				if callerCancel {
					name = operation + "/" + http.StatusText(status) + "/cancel"
				}
				t.Run(name, func(t *testing.T) {
					started := make(chan struct{})
					release := make(chan struct{})
					s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						_, _ = io.Copy(io.Discard, r.Body)
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(status)
						_, _ = io.WriteString(w, `{"RequestId":`)
						w.(http.Flusher).Flush()
						close(started)
						select {
						case <-r.Context().Done():
						case <-release:
						}
					}))
					defer s.Close()
					defer close(release)
					cfg := testConfig(s.URL)
					cfg.RequestTimeout = 100 * time.Millisecond
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					want := context.DeadlineExceeded
					if callerCancel {
						cfg.RequestTimeout = time.Second
						want = context.Canceled
						go func() {
							select {
							case <-started:
								cancel()
							case <-ctx.Done():
							}
						}()
					}
					c := newTestClient(t, cfg)
					var err error
					switch operation {
					case "list":
						_, err = c.ListSynonymsDicts(ctx)
					case "update":
						_, err = c.UpdateSynonymsDicts(ctx, UpdateSynonymsDictsRequest{
							ClientToken: "release-token",
							Dicts:       []SynonymsDict{KeepSynonymsDict("ok.txt")},
						})
					case "describe":
						_, err = c.DescribeInstance(ctx)
					}
					select {
					case <-started:
					default:
						t.Fatal("未进入响应体读取阶段")
					}
					if !errors.Is(err, want) {
						t.Fatalf("读取响应体时丢失取消原因: %v", err)
					}
				})
			}
		}
	}
}
