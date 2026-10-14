package esx

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
)

func TestEmptyAndMalformedInventory(t *testing.T) {
	for _, body := range []string{
		`{"RequestId":"rid","Result":[]}`,
		`{"RequestId":"rid"}`,
		`{"RequestId":"rid","Result":[null]}`,
		`{"RequestId":"rid","Result":[{}]}`,
		`{"Result":[]}`,
		`broken-json`,
	} {
		t.Run(body, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			}))
			defer s.Close()
			c := newTestClient(t, testConfig(s.URL))
			list, err := c.ListSynonymsDicts(context.Background())
			if body == `{"RequestId":"rid","Result":[]}` {
				if err != nil || len(list.Dicts) != 0 {
					t.Fatalf("empty inventory: %+v %v", list, err)
				}
			} else if err == nil {
				t.Fatal("incomplete response accepted")
			}
			if strings.Contains(body, "[null]") || strings.Contains(body, "[{}]") || body == "broken-json" {
				_, err := c.UpdateSynonymsDicts(context.Background(), UpdateSynonymsDictsRequest{
					ClientToken: "token",
					Dicts:       []SynonymsDict{KeepSynonymsDict("ok.txt")},
				})
				if err == nil {
					t.Fatal("incomplete update accepted")
				}
			}
		})
	}
}

func TestRejectedOperationsHaveNoDuration(t *testing.T) {
	buf := captureLogs(t)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("rejected operation reached server") }))
	defer s.Close()
	c := newTestClient(t, testConfig(s.URL), OptLogRequests(true))
	ctx := logit.WithStart(context.Background())
	if _, err := c.ListSynonymsDicts(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := c.DescribeInstance(nil); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := c.UpdateSynonymsDicts(ctx, UpdateSynonymsDictsRequest{}); err == nil {
		t.Fatal("empty update accepted")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _ = c.DescribeInstance(cancelled)
	_ = c.Close()
	_, _ = c.DescribeInstance(ctx)
	if buf.Len() != 0 {
		t.Fatalf("rejected operation logged: %s", buf.String())
	}
	logit.InfoDuration(ctx, "done")
	if strings.Contains(buf.String(), "AliyunES_search_") {
		t.Fatalf("rejected operation recorded duration: %s", buf.String())
	}
}
