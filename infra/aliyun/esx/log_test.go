package esx

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

func TestMalformedResponseLogging(t *testing.T) {
	for _, operation := range []string{"ListSynonymsDicts", "UpdateSynonymsDicts", "DescribeInstance"} {
		for _, tc := range []struct {
			name        string
			status      int
			contentType string
			body        string
			level       string
		}{
			{
				name:        "html",
				status:      http.StatusForbidden,
				contentType: "text/html",
				body:        "<html>secret-response</html>",
				level:       "WARN",
			},
			{
				name:        "broken-error",
				status:      http.StatusBadGateway,
				contentType: "application/json",
				body:        `{"secret-response":`,
				level:       "ERROR",
			},
			{
				name:        "broken-success",
				status:      http.StatusOK,
				contentType: "application/json",
				body:        `{"secret-response":`,
				level:       "ERROR",
			},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				buf := captureLogs(t)
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", tc.contentType)
					w.WriteHeader(tc.status)
					_, _ = io.WriteString(w, tc.body)
				}))
				defer server.Close()
				client := newTestClient(t, testConfig(server.URL), OptLogRequests(true))
				var err error
				switch operation {
				case "ListSynonymsDicts":
					_, err = client.ListSynonymsDicts(context.Background())
				case "UpdateSynonymsDicts":
					_, err = client.UpdateSynonymsDicts(context.Background(), UpdateSynonymsDictsRequest{
						ClientToken: "release-token",
						Dicts:       []SynonymsDict{KeepSynonymsDict("ok.txt")},
					})
				case "DescribeInstance":
					_, err = client.DescribeInstance(context.Background())
				}
				if err == nil {
					t.Fatal("异常正文应返回错误")
				}
				var record map[string]any
				if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
					t.Fatalf("每次操作应仅输出一条结果日志: %v", err)
				}
				details := record[logit.DownstreamDetailsKey].(map[string]any)
				if details["status"] != float64(tc.status) || record["level"] != tc.level || details["operation"] != operation || details["error_code"] != "client_error" {
					t.Errorf("解析失败时应保留 HTTP 状态并正确分级: %v", record)
				}
				if strings.Contains(buf.String(), "secret-response") {
					t.Error("异常正文不得进入日志")
				}
			})
		}
	}
}

func TestConcurrentResponseStatusLogging(t *testing.T) {
	buf := captureLogs(t)
	started := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := http.StatusForbidden
		if strings.HasSuffix(r.URL.Path, "/dicts") {
			status = http.StatusBadGateway
		}
		w.WriteHeader(status)
		w.(http.Flusher).Flush()
		started <- struct{}{}
		// 两次调用均收到响应头后才结束正文，确保响应解析阶段重叠。
		select {
		case <-release:
		case <-r.Context().Done():
		}
		_, _ = io.WriteString(w, `{"broken":`)
	}))
	defer server.Close()
	defer close(release)
	client := newTestClient(t, testConfig(server.URL), OptLogRequests(true))
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	wg.Go(func() {
		if _, err := client.ListSynonymsDicts(ctx); err == nil {
			t.Error("异常正文应返回错误")
		}
	})
	wg.Go(func() {
		if _, err := client.DescribeInstance(ctx); err == nil {
			t.Error("异常正文应返回错误")
		}
	})
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("并发请求未全部收到响应头")
		}
	}
	// 分别放行两个响应，保留 defer 关闭通道以处理测试提前退出。
	release <- struct{}{}
	release <- struct{}{}
	wg.Wait()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("应输出两条独立结果日志: %s", buf.String())
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		wantStatus, wantLevel := http.StatusForbidden, "WARN"
		if details["operation"] == "ListSynonymsDicts" {
			wantStatus, wantLevel = http.StatusBadGateway, "ERROR"
		}
		if details["status"] != float64(wantStatus) || record["level"] != wantLevel {
			t.Errorf("并发请求的状态不得互相覆盖: %v", record)
		}
	}
}
