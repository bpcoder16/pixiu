package cryptox_test

import (
	"bytes"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/cryptox"
)

func TestSignedRequestBindingAndReplay(t *testing.T) {
	signer, _ := cryptox.NewHMACSHA256(bytes.Repeat([]byte{3}, 32))
	now := time.Unix(1800000000, 0)
	timestamp := strconv.FormatInt(now.Unix(), 10)
	nonce, _ := cryptox.RandomToken(16)
	body := []byte(`{"job":"42"}`)
	req, _ := http.NewRequest(http.MethodPost, "https://internal.example/jobs?b=2&a=1", nil)
	message, err := requestMessage(req, body, timestamp, nonce)
	if err != nil {
		t.Fatal(err)
	}
	signature, _ := signer.Sign(message)
	for _, tc := range []struct {
		name      string
		method    string
		address   string
		body      []byte
		timestamp string
		nonce     string
	}{
		{"method", "GET", req.URL.String(), body, timestamp, nonce},
		{"host", "POST", "https://other.example/jobs?b=2&a=1", body, timestamp, nonce},
		{"path", "POST", "https://internal.example/admin?b=2&a=1", body, timestamp, nonce},
		{"query", "POST", "https://internal.example/jobs?b=3&a=1", body, timestamp, nonce},
		{"body", "POST", req.URL.String(), []byte(`{"job":"43"}`), timestamp, nonce},
		{"timestamp", "POST", req.URL.String(), body, "1800000001", nonce},
		{"nonce", "POST", req.URL.String(), body, timestamp, nonce + "x"},
		{"duplicate", "POST", "https://internal.example/jobs?a=1&a=2", body, timestamp, nonce},
		{"bad_query", "POST", "https://internal.example/jobs?a=%XX", body, timestamp, nonce},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, err := http.NewRequest(tc.method, tc.address, nil)
			if err != nil {
				t.Fatal(err)
			}
			got, err := requestMessage(changed, tc.body, tc.timestamp, tc.nonce)
			if err == nil && signer.Verify(got, signature) {
				t.Fatal("请求篡改通过验证")
			}
		})
	}
	ordered, _ := http.NewRequest(http.MethodPost, "https://internal.example/jobs?a=1&b=2", nil)
	canonical, _ := requestMessage(ordered, body, timestamp, nonce)
	if !bytes.Equal(canonical, message) {
		t.Fatal("query 顺序影响规范化")
	}

	// 单进程测试示范验签、窗口校验、原子登记的顺序；生产多实例应使用共享存储。
	var mu sync.Mutex
	used := make(map[string]int64)
	accept := func(timestamp, nonce string, signature []byte) bool {
		message, err := requestMessage(req, body, timestamp, nonce)
		if err != nil || !signer.Verify(message, signature) {
			return false
		}
		seconds, err := strconv.ParseInt(timestamp, 10, 64)
		if err != nil || seconds < now.Unix()-300 || seconds > now.Unix()+300 {
			return false
		}
		mu.Lock()
		defer mu.Unlock()
		if until, exists := used[nonce]; exists && until >= now.Unix() {
			return false
		}
		used[nonce] = seconds + 300
		return true
	}
	for _, seconds := range []int64{now.Unix() - 301, now.Unix() + 301} {
		stamp := strconv.FormatInt(seconds, 10)
		message, _ := requestMessage(req, body, stamp, nonce)
		sig, _ := signer.Sign(message)
		if accept(stamp, nonce, sig) {
			t.Fatal("时间窗口失效")
		}
	}
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			if accept(timestamp, nonce, signature) {
				accepted.Add(1)
			}
		})
	}
	wg.Wait()
	if accepted.Load() != 1 {
		t.Fatalf("原子消费次数=%d", accepted.Load())
	}
}
