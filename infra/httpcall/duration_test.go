package httpcall_test

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"weak"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

func TestRequestContextDoesNotRetainCompletedRequest(t *testing.T) {
	client := httpcall.New("inventory",
		httpcall.OptLogRequests(false),
		httpcall.OptResty(func(r *resty.Client) {
			r.SetTimeout(0)
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				return response(req, http.StatusOK), nil
			}))
		}),
	)
	ctx, request := func() (context.Context, weak.Pointer[resty.Request]) {
		req := client.Request(context.Background()).SetBody(make([]byte, 1<<20))
		if _, err := req.Post("https://example.test/items"); err != nil {
			t.Fatal(err)
		}
		return req.Context(), weak.Make(req)
	}()
	// 模拟业务继续保留请求 context；计时状态不能因此保留整个请求及其 body。
	defer runtime.KeepAlive(ctx)
	for range 5 {
		runtime.GC()
		if request.Value() == nil {
			return
		}
	}
	t.Fatal("请求已完成且业务仅保留 context，但 Request 仍无法回收")
}

func TestRequestDurationResetsAfterCompletion(t *testing.T) {
	for _, outcome := range []string{"success", "error", "panic", "retry", "muted"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				buf := captureLogs(t)
				failure := errors.New("request failed")
				attempts := 0
				client := httpcall.New("inventory",
					httpcall.OptLogRequests(outcome != "muted"),
					httpcall.OptResty(func(r *resty.Client) {
						// 传输器 panic 时标准库不会正常收尾超时 goroutine，隔离该机制。
						r.SetTimeout(0)
						if outcome == "retry" {
							r.SetRetryCount(1).
								SetRetryWaitTime(20 * time.Millisecond).
								SetRetryMaxWaitTime(20 * time.Millisecond)
						}
						r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
							time.Sleep(5 * time.Millisecond)
							attempts++
							if req.URL.Path == "/first" {
								if outcome == "panic" {
									panic(failure)
								}
								if outcome == "error" || outcome == "retry" && attempts == 1 {
									return nil, failure
								}
							}
							return response(req, http.StatusOK), nil
						}))
					}),
				)
				ctx := logit.WithStart(context.Background())
				req := client.Request(ctx)
				var recovered any
				var err error
				func() {
					defer func() { recovered = recover() }()
					_, err = req.Get("https://example.test/first")
				}()
				if outcome == "panic" {
					if recovered != failure {
						t.Fatalf("panic 未保留: %v", recovered)
					}
				} else if recovered != nil || (outcome == "error") != (err != nil) ||
					err != nil && !errors.Is(err, failure) {
					t.Fatalf("调用结果不正确: err=%v panic=%v", err, recovered)
				}

				// 调用之间的空闲时间不属于第二次 HTTP 执行。
				time.Sleep(time.Second)
				if _, err := req.Get("https://example.test/second"); err != nil {
					t.Fatal(err)
				}
				logit.InfoDuration(ctx, "request done")
				logs := records(t, buf)
				firstDuration := float64(5)
				if outcome == "retry" {
					firstDuration = 30
				}
				if outcome == "muted" {
					if len(logs) != 1 {
						t.Fatalf("关闭日志后仍输出请求结果: %v", logs)
					}
				} else if len(logs) != 3 ||
					logs[0][logit.DownstreamDurationMSKey] != firstDuration ||
					logs[1][logit.DownstreamDurationMSKey] != float64(5) {
					t.Fatalf("两次执行应独立计时: %v", logs)
				}
				summary := logs[len(logs)-1]
				if summary["HttpCall_inventory_1_duration_ms"] != firstDuration ||
					summary["HttpCall_inventory_2_duration_ms"] != float64(5) {
					t.Fatalf("耗时汇总不正确: %v", summary)
				}
			})
		})
	}
}

func TestRequestDurationIsolatedFromInheritedContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		buf := captureLogs(t)
		child := httpcall.New("child", httpcall.OptResty(func(r *resty.Client) {
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				time.Sleep(5 * time.Millisecond)
				return response(req, http.StatusOK), nil
			}))
		}))
		parent := httpcall.New("parent", httpcall.OptResty(func(r *resty.Client) {
			r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
				time.Sleep(30 * time.Millisecond)
				var wg sync.WaitGroup
				for range 2 {
					wg.Go(func() {
						if _, err := child.Request(req.Context()).Get("https://example.test/child"); err != nil {
							t.Error(err)
						}
					})
				}
				wg.Wait()
				// 未进入前置回调的无效请求也不能误用父请求起点。
				_, err := child.Request(req.Context()).
					SetFileReader("file", "x.txt", http.NoBody).
					Get("https://example.test/invalid")
				if err == nil {
					t.Error("multipart GET 应为无效请求")
				}
				return response(req, http.StatusOK), nil
			}))
		}))
		req := parent.Request(logit.WithStart(context.Background()))
		if _, err := req.Get("https://example.test/parent"); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)
		if _, err := child.Request(req.Context()).Get("https://example.test/later"); err != nil {
			t.Fatal(err)
		}
		logs := records(t, buf)
		if len(logs) != 5 {
			t.Fatalf("每次调用应恰有一条日志: %v", logs)
		}
		for i, want := range []float64{5, 5, 0, 35, 5} {
			if logs[i][logit.DownstreamDurationMSKey] != want {
				t.Errorf("第 %d 次调用耗时=%v, want %v", i, logs[i][logit.DownstreamDurationMSKey], want)
			}
		}
	})
}

func TestBeforeRequestDurationIncludesCallbacks(t *testing.T) {
	for _, outcome := range []string{"success", "error", "panic"} {
		t.Run(outcome, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				buf := captureLogs(t)
				failure := errors.New("prepare failed")
				client := httpcall.New("inventory", httpcall.OptResty(func(r *resty.Client) {
					r.OnBeforeRequest(func(*resty.Client, *resty.Request) error {
						time.Sleep(20 * time.Millisecond)
						switch outcome {
						case "error":
							return failure
						case "panic":
							panic(failure)
						}
						return nil
					})
					r.SetTransport(roundTripFunc(func(req *http.Request) (*http.Response, error) {
						time.Sleep(5 * time.Millisecond)
						return response(req, http.StatusOK), nil
					}))
				}))
				ctx := logit.WithStart(context.Background())
				var recovered any
				var err error
				func() {
					defer func() { recovered = recover() }()
					_, err = client.Request(ctx).Get("https://example.test/items")
				}()
				if outcome == "panic" {
					if recovered != failure {
						t.Fatalf("前置回调 panic 未保留: %v", recovered)
					}
				} else if recovered != nil || (outcome == "error") != (err != nil) ||
					err != nil && !errors.Is(err, failure) {
					t.Fatalf("前置回调结果不正确: err=%v panic=%v", err, recovered)
				}
				want := float64(20)
				if outcome == "success" {
					want = 25
				}
				logit.InfoDuration(ctx, "request done")
				logs := records(t, buf)
				if len(logs) != 2 || logs[0][logit.DownstreamDurationMSKey] != want ||
					logs[1]["HttpCall_inventory_1_duration_ms"] != want {
					t.Fatalf("结果日志和耗时汇总应包含前置回调: %v", logs)
				}
			})
		})
	}
}
