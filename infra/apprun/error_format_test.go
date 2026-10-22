package apprun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestErrorFormattingDoesNotBlockShutdown(t *testing.T) {
	for _, operation := range []string{"stop", "wait", "run"} {
		for _, method := range []string{"Error", "Format"} {
			t.Run(operation+"/"+method, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					manager, err := New(time.Second)
					if err != nil {
						t.Fatal(err)
					}
					formatCtx, releaseFormat := context.WithCancel(context.Background())
					defer releaseFormat()
					var failure error = &blockingCallbackError{release: formatCtx.Done()}
					if method == "Format" {
						failure = &blockingCallbackFormatError{release: formatCtx.Done()}
					}
					stopped := make(chan struct{})
					register(t, manager, Service{
						Name: "failing",
						Run: func() error {
							if operation == "run" {
								return failure
							}
							<-stopped
							return nil
						},
						Stop: func() error {
							close(stopped)
							if operation == "stop" {
								return failure
							}
							return nil
						},
						Wait: func(context.Context) error {
							if operation == "wait" {
								return failure
							}
							return nil
						},
						Cancel: func() {},
					})
					start := time.Now()
					canceled := make(chan time.Duration, 1)
					workerStopped := make(chan struct{})
					register(t, manager, Service{
						Name: "working",
						Run: func() error {
							<-workerStopped
							return nil
						},
						Stop: func() error {
							close(workerStopped)
							return nil
						},
						Wait: func(ctx context.Context) error {
							<-ctx.Done()
							return ctx.Err()
						},
						Cancel: func() { canceled <- time.Since(start) },
					})
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					if operation != "run" {
						cancel()
					}
					done := run(manager, ctx)
					// 保持错误文本求值阻塞，验证截止时间仍能驱动取消及返回。
					time.Sleep(time.Second)
					synctest.Wait()
					select {
					case err = <-done:
					default:
						t.Fatal("关闭期限耗尽后，管理器仍在等待错误格式化")
					}
					select {
					case elapsed := <-canceled:
						if elapsed != time.Second {
							t.Fatalf("其他服务的取消被拖延: %s", elapsed)
						}
					default:
						t.Fatal("未及时取消其他未完成服务")
					}
					if !errors.Is(err, failure) || !errors.Is(err, ErrShutdownTimeout) || !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal("原始错误或关闭超时错误丢失")
					}
					if method == "Format" {
						var original *blockingCallbackFormatError
						if !errors.As(err, &original) || original != failure {
							t.Fatal("errors.As 无法提取原始格式化错误")
						}
					} else {
						var original *blockingCallbackError
						if !errors.As(err, &original) || original != failure {
							t.Fatal("errors.As 无法提取原始错误")
						}
					}
					releaseFormat()
					synctest.Wait()
					want := fmt.Sprintf("apprun: service %q %s: 回调失败", "failing", operation)
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("错误文本缺少服务名或阶段: %v", err)
					}
				})
			})
		}
	}
}

type blockingCallbackError struct {
	release <-chan struct{}
}

func (e *blockingCallbackError) Error() string {
	<-e.release
	return "回调失败"
}

type blockingCallbackFormatError struct {
	release <-chan struct{}
}

func (*blockingCallbackFormatError) Error() string { return "回调失败" }

func (e *blockingCallbackFormatError) Format(state fmt.State, _ rune) {
	<-e.release
	_, _ = fmt.Fprint(state, "回调失败")
}
