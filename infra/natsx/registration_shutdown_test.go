package natsx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
)

type registrationWriteFunc func([]byte) (int, error)

func (f registrationWriteFunc) Write(p []byte) (int, error) { return f(p) }

func TestWaitConsumersIncludesRegistrationLogging(t *testing.T) {
	for _, tc := range []struct {
		name    string
		subject string
		wantErr error
	}{
		{
			name:    "成功",
			subject: "work",
		},
		{
			name:    "订阅失败",
			subject: "bad subject",
			wantErr: nats.ErrBadSubject,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := New(Config{
				Name: "registration-log",
				URLs: []string{startServer(t)},
			})
			if err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			logger := logit.MustNew(logit.OptWriter(logit.NewWriter(registrationWriteFunc(func(p []byte) (int, error) {
				if strings.Contains(string(p), "coreSubscribe") {
					close(entered)
					<-release
				}
				return len(p), nil
			}))))
			previous := logit.Default()
			logit.SetDefault(logger)
			t.Cleanup(func() {
				unblock()
				_ = c.Close()
				logit.SetDefault(previous)
				_ = logit.Close(logger)
			})
			registered := make(chan error, 1)
			go func() {
				registered <- c.RegisterCoreConsumers(context.Background(), DefaultCoreConsumer("work", tc.subject, func(context.Context, *nats.Msg) error { return nil }))
			}()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("注册日志未开始")
			}
			stopped := make(chan struct{})
			go func() {
				c.StopConsumers()
				close(stopped)
			}()
			select {
			case <-stopped:
			case <-time.After(time.Second):
				t.Fatal("慢注册日志阻塞了停收")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			if err := c.WaitConsumers(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("注册日志未结束时 WaitConsumers = %v，期望超时", err)
			}
			c.CancelConsumers()
			ctx, cancelAfter := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancelAfter()
			if err := c.WaitConsumers(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("取消后仍须等待注册日志结束: %v", err)
			}
			unblock()
			select {
			case err := <-registered:
				if !errors.Is(err, tc.wantErr) {
					t.Errorf("注册错误 = %v，期望 %v", err, tc.wantErr)
				}
			case <-time.After(time.Second):
				t.Fatal("释放日志后注册未返回")
			}
			ctx, done := context.WithTimeout(context.Background(), time.Second)
			defer done()
			if err := c.WaitConsumers(ctx); err != nil {
				t.Fatal("日志结束后未完成排空", err)
			}
		})
	}
}
