package natsx

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestAsyncErrorHandlerCoreFailures(t *testing.T) {
	for _, mode := range []string{"permission", "slow-consumer"} {
		t.Run(mode, func(t *testing.T) {
			buf := captureLogs(t)
			opts := &server.Options{
				Host:   "127.0.0.1",
				Port:   -1,
				NoLog:  true,
				NoSigs: true,
			}
			if mode == "permission" {
				opts.Users = []*server.User{{
					Username: "test",
					Password: "test",
					Permissions: &server.Permissions{
						Subscribe: &server.SubjectPermission{Deny: []string{"work"}},
					},
				}}
			}
			s := startServerWithOptions(t, opts)
			type event struct {
				conn *nats.Conn
				sub  *nats.Subscription
				err  error
			}
			events := make(chan event, 8)
			cfg := Config{
				Name: "async-errors",
				URLs: []string{s.ClientURL()},
				AsyncErrorHandler: func(nc *nats.Conn, sub *nats.Subscription, err error) {
					events <- event{conn: nc, sub: sub, err: err}
				},
			}
			if mode == "permission" {
				cfg.Username = "test"
				cfg.Password = "test"
			}
			c, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			spec := DefaultCoreConsumer("worker", "work", func(ctx context.Context, _ *nats.Msg) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil
			})
			spec.PendingMessages = 1
			if err := c.RegisterCoreConsumers(ctx, spec); err != nil {
				t.Fatal(err)
			}
			want := nats.ErrPermissionViolation
			if mode == "slow-consumer" {
				want = nats.ErrSlowConsumer
				if err := c.Publish(ctx, "work", nil); err != nil {
					t.Fatal(err)
				}
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("消费回调未开始")
				}
				for range 3 {
					if err := c.Publish(ctx, "work", nil); err != nil {
						t.Fatal(err)
					}
				}
			}
			select {
			case got := <-events:
				if got.conn != c.Conn() || !errors.Is(got.err, want) {
					t.Fatalf("异步错误通知不正确: %+v", got)
				}
				// SDK 的权限错误可能不带订阅，主题包含在原始错误中；慢消费者会带订阅。
				if mode == "slow-consumer" && (got.sub == nil || got.sub.Subject != "work") {
					t.Fatal("慢消费者通知缺少订阅")
				}
				if mode == "permission" && !strings.Contains(got.err.Error(), "work") {
					t.Fatal("权限错误丢失被拒绝的主题")
				}
				if !c.Conn().IsConnected() {
					t.Fatal("异步消费错误不应关闭连接")
				}
			case <-ctx.Done():
				t.Fatal("未收到异步错误通知")
			}
			unblock()
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, record := range readLogs(t, buf) {
				if record["nats_event"] == "async_error" && record["level"] == "ERROR" {
					found = true
				}
			}
			if !found {
				t.Fatal("用户错误通知覆盖了内部日志")
			}
		})
	}
}

func TestAsyncErrorHandlerWithNilSubscriptionAndFilteredLogs(t *testing.T) {
	logger := logit.MustNew(logit.OptMinLevel(logit.FatalLevel), logit.OptWriter(logit.NewWriter(io.Discard)))
	old := logit.Default()
	logit.SetDefault(logger)
	defer func() {
		logit.SetDefault(old)
		_ = logit.Close(logger)
	}()
	rawErr := errors.New("异步发布错误")
	var calls int
	c, err := New(Config{
		Name: "filtered",
		URLs: []string{startServer(t)},
		AsyncErrorHandler: func(nc *nats.Conn, sub *nats.Subscription, err error) {
			calls++
			if nc == nil || sub != nil || err != rawErr {
				t.Error("未原样传递连接、nil 订阅和错误")
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.nc.Opts.AsyncErrorCB(c.nc, nil, rawErr)
	if calls != 1 {
		t.Fatal("日志过滤后未通知应用")
	}
}
