package natsx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go/jetstream"
)

func newWorkerTestClient(t *testing.T, closeTimeout time.Duration) (*Client, jetstream.Consumer) {
	t.Helper()
	c, err := New(Config{
		Name:         "workers",
		URLs:         []string{startServer(t)},
		CloseTimeout: closeTimeout,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := c.JetStream().CreateStream(ctx, jetstream.StreamConfig{
		Name:     "WORKERS",
		Subjects: []string{"workers.jobs"},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatal(err)
	}
	consumer, err := c.JetStream().CreateConsumer(ctx, "WORKERS", jetstream.ConsumerConfig{
		Durable:   "worker",
		AckPolicy: jetstream.AckExplicitPolicy,
		AckWait:   time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return c, consumer
}

// 内部句柄用于验证排空、停止和完成通知，不由公开启动接口返回。
func lastWorkerHandle(t *testing.T, c *Client) jetstream.ConsumeContext {
	t.Helper()
	c.consumerMu.Lock()
	defer c.consumerMu.Unlock()
	if len(c.consumers) == 0 {
		t.Fatal("启动消费后未登记内部句柄")
	}
	return c.consumers[len(c.consumers)-1]
}

func publishWorkerMessages(t *testing.T, c *Client, count int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := range count {
		id := fmt.Sprintf("job-%d", i)
		msgCtx := logit.NewContextScope(ctx)
		logit.AddMeta(msgCtx, logit.Str(logit.LogId, id))
		if _, err := c.PublishJetStream(msgCtx, "workers.jobs", []byte(id), id); err != nil {
			t.Fatal(err)
		}
	}
}

func waitWorkerPending(t *testing.T, consumer jetstream.Consumer, want int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		info, err := consumer.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if info.NumAckPending == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("未收到预期预取消息: pending=%d, want=%d", info.NumAckPending, want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestConsumeWithWorkersBoundsConcurrencyAndDrains(t *testing.T) {
	buf := captureLogs(t)
	c, consumer := newWorkerTestClient(t, 3*time.Second)
	const workers = 3
	const total = 2 * workers
	publishWorkerMessages(t, c, total)
	entered := make(chan struct{}, total)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var active, completed atomic.Int32
	err := c.ConsumeWithWorkers(consumer, ConsumeConfig{
		Workers:     workers,
		MaxMessages: total,
	}, func(ctx context.Context, msg jetstream.Msg) {
		if current := active.Add(1); current > workers {
			t.Errorf("并发超限: %d", current)
		}
		defer active.Add(-1)
		id := string(msg.Data())
		if got, _ := logit.LogIDFromContext(ctx); got != id {
			t.Errorf("消息 logId 错误: %q, want=%q", got, id)
		}
		logit.AddField(ctx, logit.Str("worker_job", id))
		entered <- struct{}{}
		<-release
		if ctx.Err() != nil {
			t.Errorf("正常排空取消了消息 ctx: %v", ctx.Err())
		}
		logit.Info(ctx, "worker completed")
		if err := msg.Ack(); err != nil {
			t.Error(err)
		}
		completed.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	handle := lastWorkerHandle(t, c)
	for range workers {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("没有并发启动全部 worker")
		}
	}
	// 在业务处理阻塞时确认消息已到客户端；Close 必须处理完这些预取消息。
	waitWorkerPending(t, consumer, total)
	closed := make(chan error, 1)
	go func() { closed <- c.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("任务尚未结束 Close 就返回: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("排空未结束")
	}
	if completed.Load() != total || active.Load() != 0 {
		t.Fatalf("未等待全部任务: completed=%d, active=%d", completed.Load(), active.Load())
	}
	select {
	case <-handle.Closed():
	default:
		t.Fatal("消费句柄未通知完成")
	}
	var records int
	for _, record := range readLogs(t, buf) {
		if record["msg"] == "worker completed" {
			records++
			if record[logit.LogId] != record["worker_job"] {
				t.Errorf("并发消息日志作用域串联错误: %v", record)
			}
		}
	}
	if records != total {
		t.Fatalf("消息完成日志数量: %d, want=%d", records, total)
	}
}

func TestConsumeWithWorkersStopWaitsAndDiscardsBufferedMessages(t *testing.T) {
	c, consumer := newWorkerTestClient(t, 3*time.Second)
	publishWorkerMessages(t, c, 6)
	entered := make(chan struct{}, 6)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	var completed atomic.Int32
	err := c.ConsumeWithWorkers(consumer, ConsumeConfig{
		Workers:     2,
		MaxMessages: 4,
	}, func(ctx context.Context, msg jetstream.Msg) {
		entered <- struct{}{}
		<-release
		if ctx.Err() != nil {
			t.Errorf("Stop 不应取消正在执行的消息 ctx: %v", ctx.Err())
		}
		if err := msg.Ack(); err != nil {
			t.Error(err)
		}
		completed.Add(1)
	})
	if err != nil {
		t.Fatal(err)
	}
	handle := lastWorkerHandle(t, c)
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("worker 未启动")
		}
	}
	waitWorkerPending(t, consumer, 4)
	handle.Stop()
	select {
	case <-handle.Closed():
		t.Fatal("Closed 没有等待正在执行的任务")
	default:
	}
	unblock()
	select {
	case <-handle.Closed():
	case <-time.After(3 * time.Second):
		t.Fatal("Stop 后任务未完成")
	}
	if completed.Load() != 2 {
		t.Fatalf("Stop 后仍处理预取消息: %d", completed.Load())
	}
}

func TestConsumeWithWorkersCloseTimeoutStopsDispatch(t *testing.T) {
	c, consumer := newWorkerTestClient(t, 80*time.Millisecond)
	publishWorkerMessages(t, c, 6)
	entered := make(chan struct{}, 6)
	var started atomic.Int32
	err := c.ConsumeWithWorkers(consumer, ConsumeConfig{
		Workers:     2,
		MaxMessages: 4,
	}, func(ctx context.Context, _ jetstream.Msg) {
		started.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
	})
	if err != nil {
		t.Fatal(err)
	}
	handle := lastWorkerHandle(t, c)
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("worker 未启动")
		}
	}
	if err := c.Close(); !errors.Is(err, ErrCloseTimeout) {
		t.Fatalf("Close 超时错误: %v", err)
	}
	select {
	case <-handle.Closed():
	case <-time.After(time.Second):
		t.Fatal("超时后未取消 worker 并结束调度")
	}
	if started.Load() != 2 {
		t.Fatalf("超时后仍启动预取任务: %d", started.Load())
	}
}

func TestConsumeWithWorkersValidatesMaxRequestExpires(t *testing.T) {
	for _, maxExpires := range []time.Duration{0, 10 * time.Second, jetstream.DefaultExpires - 1, jetstream.DefaultExpires, time.Minute} {
		t.Run(maxExpires.String(), func(t *testing.T) {
			captureLogs(t)
			c, consumer := newWorkerTestClient(t, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			config := consumer.CachedInfo().Config
			config.MaxRequestExpires = maxExpires
			consumer, err := c.JetStream().UpdateConsumer(ctx, "WORKERS", config)
			if err != nil {
				t.Fatal(err)
			}
			publishWorkerMessages(t, c, 1)
			finished := make(chan error, 1)
			subscriptions := c.Conn().NumSubscriptions()
			err = c.ConsumeWithWorkers(consumer, ConsumeConfig{Workers: 1}, func(_ context.Context, msg jetstream.Msg) {
				finished <- msg.Ack()
			})
			if maxExpires > 0 && maxExpires < jetstream.DefaultExpires {
				if err == nil || !strings.Contains(err.Error(), "MaxRequestExpires") {
					t.Fatalf("不兼容的拉取期限未在启动时拒绝: %v", err)
				}
				if got := c.Conn().NumSubscriptions(); got != subscriptions {
					t.Fatalf("非法配置启动了拉取订阅: got=%d, want=%d", got, subscriptions)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatalf("消息 ACK 失败: %v", err)
				}
			case <-ctx.Done():
				t.Fatal("合法的拉取期限配置未能正常消费消息")
			}
		})
	}
}

func TestConsumeWithWorkersRejectsInvalidArguments(t *testing.T) {
	c, consumer := newWorkerTestClient(t, time.Second)
	handler := func(context.Context, jetstream.Msg) {}
	for _, tc := range []struct {
		name     string
		consumer jetstream.Consumer
		config   ConsumeConfig
		handler  func(context.Context, jetstream.Msg)
	}{
		{"nil consumer", nil, ConsumeConfig{Workers: 1}, handler},
		{"nil handler", consumer, ConsumeConfig{Workers: 1}, nil},
		{"zero workers", consumer, ConsumeConfig{}, handler},
		{"negative workers", consumer, ConsumeConfig{Workers: -1}, handler},
		{"negative prefetch", consumer, ConsumeConfig{Workers: 1, MaxMessages: -1}, handler},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.ConsumeWithWorkers(tc.consumer, tc.config, tc.handler); err == nil {
				t.Fatal("非法配置未拒绝")
			}
		})
	}
}

// 用确定的错误序列覆盖心跳恢复，不依赖真实网络等待两次默认心跳间隔。
type workerErrorConsumer struct {
	jetstream.Consumer
	iterator jetstream.MessagesContext
}

func (c workerErrorConsumer) Messages(...jetstream.PullMessagesOpt) (jetstream.MessagesContext, error) {
	return c.iterator, nil
}

type workerErrorIterator struct {
	errors []error
}

func (i *workerErrorIterator) Next(...jetstream.NextOpt) (jetstream.Msg, error) {
	err := i.errors[0]
	i.errors = i.errors[1:]
	return nil, err
}

func (*workerErrorIterator) Stop()  {}
func (*workerErrorIterator) Drain() {}

func TestConsumeWithWorkersContinuesAfterMissingHeartbeat(t *testing.T) {
	captureLogs(t)
	c, consumer := newWorkerTestClient(t, time.Second)
	called := make(chan error, 2)
	err := c.ConsumeWithWorkers(workerErrorConsumer{
		Consumer: consumer,
		iterator: &workerErrorIterator{errors: []error{
			jetstream.ErrNoHeartbeat,
			jetstream.ErrConsumerDeleted,
		}},
	}, ConsumeConfig{
		Workers: 2,
		ErrorHandler: func(_ jetstream.ConsumeContext, err error) {
			called <- err
		},
	}, func(context.Context, jetstream.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	handle := lastWorkerHandle(t, c)
	select {
	case <-handle.Closed():
	case <-time.After(time.Second):
		t.Fatal("终止错误未停止消费")
	}
	for _, want := range []error{jetstream.ErrNoHeartbeat, jetstream.ErrConsumerDeleted} {
		select {
		case got := <-called:
			if !errors.Is(got, want) {
				t.Errorf("错误通知: %v, want=%v", got, want)
			}
		default:
			t.Fatalf("心跳异常后未继续拉取，缺少错误通知: %v", want)
		}
	}
}
