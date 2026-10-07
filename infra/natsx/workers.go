package natsx

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ConsumeConfig 配置启动阶段固定的消费并发和预取数量。
type ConsumeConfig struct {
	// Workers 必须为正数，限制同时执行的 handler 数；handler 须支持并发调用。
	Workers int
	// MaxMessages 是 SDK 预取上限，不含已经交给 worker 的消息；零值等于 Workers，负值无效。
	// 服务端 MaxAckPending 和 ACK 等待期限仍由业务 ConsumerConfig 决定。
	MaxMessages int
	// ErrorHandler 在模块记录消费运行错误后同步调用；nil 时仅记录日志。
	// 不得阻塞或在这里调用 Client.Close；心跳丢失后继续拉取，其他迭代器错误终止消费。
	ErrorHandler jetstream.ConsumeErrHandlerFunc
}

// ConsumeWithWorkers 仅在启动阶段建立 Messages 迭代器并启动固定数量的 worker，成功后立即返回。
// 不自动 ACK、NAK 或 Term；每条消息获得独立日志作用域和 Client 管理的 ctx。
// 仅返回启动错误；消费句柄由 Client 内部登记和管理，不通过返回值提供。
// Client.Close 统一排空，正常关闭完成或超时后取消消息 ctx；handler 内不得调用 Close。
func (c *Client) ConsumeWithWorkers(consumer jetstream.Consumer, cfg ConsumeConfig, handler func(context.Context, jetstream.Msg)) error {
	if consumer == nil || handler == nil {
		return errors.New("natsx: nil consumer or handler")
	}
	if cfg.Workers <= 0 || cfg.MaxMessages < 0 {
		return errors.New("natsx: workers must be positive and max messages must be non-negative")
	}
	if cfg.MaxMessages == 0 {
		cfg.MaxMessages = cfg.Workers
	}
	c.consumerMu.Lock()
	defer c.consumerMu.Unlock()
	if c.closing.Load() {
		return ErrClosed
	}
	iterator, err := consumer.Messages(jetstream.PullMaxMessages(cfg.MaxMessages))
	if err != nil {
		return err
	}
	// Stop 只取消取消息和调度；业务消息仍使用 Client 的 ctx，允许完成 ACK 和下游操作。
	ctx, cancel := context.WithCancel(c.handlerCtx)
	handle := &workerConsumer{
		iterator: iterator,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
	c.consumers = append(c.consumers, handle)
	go c.runWorkers(ctx, consumer, handle, cfg, handler)
	return nil
}

type workerConsumer struct {
	iterator jetstream.MessagesContext
	cancel   context.CancelFunc
	done     chan struct{}
}

func (w *workerConsumer) Stop() {
	w.cancel()
	w.iterator.Stop()
}

func (w *workerConsumer) Drain() { w.iterator.Drain() }

func (w *workerConsumer) Closed() <-chan struct{} { return w.done }

func (c *Client) logConsumeError(ctx context.Context, consumer jetstream.Consumer, err error) {
	if !logit.LoggerFromContext(ctx).Enabled(logit.ErrorLevel) {
		return
	}
	fields := []logit.Field{
		logit.Str("nats_name", c.name),
		logit.Str("nats_event", "consume_error"),
		logit.Err(err),
	}
	if info := consumer.CachedInfo(); info != nil {
		fields = append(fields, logit.Str("stream", info.Stream), logit.Str("consumer", info.Name))
	}
	logit.Output(ctx, logit.ErrorLevel, 0, "nats consumer", fields...)
}

func (c *Client) runWorkers(ctx context.Context, consumer jetstream.Consumer, handle *workerConsumer, cfg ConsumeConfig, handler func(context.Context, jetstream.Msg)) {
	jobs := make(chan jetstream.Msg)
	slots := make(chan struct{}, cfg.Workers)
	var workers sync.WaitGroup
	workers.Add(cfg.Workers)
	for range cfg.Workers {
		go func() {
			defer workers.Done()
			for msg := range jobs {
				msgCtx := MessageContext(c.handlerCtx, msg.Headers())
				begin := time.Now()
				handler(msgCtx, msg)
				message := &nats.Msg{
					Data:   msg.Data(),
					Header: msg.Headers(),
				}
				c.logResult(msgCtx, "jetStreamCallback", msg.Subject(), message, nil, begin, nil, nil)
				<-slots
			}
		}()
	}
	// 先停止迭代器和派发，再等 worker 退出，最后通知 Client.Close 消费已完全结束。
	defer close(handle.done)
	defer workers.Wait()
	defer close(jobs)
	defer handle.Stop()
	nextContext := jetstream.NextContext(ctx)
	for ctx.Err() == nil {
		// 满载时不调用 Next，避免把 SDK 预取消息再搬入另一层业务等待队列。
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		msg, err := handle.iterator.Next(nextContext)
		if err != nil {
			<-slots
			if ctx.Err() != nil || errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				return
			}
			c.logConsumeError(c.handlerCtx, consumer, err)
			if cfg.ErrorHandler != nil {
				cfg.ErrorHandler(handle, err)
			}
			if errors.Is(err, jetstream.ErrNoHeartbeat) {
				// SDK 已重置待拉取额度；继续 Next 可恢复拉取，无需重建 Consumer。
				continue
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case jobs <- msg:
		case <-ctx.Done():
			return
		}
	}
}
