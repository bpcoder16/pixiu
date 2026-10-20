package natsx

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// JetStreamHandler 返回 nil 时由组件 ACK，错误或 panic 按配置重投。
// handler 须支持配置的并发度并在返回前完成本条工作；不得自行 ACK/NAK/Term 或调用 Client.Close。
// 消息只在回调期间借用，不得修改 Header，跨回调保留数据须自行复制。
type JetStreamHandler func(context.Context, jetstream.Msg) error

// CoreHandler 返回的错误仅记录日志，不重投；panic 恢复后记录堆栈。
// 需要响应时显式调用 Msg.Respond；不得在回调中调用 Client.Close。
type CoreHandler func(context.Context, *nats.Msg) error

// JetStreamConsumer 绑定远端 Pull Consumer 配置、本地消费参数与 handler。
// Config 必须提供 Durable 并使用 AckExplicitPolicy，且显式配置正值 AckWait 或非空 BackOff；
// 两者同时配置时 BackOff 优先，Name 非空时必须等于 Durable，DeliverSubject 必须为空。
type JetStreamConsumer struct {
	Stream string
	Config jetstream.ConsumerConfig
	// Consume 复用固定 worker 配置；本入口 Workers 零值默认 1，MaxMessages 零值等于 Workers 的两倍。
	Consume ConsumeConfig
	// RetryDelay 为正时是业务失败后的 NAK 延迟；零值不主动确认，等待服务端按 BackOff 重投，
	// 未配置 BackOff 时使用 AckWait；负值无效，BackOff 非空时 RetryDelay 必须为零。
	RetryDelay time.Duration
	Handler    JetStreamHandler
}

// CoreConsumer 声明 Core 订阅；Queue 为空时广播，否则为同组竞争消费。
// 同一订阅内串行调用 handler，多个订阅之间可并发执行且不保证处理顺序，handler 须支持并发调用。
// pending 溢出由 SDK 异步错误日志报告，不保证重投。
type CoreConsumer struct {
	// Name 是当前 Client 内唯一的本地声明标识，必填且不能全为空白，不发送给服务端。
	Name    string
	Subject string
	Queue   string
	// Subscribers 仅在 Queue 非空时控制当前 Client 创建的同组订阅数；零值默认 1，负值无效。
	// Queue 为空时忽略此数量，始终只创建一个广播订阅。
	Subscribers int
	Handler     CoreHandler
	// PendingMessages 和 PendingBytes 为零时沿用 SDK 默认，正值覆盖，负值无效。
	PendingMessages int
	PendingBytes    int
}

// DefaultStreamConfig 构建可直接修改的原生 Stream 配置，复制 subjects；校验由注册入口和 SDK 承担。
// 副本数由 replicas 原样指定；默认 LimitsPolicy、文件存储、5 GiB 容量、满额拒绝新消息、最多保留 90 天（约 3 个月）、2 分钟去重，并保留发布确认。
// 单条消息的 Header 与正文总大小默认不超过 512 KiB。
// LimitsPolicy 下 ACK 不释放容量；过期不等待消费，未 ACK 的旧消息到期也会删除。
func DefaultStreamConfig(name string, replicas int, subjects ...string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:     name,                   // Stream 名称，同一账户内唯一。
		Subjects: slices.Clone(subjects), // 接收的主题；复制后与调用方切片独立。
		// 默认使用 LimitsPolicy，可按需覆盖；三种保留策略均受容量和过期限制：
		// LimitsPolicy：按容量和过期规则保留，ACK 不删除消息。
		// InterestPolicy：所有匹配消费者 ACK 后删除；无匹配消费者时不保留。
		// WorkQueuePolicy：任一消费者 ACK 后删除；不同消费者的主题范围不能重叠。
		Retention:  jetstream.LimitsPolicy,
		Storage:    jetstream.FileStorage, // 消息存储到磁盘。
		Replicas:   replicas,              // 调用方按部署指定副本数。
		MaxBytes:   5 << 30,               // 单个 Stream 的容量上限：5 GiB。
		MaxMsgSize: 512 << 10,             // 单条消息总大小上限：512 KiB，包含 Header 与正文。
		Discard:    jetstream.DiscardNew,  // 容量满时拒绝新消息，向发布方返回错误。
		MaxAge:     90 * 24 * time.Hour,   // 固定 90 天，约 3 个月；到期自动删除。
		Duplicates: 2 * time.Minute,       // 同一消息 ID 在 2 分钟内发布去重。
		NoAck:      false,                 // 保留发布确认 PubAck，与消费者 ACK 不同。
	}
}

// DefaultJetStreamConsumer 构建可直接修改的消费声明，不建立连接或校验参数。
// 复制 subjects 后写入 FilterSubjects（要求 NATS Server >= 2.10），不传主题时消费整个 Stream。
// 默认显式确认、AckWait 为 60 秒、最多投递 5 次（含首次），不设置 BackOff 或延迟 NAK。
// MaxAckPending 显式设为 1000；MaxRequestBatch 和 MaxRequestExpires 保留零值。
// Workers 默认设为 1；MaxMessages 保留零值，由注册入口按最终并发配置归一化。
func DefaultJetStreamConsumer(stream, durable string, handler JetStreamHandler, subjects ...string) JetStreamConsumer {
	return JetStreamConsumer{
		Stream: stream,
		Config: jetstream.ConsumerConfig{
			Durable:        durable,                     // 持久化消费者名称，用于恢复消费进度。
			FilterSubjects: slices.Clone(subjects),      // 仅消费匹配主题的消息；为空时消费整个 Stream，切片与调用方独立。
			AckPolicy:      jetstream.AckExplicitPolicy, // 每条消息都须单独 ACK，由组件在 handler 成功后确认。
			AckWait:        60 * time.Second,            // 投递后等待 ACK 的时限；默认 60 秒，超时未确认则重投。
			MaxDeliver:     5,                           // 每条消息最多投递 5 次（含首次），达到上限后停止重投。
			MaxAckPending:  1000,                        // 同一 Durable 下所有实例合计最多保留 1000 条未确认消息。
		},
		Consume: ConsumeConfig{
			Workers: 1,
		},
		Handler: handler,
	}
}

// DefaultCoreConsumer 构建可直接修改的 Core 广播消费声明，不建立连接或校验参数。
// Subscribers 显式设为 1；PendingMessages 和 PendingBytes 保留零值，注册时沿用 SDK 默认限额。
func DefaultCoreConsumer(name, subject string, handler CoreHandler) CoreConsumer {
	return CoreConsumer{
		Name:        name,
		Subject:     subject,
		Subscribers: 1,
		Handler:     handler,
	}
}

// DefaultCoreQueueConsumer 构建可直接修改的 Core 同组消费声明，不建立连接或校验参数。
// subscribers 原样写入，注册时零值默认 1、负值无效；pending 默认值与 DefaultCoreConsumer 相同。
// Queue 为空时沿用 CoreConsumer 的广播语义。
func DefaultCoreQueueConsumer(name, subject, queue string, subscribers int, handler CoreHandler) CoreConsumer {
	consumer := DefaultCoreConsumer(name, subject, handler)
	consumer.Queue = queue
	consumer.Subscribers = subscribers
	return consumer
}

// checkRegistration 只检查 context 是否存在和初始关闭状态；资源操作的取消由 SDK 处理。
func (c *Client) checkRegistration(ctx context.Context) error {
	if ctx == nil {
		return errors.New("natsx: nil registration context")
	}
	if c.closing.Load() || c.nc.IsClosed() {
		return ErrClosed
	}
	return nil
}

// RegisterStreams 在启动期依次创建或复用 Stream，不自动更新、删除已有资源。
// 只在启动阶段串行调用，不与 Close 并发。
// 通过入口检查后，每个 Client 只允许调用一次；须一次传入全部配置，空清单或失败也不可重试。
// 配置直接交给 SDK；同批名称不得重复。失败保留已创建资源，由应用中止启动并 Close。
func (c *Client) RegisterStreams(ctx context.Context, configs ...jetstream.StreamConfig) error {
	if err := c.checkRegistration(ctx); err != nil {
		return err
	}
	if !c.streamRegistrationStarted.CompareAndSwap(false, true) {
		return errors.New("natsx: RegisterStreams already called")
	}
	seen := make(map[string]struct{}, len(configs))
	for _, cfg := range configs {
		if strings.TrimSpace(cfg.Name) == "" {
			return errors.New("natsx: empty stream name")
		}
		if _, exists := seen[cfg.Name]; exists {
			return fmt.Errorf("natsx: duplicate stream %q", cfg.Name)
		}
		seen[cfg.Name] = struct{}{}
	}
	for _, cfg := range configs {
		if _, err := c.js.CreateStream(ctx, cfg); err != nil {
			return fmt.Errorf("natsx: register stream %q: %w", cfg.Name, err)
		}
	}
	return nil
}

// RegisterJetStreamConsumers 先准备本批全部远端 Consumer，再启动固定 worker；返回前可能执行 handler。
// 只在依赖就绪后的启动期串行调用，不与 Close 并发。
// 通过入口检查后，每个 Client 只允许调用一次；须一次传入全部配置，空清单或失败也不可重试。
// ctx 仅传给资源 RPC，不额外限制本地 worker 启动，也不影响长期 handler。
// 失败中止启动，由 Client.Close 回收已启动消费；
// 已创建远端资源不删除，已执行业务不回滚，不支持失败后重新登记整批。
func (c *Client) RegisterJetStreamConsumers(ctx context.Context, consumers ...JetStreamConsumer) error {
	if err := c.checkRegistration(ctx); err != nil {
		return err
	}
	if !c.jetStreamRegistrationStarted.CompareAndSwap(false, true) {
		return errors.New("natsx: RegisterJetStreamConsumers already called")
	}
	// 只复制需要归一化的声明，不改写调用方的消费参数。
	specs := slices.Clone(consumers)
	seen := make(map[[2]string]struct{}, len(specs))
	for i := range specs {
		spec := &specs[i]
		if strings.TrimSpace(spec.Stream) == "" || strings.TrimSpace(spec.Config.Durable) == "" || spec.Handler == nil {
			return fmt.Errorf("natsx: consumer %d requires stream, durable and handler", i)
		}
		if spec.Config.Name != "" && spec.Config.Name != spec.Config.Durable {
			return fmt.Errorf("natsx: consumer %q name must equal durable", spec.Config.Durable)
		}
		if spec.Config.DeliverSubject != "" {
			return fmt.Errorf("natsx: consumer %q requires pull mode; DeliverSubject must be empty", spec.Config.Durable)
		}
		if spec.Config.AckPolicy != jetstream.AckExplicitPolicy {
			return fmt.Errorf("natsx: consumer %q requires explicit ack", spec.Config.Durable)
		}
		if spec.Config.AckWait <= 0 && len(spec.Config.BackOff) == 0 {
			return fmt.Errorf("natsx: consumer %q requires AckWait or BackOff", spec.Config.Durable)
		}
		if spec.Consume.Workers < 0 || spec.Consume.MaxMessages < 0 || spec.RetryDelay < 0 {
			return fmt.Errorf("natsx: consumer %q has negative consumption parameter", spec.Config.Durable)
		}
		if len(spec.Config.BackOff) > 0 && spec.RetryDelay > 0 {
			return fmt.Errorf("natsx: consumer %q cannot combine BackOff with positive RetryDelay", spec.Config.Durable)
		}
		if spec.Consume.Workers == 0 {
			spec.Consume.Workers = 1
		}
		if spec.Consume.MaxMessages == 0 {
			spec.Consume.MaxMessages = 2 * spec.Consume.Workers
		}
		if err := validatePullLimits(spec.Config, spec.Consume.MaxMessages); err != nil {
			return err
		}
		key := [2]string{spec.Stream, spec.Config.Durable}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("natsx: duplicate consumer %q/%q", key[0], key[1])
		}
		seen[key] = struct{}{}
	}
	prepared := make([]jetstream.Consumer, len(specs))
	for i, spec := range specs {
		consumer, err := c.js.CreateConsumer(ctx, spec.Stream, spec.Config)
		if err != nil {
			return fmt.Errorf("natsx: register consumer %q/%q: %w", spec.Stream, spec.Config.Durable, err)
		}
		// 以服务端有效配置校验拉取限制；全部校验完成前不启动本批 handler。
		if err = validatePullLimits(consumer.CachedInfo().Config, spec.Consume.MaxMessages); err != nil {
			return err
		}
		prepared[i] = consumer
	}
	for i, spec := range specs {
		maxDeliver := prepared[i].CachedInfo().Config.MaxDeliver
		err := c.consumeWithWorkers(prepared[i], spec.Consume, func(ctx context.Context, msg jetstream.Msg) error {
			return handleJetStream(ctx, msg, spec.Handler, spec.RetryDelay, maxDeliver)
		})
		if err != nil {
			return fmt.Errorf("natsx: start consumer %q/%q: %w", spec.Stream, spec.Config.Durable, err)
		}
	}
	return nil
}

// RegisterCoreConsumers 在启动期建立全部订阅并执行一次 Flush；空清单不 Flush。
// 只在启动阶段串行调用，不与 Close 并发。
// 通过入口检查后，每个 Client 只允许调用一次；须一次传入全部声明，空清单或失败也不可重试。
// 同批 Name 不得重复，全部声明校验完成前不建立订阅。
// ctx 有 deadline 时用于 Flush，否则沿用 SDK 默认 Flush；不额外限制本地订阅建立。
// handler 使用 Client 生命周期，返回前可能开始处理消息。
// Flush 不保证逐个订阅授权成功，异步故障通过现有日志报告。
// 失败后由应用中止启动并 Close，不回滚已执行业务；调用方不可自行停止受管订阅。
func (c *Client) RegisterCoreConsumers(ctx context.Context, consumers ...CoreConsumer) error {
	if err := c.checkRegistration(ctx); err != nil {
		return err
	}
	if !c.coreRegistrationStarted.CompareAndSwap(false, true) {
		return errors.New("natsx: RegisterCoreConsumers already called")
	}
	seen := make(map[string]struct{}, len(consumers))
	for i, spec := range consumers {
		if strings.TrimSpace(spec.Name) == "" ||
			strings.TrimSpace(spec.Subject) == "" ||
			spec.Handler == nil ||
			(spec.Queue != "" && strings.TrimSpace(spec.Queue) == "") ||
			spec.Subscribers < 0 ||
			spec.PendingMessages < 0 ||
			spec.PendingBytes < 0 {
			return fmt.Errorf("natsx: invalid Core consumer %d", i)
		}
		if _, exists := seen[spec.Name]; exists {
			return fmt.Errorf("natsx: duplicate Core consumer %q", spec.Name)
		}
		seen[spec.Name] = struct{}{}
	}
	for _, spec := range consumers {
		subscribers := 1
		if spec.Queue != "" && spec.Subscribers > 0 {
			subscribers = spec.Subscribers
		}
		for range subscribers {
			sub, err := c.subscribeHandler(spec.Subject, spec.Queue, func(ctx context.Context, msg *nats.Msg) error {
				return invokeHandler(ctx, func() error { return spec.Handler(ctx, msg) })
			})
			if err != nil {
				return fmt.Errorf("natsx: register Core subject %q: %w", spec.Subject, err)
			}
			if spec.PendingMessages != 0 || spec.PendingBytes != 0 {
				messages, bytes, pendingErr := sub.PendingLimits()
				if pendingErr != nil {
					return pendingErr
				}
				if spec.PendingMessages > 0 {
					messages = spec.PendingMessages
				}
				if spec.PendingBytes > 0 {
					bytes = spec.PendingBytes
				}
				if setErr := sub.SetPendingLimits(messages, bytes); setErr != nil {
					return setErr
				}
			}
		}
	}
	if len(consumers) > 0 {
		var err error
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			err = c.nc.FlushWithContext(ctx)
		} else {
			// 启动方无需设置整体期限；复用 SDK Flush 默认等待，不额外创建计时器或 goroutine。
			err = c.nc.Flush()
		}
		if err != nil {
			return fmt.Errorf("natsx: flush Core registrations: %w", err)
		}
	}
	return nil
}

// invokeHandler 只恢复当前回调的 panic；业务自行启动的 goroutine 仍由业务负责。
func invokeHandler(ctx context.Context, handler func() error) (err error) {
	defer func() {
		if value := recover(); value != nil {
			logit.AddField(ctx, logit.Str("panic_stack", string(debug.Stack())))
			err = fmt.Errorf("natsx: handler panic: %v", value)
		}
	}()
	return handler()
}

func handleJetStream(ctx context.Context, msg jetstream.Msg, handler JetStreamHandler, retryDelay time.Duration, maxDeliver int) error {
	metadata, err := msg.Metadata()
	if err != nil {
		return fmt.Errorf("natsx: message metadata: %w", err)
	}
	logit.AddField(ctx,
		logit.Str("stream", metadata.Stream),
		logit.Str("consumer", metadata.Consumer),
		logit.Uint64("num_delivered", metadata.NumDelivered),
	)
	err = invokeHandler(ctx, func() error { return handler(ctx, msg) })
	// 正常排空期间 context 保持有效；超时或永久关闭后不再发起新的确认。
	if canceled := ctx.Err(); canceled != nil {
		logit.AddField(ctx, logit.Str("outcome", "canceled"))
		return errors.Join(err, canceled)
	}
	if err == nil {
		logit.AddField(ctx, logit.Str("outcome", "ack"))
		return msg.Ack()
	}
	if maxDeliver > 0 && metadata.NumDelivered >= uint64(maxDeliver) {
		logit.AddField(ctx, logit.Str("outcome", "exhausted"))
		return err
	}
	logit.AddField(ctx, logit.Str("outcome", "retry"))
	if retryDelay > 0 {
		return errors.Join(err, msg.NakWithDelay(retryDelay))
	}
	// 零值保持未确认，由服务端的 BackOff/AckWait 决定重投时机。
	return err
}
