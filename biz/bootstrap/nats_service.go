package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go/jetstream"
)

// NewNATSConsumerService 读取 nats-register.<instanceName>.yaml 并绑定 Handler，构造名为 nats:<instanceName> 的服务。
// 要求 env、Handler 及底层客户端已就绪；一个 Client 只创建一个消费组件，实际注册在 Run 中执行。
// 构造不启动订阅，错误原样返回；服务接入统一停收、排空和取消协议，共享客户端仍由资源栈关闭。
// 客户端通过 natsx.Named 查询，实例未登记或 CloseAll 开始后查询会 panic。
// 配置先预检并绑定 Handler；各类资源的通用参数校验和判重交给各自的 natsx 注册入口，失败也占用其注册次数。
// Stream subjects 列表在配置层必填；消费者在 streams[].consumers 中声明并继承父级 name，不支持仅引用外部 Stream。
// 三个入口各调用一次，空清单也占用次数，不能与手动注册混用。
// 各入口分阶段预检，后序校验失败时前序资源或消费可能已启动。
// nil 注册表视为空表，未引用的 Handler 不启动；全局 streamDefaults.replicas 始终必填且为正整数。
// namespace 来自 nats.<instanceName>.yaml 的客户端初始化配置，本文件不接受该字段。
// 配置填写原始名称和主题，前缀统一由 Client 的注册及发送入口添加；Handler 名称不变。
// 逐项 Stream 的 replicas 不得超过全局值，超限时在注册任何资源前返回错误。
// Stream maxBytes、retention、maxAge、duplicates 可选，省略或 null 沿用 Default；显式零值按 NATS 语义处理。
// JetStream ackWait、maxDeliver、maxAckPending、consume.workers、consume.maxMessages、retryDelay 可选，
// 省略或 null 沿用 Default，显式值的归一化及校验交给 natsx 注册入口和服务端。
// Core queue 非空时 subscribers 必须大于 1，否则在注册任何资源前返回错误。
// Run 注册期间可能执行 Handler；失败须中止启动并先排空已启动消费，再由资源栈关闭 Client，不回滚远端资源或业务。
// 注册 context 可由 Stop 取消，不设置整体注册期限；资源请求与 Core Flush 沿用 SDK 默认行为。
// Client 关闭检查沿用 natsx；长期 Handler 使用独立消息 context，不提供热更新或整批重试。
func NewNATSConsumerService(instanceName string, jetStreamHandlers *natsx.JetStreamHandlerRegistry, coreHandlers *natsx.CoreHandlerRegistry) (apprun.Service, error) {
	c, err := newNATSConsumer(instanceName, jetStreamHandlers, coreHandlers)
	if err != nil {
		return apprun.Service{}, err
	}
	return apprun.Service{
		Name:   "nats:" + c.name,
		Run:    c.Run,
		Stop:   c.Stop,
		Wait:   c.Wait,
		Cancel: c.Cancel,
	}, nil
}

// natsConsumer 管理一个共享客户端的消费者；连接始终由资源栈关闭。
type natsConsumer struct {
	name               string
	client             *natsx.Client
	batch              *natsRegistrations
	location           string
	mu                 sync.Mutex
	stopped            bool
	started            bool
	done               chan struct{}
	registerCtx        context.Context
	cancelRegistration context.CancelFunc
}

// newNATSConsumer 读取配置并绑定 Handler，准备可取消的注册 context。
func newNATSConsumer(instanceName string, jetStreamHandlers *natsx.JetStreamHandlerRegistry, coreHandlers *natsx.CoreHandlerRegistry) (*natsConsumer, error) {
	if instanceName == "" || strings.IndexFunc(instanceName, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) >= 0 {
		return nil, fmt.Errorf("bootstrap: invalid NATS instance name %q", instanceName)
	}
	// 先查询命名实例，避免未初始化的配置环境遮蔽 Named 的 panic。
	client := natsx.Named(instanceName)
	path := filepath.Join(env.ConfigDirPath(), "nats-register."+instanceName+".yaml")
	location := fmt.Sprintf("bootstrap: NATS %q config %q", instanceName, path)
	cfg, err := configx.Parse[natsRegistrationConfig](path)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", location, err)
	}
	batch, err := cfg.bind(jetStreamHandlers, coreHandlers)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", location, err)
	}
	registerCtx, cancelRegistration := context.WithCancel(context.Background())
	return &natsConsumer{
		name:               instanceName,
		client:             client,
		batch:              batch,
		location:           location,
		done:               make(chan struct{}),
		registerCtx:        registerCtx,
		cancelRegistration: cancelRegistration,
	}, nil
}

// Run 注册消费者并等待停收。启动注册可以被 Stop 取消，不阻塞其他组件停收。
func (c *natsConsumer) Run() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	if c.started {
		c.mu.Unlock()
		return apprun.ErrStarted
	}
	c.started = true
	c.mu.Unlock()
	err := c.register(c.registerCtx)
	if err != nil {
		c.mu.Lock()
		stopping := c.stopped
		c.mu.Unlock()
		if !stopping || !errors.Is(err, context.Canceled) && !errors.Is(err, natsx.ErrClosed) {
			return err
		}
	}
	<-c.done
	return nil
}

// Stop 停止注册和消费接入，保留已有 Handler 的 context 与共享连接。
func (c *natsConsumer) Stop() error {
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return nil
	}
	c.stopped = true
	c.cancelRegistration()
	close(c.done)
	c.mu.Unlock()
	c.client.StopConsumers()
	return nil
}

// Wait 等待实际消费排空，超时不关闭共享连接。
func (c *natsConsumer) Wait(ctx context.Context) error { return c.client.WaitConsumers(ctx) }

// Cancel 先停收，再协作取消剩余业务；共享连接仍由资源栈回收。
func (c *natsConsumer) Cancel() {
	_ = c.Stop()
	c.client.CancelConsumers()
}

func (c *natsConsumer) register(ctx context.Context) error {
	client, batch, location := c.client, c.batch, c.location
	if err := client.RegisterStreams(ctx, batch.streams...); err != nil {
		return fmt.Errorf("%s: register streams: %w", location, err)
	}
	if err := client.RegisterJetStreamConsumers(ctx, batch.jetStreamConsumers...); err != nil {
		return fmt.Errorf("%s: register JetStream consumers: %w", location, err)
	}
	if err := client.RegisterCoreConsumers(ctx, batch.coreConsumers...); err != nil {
		return fmt.Errorf("%s: register Core consumers: %w", location, err)
	}
	return nil
}

type natsRegistrationConfig struct {
	StreamDefaults struct {
		Replicas int `mapstructure:"replicas"`
	} `mapstructure:"streamDefaults"`
	Streams       []natsStreamRegistration `mapstructure:"streams"`
	CoreConsumers []natsCoreRegistration   `mapstructure:"coreConsumers"`
}

type natsStreamRegistration struct {
	Name       string                      `mapstructure:"name"`
	Subjects   []string                    `mapstructure:"subjects"`
	Replicas   *int                        `mapstructure:"replicas"`
	MaxBytes   *int64                      `mapstructure:"maxBytes"`
	Retention  *string                     `mapstructure:"retention"`
	MaxAge     *time.Duration              `mapstructure:"maxAge"`
	Duplicates *time.Duration              `mapstructure:"duplicates"`
	Consumers  []natsJetStreamRegistration `mapstructure:"consumers"`
}

type natsJetStreamRegistration struct {
	Durable        string         `mapstructure:"durable"`
	FilterSubjects []string       `mapstructure:"filterSubjects"`
	Handler        string         `mapstructure:"handler"`
	AckWait        *time.Duration `mapstructure:"ackWait"`
	MaxDeliver     *int           `mapstructure:"maxDeliver"`
	MaxAckPending  *int           `mapstructure:"maxAckPending"`
	Consume        struct {
		Workers     *int `mapstructure:"workers"`
		MaxMessages *int `mapstructure:"maxMessages"`
	} `mapstructure:"consume"`
	RetryDelay *time.Duration `mapstructure:"retryDelay"`
}

type natsCoreRegistration struct {
	Name        string `mapstructure:"name"`
	Subject     string `mapstructure:"subject"`
	Queue       string `mapstructure:"queue"`
	Subscribers int    `mapstructure:"subscribers"`
	Handler     string `mapstructure:"handler"`
}

type natsRegistrations struct {
	streams            []jetstream.StreamConfig
	jetStreamConsumers []natsx.JetStreamConsumer
	coreConsumers      []natsx.CoreConsumer
}

func (cfg *natsRegistrationConfig) bind(jsRegistry *natsx.JetStreamHandlerRegistry, coreRegistry *natsx.CoreHandlerRegistry) (*natsRegistrations, error) {
	replicas := cfg.StreamDefaults.Replicas
	if replicas <= 0 {
		return nil, errors.New("streamDefaults.replicas must be a positive integer")
	}
	jsHandlers := jsRegistry.Handlers()
	coreHandlers := coreRegistry.Handlers()
	batch := &natsRegistrations{}
	for i, spec := range cfg.Streams {
		if len(spec.Subjects) == 0 {
			return nil, fmt.Errorf("streams[%d] %q requires nonempty subjects", i, spec.Name)
		}
		effectiveReplicas := replicas
		if spec.Replicas != nil && *spec.Replicas > 0 {
			if *spec.Replicas > replicas {
				return nil, fmt.Errorf("streams[%d] %q replicas %d must not exceed streamDefaults.replicas %d", i, spec.Name, *spec.Replicas, replicas)
			}
			effectiveReplicas = *spec.Replicas
		}
		stream := natsx.DefaultStreamConfig(
			spec.Name,
			effectiveReplicas,
			spec.Subjects...,
		)
		// 仅覆盖显式容量，保留 0/-1 的不限量语义；账户及服务端配额继续由 NATS 校验。
		if spec.MaxBytes != nil {
			stream.MaxBytes = *spec.MaxBytes
		}
		if spec.Retention != nil {
			switch *spec.Retention {
			case "limits":
				stream.Retention = jetstream.LimitsPolicy
			case "interest":
				stream.Retention = jetstream.InterestPolicy
			case "workqueue":
				stream.Retention = jetstream.WorkQueuePolicy
			default:
				return nil, fmt.Errorf("streams[%d] %q retention %q must be one of limits, interest, workqueue", i, spec.Name, *spec.Retention)
			}
		}
		// 显式零时长须覆盖 Default；范围及组合约束继续由服务端校验。
		if spec.MaxAge != nil {
			stream.MaxAge = *spec.MaxAge
		}
		if spec.Duplicates != nil {
			stream.Duplicates = *spec.Duplicates
		}
		batch.streams = append(batch.streams, stream)
		// 按父级及内部声明顺序展开，Stream 名称只有父级这一处配置来源。
		for j, consumerSpec := range spec.Consumers {
			handler, ok := jsHandlers[consumerSpec.Handler]
			if !ok {
				return nil, fmt.Errorf("streams[%d].consumers[%d] %q/%q: unknown JetStream handler %q", i, j, spec.Name, consumerSpec.Durable, consumerSpec.Handler)
			}
			consumer := natsx.DefaultJetStreamConsumer(
				spec.Name,
				consumerSpec.Durable,
				handler,
				consumerSpec.FilterSubjects...,
			)
			// 仅覆盖显式配置的字段，保留零值语义，由 natsx 和服务端统一归一化及校验。
			if consumerSpec.AckWait != nil {
				consumer.Config.AckWait = *consumerSpec.AckWait
			}
			if consumerSpec.MaxDeliver != nil {
				consumer.Config.MaxDeliver = *consumerSpec.MaxDeliver
			}
			if consumerSpec.MaxAckPending != nil {
				consumer.Config.MaxAckPending = *consumerSpec.MaxAckPending
			}
			if consumerSpec.Consume.Workers != nil {
				consumer.Consume.Workers = *consumerSpec.Consume.Workers
			}
			if consumerSpec.Consume.MaxMessages != nil {
				consumer.Consume.MaxMessages = *consumerSpec.Consume.MaxMessages
			}
			if consumerSpec.RetryDelay != nil {
				consumer.RetryDelay = *consumerSpec.RetryDelay
			}
			batch.jetStreamConsumers = append(batch.jetStreamConsumers, consumer)
		}
	}
	for i, spec := range cfg.CoreConsumers {
		if spec.Queue != "" && spec.Subscribers <= 1 {
			return nil, fmt.Errorf("coreConsumers[%d] %q subscribers %d must be greater than 1 when queue is set", i, spec.Name, spec.Subscribers)
		}
		handler, ok := coreHandlers[spec.Handler]
		if !ok {
			return nil, fmt.Errorf("coreConsumers[%d] %q: unknown Core handler %q", i, spec.Name, spec.Handler)
		}
		// queue 为空时沿用广播语义，订阅数由 natsx 固定为 1。
		batch.coreConsumers = append(
			batch.coreConsumers,
			natsx.DefaultCoreQueueConsumer(
				spec.Name,
				spec.Subject,
				spec.Queue,
				spec.Subscribers,
				handler,
			),
		)
	}
	return batch, nil
}
