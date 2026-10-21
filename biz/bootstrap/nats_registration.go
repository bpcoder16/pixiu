package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go/jetstream"
)

// RegisterNATSFromConfig 读取 nats-register.<instanceName>.yaml，绑定 Handler 并批量注册。
// 要求 env 已初始化；仅在依赖就绪后启动期串行调用，借用命名 Client，不另建连接或接管关闭。
// 客户端通过 natsx.Named 查询，实例未登记或 CloseAll 开始后查询会 panic。
// 配置先预检并绑定 Handler；各类资源的通用参数校验和判重交给各自的 natsx 注册入口，失败也占用其注册次数。
// Stream subjects 列表在配置层必填；三个入口各调用一次，空清单也占用次数，不能与手动注册混用。
// 各入口分阶段预检，后序校验失败时前序资源或消费可能已启动。
// nil 注册表视为空表，未引用的 Handler 不启动；全局 streamDefaults.replicas 始终必填且为正整数。
// 逐项 Stream 的 replicas 不得超过全局值，超限时在注册任何资源前返回错误。
// Core queue 非空时 subscribers 必须大于 1，否则在注册任何资源前返回错误。
// 返回前可能执行 Handler；失败须中止启动并由原资源栈关闭 Client，不回滚远端资源或业务。
// ctx 必须非 nil；其校验与 Client 关闭检查沿用 natsx，ctx 不控制长期 Handler，不提供热更新或整批重试。
func RegisterNATSFromConfig(ctx context.Context, instanceName string, jetStreamHandlers *natsx.JetStreamHandlerRegistry, coreHandlers *natsx.CoreHandlerRegistry) error {
	if instanceName == "" || strings.IndexFunc(instanceName, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-')
	}) >= 0 {
		return fmt.Errorf("bootstrap: invalid NATS instance name %q", instanceName)
	}
	// 先查询命名实例，避免未初始化的配置环境遮蔽 Named 的 panic。
	client := natsx.Named(instanceName)
	path := filepath.Join(env.ConfigDirPath(), "nats-register."+instanceName+".yaml")
	location := fmt.Sprintf("bootstrap: NATS %q config %q", instanceName, path)
	cfg, err := configx.Parse[natsRegistrationConfig](path)
	if err != nil {
		return fmt.Errorf("%s: %w", location, err)
	}
	batch, err := cfg.bind(jetStreamHandlers, coreHandlers)
	if err != nil {
		return fmt.Errorf("%s: %w", location, err)
	}
	if err = client.RegisterStreams(ctx, batch.streams...); err != nil {
		return fmt.Errorf("%s: register streams: %w", location, err)
	}
	if err = client.RegisterJetStreamConsumers(ctx, batch.jetStreamConsumers...); err != nil {
		return fmt.Errorf("%s: register JetStream consumers: %w", location, err)
	}
	if err = client.RegisterCoreConsumers(ctx, batch.coreConsumers...); err != nil {
		return fmt.Errorf("%s: register Core consumers: %w", location, err)
	}
	return nil
}

type natsRegistrationConfig struct {
	StreamDefaults struct {
		Replicas int `mapstructure:"replicas"`
	} `mapstructure:"streamDefaults"`
	Streams            []natsStreamRegistration    `mapstructure:"streams"`
	JetStreamConsumers []natsJetStreamRegistration `mapstructure:"jetStreamConsumers"`
	CoreConsumers      []natsCoreRegistration      `mapstructure:"coreConsumers"`
}

type natsStreamRegistration struct {
	Name     string   `mapstructure:"name"`
	Subjects []string `mapstructure:"subjects"`
	Replicas *int     `mapstructure:"replicas"`
}

type natsJetStreamRegistration struct {
	Stream         string   `mapstructure:"stream"`
	Durable        string   `mapstructure:"durable"`
	FilterSubjects []string `mapstructure:"filterSubjects"`
	Handler        string   `mapstructure:"handler"`
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
		batch.streams = append(batch.streams, natsx.DefaultStreamConfig(spec.Name, effectiveReplicas, spec.Subjects...))
	}
	for i, spec := range cfg.JetStreamConsumers {
		handler, ok := jsHandlers[spec.Handler]
		if !ok {
			return nil, fmt.Errorf("jetStreamConsumers[%d] %q/%q: unknown JetStream handler %q", i, spec.Stream, spec.Durable, spec.Handler)
		}
		batch.jetStreamConsumers = append(
			batch.jetStreamConsumers,
			natsx.DefaultJetStreamConsumer(
				spec.Stream,
				spec.Durable,
				handler,
				spec.FilterSubjects...,
			),
		)
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
