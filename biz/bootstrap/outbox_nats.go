//go:build !pixiu_bootstrap_no_nats && (!pixiu_bootstrap_no_mysql || !pixiu_bootstrap_no_postgresql)

package bootstrap

import (
	"fmt"
	"slices"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/biz/outbox/natsoutbox"
)

// MustRegisterJetStreamOutbox 声明默认 Outbox 的 JetStream 快捷组合，与 MustRegisterOutbox 共用唯一声明位置。
// natsNames 至少包含一个合法命名客户端；名称按值保存，客户端在 MustBaseInit 的 NATS 初始化后解析。
// 一次构造的 JetStream Handler 支持全部传入客户端，按消息的 Client 选择；混合 Handler 使用通用入口。
// 其余声明、默认实例、迁移和调度约束与 MustRegisterOutbox 相同。
func MustRegisterJetStreamOutbox(backend outbox.Backend, databaseName string, natsNames ...string) {
	if len(natsNames) == 0 {
		panic(fmt.Errorf("bootstrap: Outbox requires at least one NATS client"))
	}
	for _, name := range natsNames {
		if err := validateInstanceName("Outbox NATS", name); err != nil {
			panic(err)
		}
	}
	names := slices.Clone(natsNames)
	MustRegisterOutbox(backend, databaseName, func() map[outbox.HandlerKey]outbox.Handler {
		return map[outbox.HandlerKey]outbox.Handler{
			{
				Kind:    natsoutbox.JetStreamKind,
				Version: natsoutbox.JetStreamVersion,
			}: natsoutbox.NewJetStreamHandler(names...),
		}
	})
}
