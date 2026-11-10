//go:build !pixiu_bootstrap_no_mysql || !pixiu_bootstrap_no_postgresql

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/env"
)

type outboxRegistration struct {
	backend      outbox.Backend
	databaseName string
	build        func() map[outbox.HandlerKey]outbox.Handler
}

var registeredOutbox *outboxRegistration

// MustRegisterOutbox 声明唯一的默认 Outbox，仅在 MustBaseInit 前串行调用。
// databaseName 选择已声明或由应用初始化的命名主库；声明顺序不限，不要求数据库为默认实例。
// 声明不读取配置、查询客户端或调用工厂；依赖就绪后才调用 handlerFactory，并复制其返回的 Handler 表。
// 后端或名称非法、nil 工厂、重复声明、后端被构建标签禁用或初始化已开始时 panic。
// 只借用数据库和 Handler 资源；表迁移、补偿调度和底层资源关闭由现有流程负责。
func MustRegisterOutbox(backend outbox.Backend, databaseName string, handlerFactory func() map[outbox.HandlerKey]outbox.Handler) {
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started; cannot register Outbox"))
	}
	if registeredOutbox != nil {
		panic(fmt.Errorf("bootstrap: Outbox is already registered"))
	}
	var disabledTag string
	switch backend {
	case outbox.MySQL:
		disabledTag = outboxMySQLDisabledTag
	case outbox.PostgreSQL:
		disabledTag = outboxPostgreSQLDisabledTag
	default:
		panic(fmt.Errorf("bootstrap: invalid Outbox backend %q", backend))
	}
	if disabledTag != "" {
		panic(fmt.Errorf("bootstrap: cannot register Outbox using %s with build tag %s", backend, disabledTag))
	}
	if err := validateInstanceName("Outbox database", databaseName); err != nil {
		panic(err)
	}
	if handlerFactory == nil {
		panic(fmt.Errorf("bootstrap: nil Outbox handler factory"))
	}
	registeredOutbox = &outboxRegistration{
		backend:      backend,
		databaseName: databaseName,
		build:        handlerFactory,
	}
}

func initOutbox() error {
	if registeredOutbox == nil {
		return nil
	}
	registration := registeredOutbox
	var cfg outbox.StoreConfig
	switch registration.backend {
	case outbox.MySQL:
		cfg = outboxMySQLConfig(registration.databaseName)
	case outbox.PostgreSQL:
		cfg = outboxPostgreSQLConfig(registration.databaseName)
	}
	cfg.Location = env.TimeLocation()
	// 先解析依赖并构造 Handler，工厂失败时不提前发布默认 Store；工厂 panic 原样传播。
	handlers := registration.build()
	if _, err := outbox.NewDefaultStore(cfg); err != nil {
		return err
	}
	_, err := outbox.NewDefaultWorker(handlers)
	return err
}
