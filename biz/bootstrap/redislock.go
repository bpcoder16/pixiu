package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/biz/lockx"
	"github.com/bpcoder16/pixiu/biz/lockx/redislock"
)

type redisLockRegistration struct {
	name      string
	isDefault bool
	redisName string
	config    redislock.Config
}

var redisLocks []redisLockRegistration

// MustRegisterRedisLock 按代码配置声明锁执行器，不读取锁配置文件或建立连接。
// 仅在 MustBaseInit 前串行调用；name 与 redisName 只允许非空 ASCII 字母、数字、下划线和连字符。
// 重复名称、第二个默认实例或初始化开始后注册均 panic；cfg 按值保存，零值采用 redislock 默认值。
// MustBaseInit 在 Redis 就绪后通过 redisName 解析共享客户端，并校验锁配置及禁用命令重试的要求。
// 多个锁实例可共享同一 Redis；实例名不改变业务 key，执行器不拥有或关闭 Redis 资源。
func MustRegisterRedisLock(name string, isDefault bool, redisName string, cfg redislock.Config) {
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started; cannot register RedisLock %q", name))
	}
	if err := validateInstanceName("RedisLock", name); err != nil {
		panic(err)
	}
	if err := validateInstanceName("Redis", redisName); err != nil {
		panic(err)
	}
	for _, registered := range redisLocks {
		if registered.name == name {
			panic(fmt.Errorf("bootstrap: RedisLock %q is already registered", name))
		}
		if isDefault && registered.isDefault {
			panic(fmt.Errorf("bootstrap: default RedisLock %q is already registered; cannot register %q as default", registered.name, name))
		}
	}
	redisLocks = append(redisLocks, redisLockRegistration{
		name:      name,
		isDefault: isDefault,
		redisName: redisName,
		config:    cfg,
	})
}

func initRedisLocks() error {
	for _, registration := range redisLocks {
		if err := initRedisLock(registration); err != nil {
			return fmt.Errorf("instance %q using Redis %q: %w", registration.name, registration.redisName, err)
		}
	}
	return nil
}

func initRedisLock(registration redisLockRegistration) error {
	client, err := namedRedis(registration.redisName)
	if err != nil {
		return err
	}
	backend, err := redislock.New(client, registration.config)
	if err != nil {
		return err
	}
	// 执行器只引用共享客户端，不向资源栈重复登记 Redis 关闭。
	if registration.isDefault {
		_, err = lockx.NewDefault(registration.name, backend)
	} else {
		_, err = lockx.NewNamed(registration.name, backend)
	}
	return err
}
