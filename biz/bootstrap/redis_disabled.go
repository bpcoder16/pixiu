//go:build pixiu_bootstrap_no_redis

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterRedis 在禁用 Redis 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterRedis(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register Redis instance %q with build tag pixiu_bootstrap_no_redis", name))
}

func initRedis(*lifecycle.Stack) error {
	return nil
}

func initRedisLocks() error {
	return nil
}
