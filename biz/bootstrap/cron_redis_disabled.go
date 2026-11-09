//go:build pixiu_bootstrap_no_redis

package bootstrap

import (
	"fmt"

	"github.com/go-co-op/gocron/v2"
)

// 普通 Cron 仍可使用；配置分布式锁时明确报错，避免降级为无锁执行。
func (c cronLockConfig) locker() (gocron.Locker, error) {
	return nil, fmt.Errorf("bootstrap: Cron Redis lock %q is unavailable with build tag pixiu_bootstrap_no_redis", c.RedisName)
}
