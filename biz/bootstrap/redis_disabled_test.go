//go:build pixiu_bootstrap_no_redis

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutRedis(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_redis", func() {
		bootstrap.MustRegisterRedis("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/redisx",
		"github.com/bpcoder16/pixiu/biz/lockx/redislock",
		"github.com/redis/go-redis/",
		"github.com/go-co-op/gocron-redis-lock/",
		"github.com/go-redsync/",
	)
}
