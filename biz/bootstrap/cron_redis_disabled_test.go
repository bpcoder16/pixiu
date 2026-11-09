//go:build pixiu_bootstrap_no_redis

package bootstrap_test

import (
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestCronWithoutRedis(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeCronConfig(t, "jobs: []\n")
		service, err := bootstrap.NewCronService()
		if err != nil {
			t.Fatal(err)
		}
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Fatal(err)
		}
		writeCronConfig(t, "lock: {redisName: cache, keyPrefix: 'test:'}\njobs: []\n")
		service, err = bootstrap.NewCronService()
		if err == nil || !strings.Contains(err.Error(), "pixiu_bootstrap_no_redis") || service.Run != nil {
			t.Fatalf("禁用 Redis 后配置 Cron 锁必须明确失败: %v", err)
		}
	})
}
