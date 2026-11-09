//go:build !pixiu_bootstrap_no_redis

package bootstrap

import (
	"errors"
	"strings"
	"time"

	redislock "github.com/go-co-op/gocron-redis-lock/v2"
	"github.com/go-co-op/gocron/v2"
)

func (c cronLockConfig) locker() (gocron.Locker, error) {
	if c.RedisName == "" || strings.TrimSpace(c.RedisName) != c.RedisName {
		return nil, errors.New("invalid redisName")
	}
	if c.KeyPrefix == "" || strings.TrimSpace(c.KeyPrefix) != c.KeyPrefix {
		return nil, errors.New("invalid keyPrefix")
	}
	if c.TTL < 0 || c.RenewInterval < 0 {
		return nil, errors.New("negative ttl or renewInterval")
	}
	if c.TTL == 0 {
		c.TTL = 30 * time.Second
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = c.TTL / 3
	}
	if c.RenewInterval <= 0 || c.RenewInterval >= c.TTL {
		return nil, errors.New("renewInterval must be positive and less than ttl")
	}
	client, err := namedRedis(c.RedisName)
	if err != nil {
		return nil, err
	}
	return redislock.NewRedisLockerWithOptions(
		client.Client(),
		redislock.WithKeyPrefix(c.KeyPrefix),
		redislock.WithAutoExtendDuration(c.RenewInterval),
		redislock.WithRedsyncOptions(redislock.WithExpiry(c.TTL), redislock.WithTries(1)),
	)
}
