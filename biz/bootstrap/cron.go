package bootstrap

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/cronx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/logit"
	redislock "github.com/go-co-op/gocron-redis-lock/v2"
	"github.com/go-co-op/gocron/v2"
)

// NewCronService 严格读取 cron.yaml，绑定 Handler 并构造名为 cron 的服务，构造错误原样返回。
// 只启用 jobs 中列出的任务；允许多注册 Handler，配置引用未注册的名称会报错。
// 注释或删除某个 job 后重启即可停用；全部注释时允许零任务运行。
// 构造不执行业务；启用锁时借用已初始化的命名 Redis，运行与关闭由服务管理器接管。
// 只转换关闭早于 Run 的 ErrClosed，保留实际运行和排空错误。
func NewCronService(handlers ...cronx.NamedHandler) (apprun.Service, error) {
	s, err := newCronScheduler(handlers...)
	if err != nil {
		return apprun.Service{}, err
	}
	return apprun.Service{
		Name: "cron",
		Run: func() error {
			err := s.Run()
			if errors.Is(err, cronx.ErrClosed) {
				return nil
			}
			return err
		},
		Stop:   s.Stop,
		Wait:   s.Wait,
		Cancel: s.Cancel,
	}, nil
}

type cronConfig struct {
	LoggerName string          `mapstructure:"loggerName"`
	Lock       *cronLockConfig `mapstructure:"lock"`
	Jobs       []cronJobConfig `mapstructure:"jobs"`
}

type cronJobConfig struct {
	Name string `mapstructure:"name"`
	// 指针保留调度字段是否出现，避免 every: 0 与 cron 同时配置时被当作单选。
	Cron         *string           `mapstructure:"cron"`
	Every        *time.Duration    `mapstructure:"every"`
	RandomEvery  *cronRandomConfig `mapstructure:"randomEvery"`
	Timeout      time.Duration     `mapstructure:"timeout"`
	SuccessLevel string            `mapstructure:"successLevel"`
}

type cronRandomConfig struct {
	Min time.Duration `mapstructure:"min"`
	Max time.Duration `mapstructure:"max"`
}

type cronLockConfig struct {
	RedisName     string        `mapstructure:"redisName"`
	KeyPrefix     string        `mapstructure:"keyPrefix"`
	TTL           time.Duration `mapstructure:"ttl"`
	RenewInterval time.Duration `mapstructure:"renewInterval"`
}

// newCronScheduler 读取配置并构造调度器；Redis 锁借用已初始化的命名客户端。
func newCronScheduler(handlers ...cronx.NamedHandler) (*cronx.Scheduler, error) {
	path := filepath.Join(env.ConfigDirPath(), "cron.yaml")
	cfg, err := configx.Parse[cronConfig](path)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: load Cron config: %w", err)
	}
	jobs, err := cfg.bind(handlers)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: Cron config %q: %w", path, err)
	}
	schedulerConfig := cronx.Config{
		Location:   env.TimeLocation(),
		LoggerName: cfg.LoggerName,
	}
	if cfg.Lock != nil {
		schedulerConfig.Locker, err = cfg.Lock.locker()
		if err != nil {
			return nil, fmt.Errorf("bootstrap: Cron lock from %q: %w", path, err)
		}
	}
	scheduler, err := cronx.New(schedulerConfig, jobs...)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: create Cron scheduler from %q: %w", path, err)
	}
	return scheduler, nil
}

func (c *cronConfig) bind(handlers []cronx.NamedHandler) ([]cronx.Job, error) {
	bindings := make(map[string]cronx.Handler, len(handlers))
	for _, handler := range handlers {
		if handler.Name == "" || strings.TrimSpace(handler.Name) != handler.Name || handler.Handler == nil {
			return nil, fmt.Errorf("invalid Cron handler %q", handler.Name)
		}
		if _, exists := bindings[handler.Name]; exists {
			return nil, fmt.Errorf("duplicate Cron handler %q", handler.Name)
		}
		bindings[handler.Name] = handler.Handler
	}
	// 配置决定启用哪些任务，未被 jobs 引用的已注册 Handler 不创建调度任务。
	jobs := make([]cronx.Job, 0, len(c.Jobs))
	for _, config := range c.Jobs {
		handler, ok := bindings[config.Name]
		if !ok {
			return nil, fmt.Errorf("unknown Cron handler %q", config.Name)
		}
		job := cronx.Job{
			Name:    config.Name,
			Handler: handler,
			Timeout: config.Timeout,
		}
		count := 0
		if config.Cron != nil {
			count++
			job.Cron = *config.Cron
		}
		if config.Every != nil {
			count++
			job.Every = *config.Every
		}
		if config.RandomEvery != nil {
			count++
			job.RandomEvery = &cronx.RandomInterval{
				Min: config.RandomEvery.Min,
				Max: config.RandomEvery.Max,
			}
		}
		if count != 1 {
			return nil, fmt.Errorf("job %q requires exactly one schedule", config.Name)
		}
		switch config.SuccessLevel {
		case "", "info":
			job.SuccessLevel = logit.InfoLevel
		case "debug":
			job.SuccessLevel = logit.DebugLevel
		default:
			return nil, fmt.Errorf("job %q: invalid successLevel %q", config.Name, config.SuccessLevel)
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

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
