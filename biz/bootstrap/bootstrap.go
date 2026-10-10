package bootstrap

import (
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/biz/internal/baseconfig"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 启动声明和初始化由调用方串行执行；开始后不支持重新初始化。
var baseInitStarted bool

// MustBaseInit 执行通用初始化；参数缺失时 panic。
// 参数校验通过后封闭注册；先应用 env 时区，再依次初始化日志、已声明的 MySQL 和 Redis。
// 必须在其他 goroutine 并发使用时间或日志前调用，运行期不再修改 time.Local。
// 日志最先登记，以保证最后关闭；重复调用或初始化失败后重试会 panic。
// resources 由应用创建为空栈并负责关闭，调用方须提前登记关闭 defer。
func MustBaseInit(config *baseconfig.AppConfig, resources *lifecycle.Stack) {
	if err := validateParams(config, resources); err != nil {
		panic(err)
	}
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started"))
	}
	baseInitStarted = true
	// 轮转文件创建后即启动后台清理，因此必须在初始化日志前设置默认时区。
	time.Local = env.TimeLocation()
	if err := initLog(config.Log, resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize log: %w", err))
	}
	if err := mysqlInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize MySQL: %w", err))
	}
	if err := redisInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Redis: %w", err))
	}
}

func validateParams(config *baseconfig.AppConfig, resources *lifecycle.Stack) error {
	if config == nil {
		return fmt.Errorf("bootstrap: nil app config")
	}
	if resources == nil {
		return fmt.Errorf("bootstrap: nil resource stack")
	}
	return nil
}
