package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/biz/internal/baseconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 启动声明和初始化由调用方串行执行；开始后不支持重新初始化。
var baseInitStarted bool

// MustBaseInit 执行通用初始化；参数缺失时 panic。
// 参数校验通过后封闭注册；先初始化日志，再加载和初始化已声明的 MySQL。
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
	if err := initLog(config.Log, resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize log: %w", err))
	}
	if err := initMySQL(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize MySQL: %w", err))
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
