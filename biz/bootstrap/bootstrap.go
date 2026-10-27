package bootstrap

import (
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 启动声明和初始化由调用方串行执行；开始后不支持重新初始化。
var baseInitStarted bool

// MustBaseInit 执行通用初始化；参数缺失时 panic。
// 参数校验通过后封闭注册；先应用 env 时区，再依次初始化日志、OSS、STS、阿里云 ES、MySQL、Redis、Elasticsearch 和 NATS。
// 必须在其他 goroutine 并发使用时间或日志前调用，运行期不再修改 time.Local。
// 服务停止后，共享资源按 NATS、Elasticsearch、Redis、MySQL、阿里云 ES、STS、OSS、日志的顺序关闭；未声明的组件跳过。
// 成功返回后再创建和保存模块 Logger；重复调用或初始化失败后重试会 panic。
// resources 由应用创建为空栈并负责关闭，调用方须提前登记关闭 defer。
// NATS 消费者由 Start 排空后，才回收资源栈中的客户端；服务排空超时先取消任务，再执行有界资源栈回收。
func MustBaseInit(config *appconfig.AppConfig, resources *lifecycle.Stack) {
	mustBaseInit(config, resources, true)
}

// MustBaseInitWithoutLogit 提供迁移过渡入口，执行除 logit 外的通用初始化。
// 跳过日志配置校验、Logger 创建与发布、日志关闭登记及 stdout/stderr 重定向。
// 参数、时区及声明约束与 MustBaseInit 相同，两个入口共用一次初始化状态。
// 必须在其他 goroutine 并发使用时间或日志前调用；自管日志由应用负责最后关闭。
func MustBaseInitWithoutLogit(config *appconfig.AppConfig, resources *lifecycle.Stack) {
	mustBaseInit(config, resources, false)
}

func mustBaseInit(config *appconfig.AppConfig, resources *lifecycle.Stack, initializeLog bool) {
	if err := validateParams(config, resources); err != nil {
		panic(err)
	}
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started"))
	}
	baseInitStarted = true
	// 轮转文件创建后即启动后台清理，因此必须在初始化日志前设置默认时区。
	time.Local = env.TimeLocation()
	if initializeLog {
		if err := initLog(config.Log, resources); err != nil {
			panic(fmt.Errorf("bootstrap: initialize log: %w", err))
		}
	}
	if err := aliyunOSSInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Aliyun OSS: %w", err))
	}
	if err := aliyunSTSInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Aliyun STS: %w", err))
	}
	if err := aliyunESInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Aliyun ES: %w", err))
	}
	if err := mysqlInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize MySQL: %w", err))
	}
	if err := redisInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Redis: %w", err))
	}
	if err := elasticsearchInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize Elasticsearch: %w", err))
	}
	if err := natsInstances.initialize(resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize NATS: %w", err))
	}
}

func validateParams(config *appconfig.AppConfig, resources *lifecycle.Stack) error {
	if config == nil {
		return fmt.Errorf("bootstrap: nil app config")
	}
	if resources == nil {
		return fmt.Errorf("bootstrap: nil resource stack")
	}
	return nil
}
