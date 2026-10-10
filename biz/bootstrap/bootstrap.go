package bootstrap

import (
	"context"
	"fmt"

	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustInit 执行通用初始化；参数缺失或启动 context 已结束时 panic。
// 日志最先初始化并登记，后续组件按需接入，以保证日志最后关闭。
// resources 由应用创建为空栈并负责关闭，调用方须提前登记关闭 defer。
func MustInit(ctx context.Context, config *httpconfig.AppConfig, resources *lifecycle.Stack) {
	if err := validateParams(ctx, config, resources); err != nil {
		panic(err)
	}
	if err := initLog(config.Log, resources); err != nil {
		panic(fmt.Errorf("bootstrap: initialize log: %w", err))
	}
}

func validateParams(ctx context.Context, config *httpconfig.AppConfig, resources *lifecycle.Stack) error {
	if ctx == nil {
		return fmt.Errorf("bootstrap: nil context")
	}
	if config == nil {
		return fmt.Errorf("bootstrap: nil app config")
	}
	if resources == nil {
		return fmt.Errorf("bootstrap: nil resource stack")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("bootstrap: initialize: %w", err)
	}
	return nil
}
