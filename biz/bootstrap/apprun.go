package bootstrap

import (
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
)

// NewAppManager 创建由调用方持有的服务管理器，不注册服务或订阅信号。
// 可省略关闭期限或传入 0，沿用 apprun 默认的 20 秒；最多传入一个期限，正值不得小于 5 秒。
func NewAppManager(shutdownTimeout ...time.Duration) (*apprun.Manager, error) {
	if len(shutdownTimeout) > 1 {
		return nil, fmt.Errorf("bootstrap: expected at most one app shutdown timeout")
	}
	var timeout time.Duration
	if len(shutdownTimeout) == 1 {
		timeout = shutdownTimeout[0]
	}
	if timeout > 0 && timeout < 5*time.Second {
		return nil, fmt.Errorf("bootstrap: app shutdown timeout must be at least 5s: %s", timeout)
	}
	manager, err := apprun.New(timeout)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: create app manager: %w", err)
	}
	return manager, nil
}
