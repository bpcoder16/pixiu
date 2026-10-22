package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/bpcoder16/pixiu/infra/apprun"
)

// ServiceFactory 延迟构造一个常驻服务；返回错误时须自行回收本轮部分构造的资源。
type ServiceFactory func() (apprun.Service, error)

// Start 按顺序构造、登记服务，全部成功后接管 SIGINT/SIGTERM 并阻塞至运行和关闭结束。
// 构造或登记失败时停止后续构造，逆序回收已构造服务并合并错误；nil 构造函数返回错误。
// 运行后由管理器统一停收并排空，不重复回收；运行期总关闭预算默认 30 秒，不含构造及启动前失败回收。
// 自定义关闭预算使用 StartWithConfig；自定义运行 context 使用 NewAppManager 自行装配。
// 应用只应有一个信号所有者。
func Start(factories ...ServiceFactory) error {
	return startServices(30*time.Second, factories...)
}

// StartConfig 显式配置运行期总关闭预算，不包含构造、资源栈和日志关闭。
type StartConfig struct {
	ShutdownTimeout time.Duration
}

// StartWithConfig 与 Start 使用同一启动链路；显式期限不得小于 5 秒。
func StartWithConfig(cfg StartConfig, factories ...ServiceFactory) error {
	if cfg.ShutdownTimeout < 5*time.Second {
		return fmt.Errorf("bootstrap: explicit app shutdown timeout must be at least 5s: %s", cfg.ShutdownTimeout)
	}
	return startServices(cfg.ShutdownTimeout, factories...)
}

func startServices(timeout time.Duration, factories ...ServiceFactory) (err error) {
	manager, err := NewAppManager(timeout)
	if err != nil {
		return err
	}
	services := make([]apprun.Service, 0, len(factories))
	defer func() {
		if len(services) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		// 构造失败也先通知全部已构造服务停收，再共享一个期限收尾。
		for _, service := range slices.Backward(services) {
			if service.Stop != nil {
				err = errors.Join(err, service.Stop())
			}
		}
		for _, service := range slices.Backward(services) {
			var waitErr error
			if service.Wait != nil {
				waitErr = service.Wait(ctx)
				err = errors.Join(err, waitErr)
			}
			// Wait 失败不能确认业务已结束，与管理器一样发出取消兜底。
			if (waitErr != nil || ctx.Err() != nil) && service.Cancel != nil {
				service.Cancel()
			}
		}
	}()
	for i, factory := range factories {
		if factory == nil {
			return fmt.Errorf("bootstrap: nil service factory at index %d", i)
		}
		service, constructErr := factory()
		if constructErr != nil {
			return constructErr
		}
		// 登记前纳入回收，确保本轮登记失败的服务也会关闭。
		services = append(services, service)
		if registerErr := manager.Register(service); registerErr != nil {
			return registerErr
		}
	}
	// 运行阶段由管理器接管关闭职责，返回后不再执行启动前回收。
	services = nil
	return manager.RunSignals()
}

// NewAppManager 创建由调用方持有的服务管理器，不注册服务或订阅信号。
// 可省略关闭期限或传入 0，沿用 apprun 默认的 30 秒；最多传入一个期限，正值不得小于 5 秒。
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
