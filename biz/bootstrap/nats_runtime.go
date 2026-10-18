package bootstrap

import (
	"errors"
	"sync/atomic"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/natsx"
)

// NATSJetStreamService 将成功构造的 JetStreamRuntime 交给应用管理器；nil 属于调用错误并 panic。
// 不关闭借用的 Client，不管理信号或创建资源。
func NATSJetStreamService(runtime *natsx.JetStreamRuntime) apprun.Service {
	if runtime == nil {
		panic("bootstrap: nil NATS runtime")
	}
	return natsRuntimeService("nats:"+runtime.Name(), runtime.Run, runtime.Shutdown, func() error { return runtime.Status().LastError })
}

// NATSCoreService 适配 CoreRuntime；就绪依赖由业务入口可取消地等待 WaitReady。
func NATSCoreService(runtime *natsx.CoreRuntime) apprun.Service {
	if runtime == nil {
		panic("bootstrap: nil NATS Core runtime")
	}
	return natsRuntimeService("nats-core:"+runtime.Name(), runtime.Run, runtime.Shutdown, func() error { return runtime.Status().LastError })
}

func natsRuntimeService(name string, run, shutdown func() error, lastError func() error) apprun.Service {
	var stopped atomic.Bool
	return apprun.Service{
		Name: name,
		Run: func() error {
			err := run()
			// 只转换本适配器已请求的干净停机，不能用 errors.Is 吞掉复合故障。
			if errors.Is(err, natsx.ErrClosed) && stopped.Load() && lastError() == nil {
				return nil
			}
			return err
		},
		Shutdown: func() error { stopped.Store(true); return shutdown() },
	}
}
