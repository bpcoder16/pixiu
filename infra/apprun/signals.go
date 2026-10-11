package apprun

import (
	"context"
	"os"
	"os/signal"
	"syscall"
)

// RunSignals 显式订阅 SIGINT/SIGTERM，收到信号或 ctx 取消后按 Run 的协议关闭服务。
// 首次停机时恢复原有信号行为，使后续中断可直接终止进程；应用只应有一个信号所有者。
// 不接管 SIGKILL/SIGSTOP；信号处理不覆盖调用前的配置加载和初始化。
func (m *Manager) RunSignals(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	signalCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	return m.run(signalCtx, stop)
}
