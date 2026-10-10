// Package bootstrap 提供通用应用初始化入口，供项目自己的 bootstrap 先调用，
// 再执行项目配置加载、客户端与业务服务装配。设计见 docs/bootstrap-design.md。
//
// 应用负责加载配置、创建启动 context 和资源关闭栈，并在初始化前登记关闭流程：
//
//	config := httpconfig.MustLoadAppConfig("./conf/app.yaml")
//	ctx, cancel := context.WithCancel(context.Background())
//	var resources lifecycle.Stack
//	defer func() {
//		cancel()
//		if err := resources.Close(); err != nil {
//			log.Printf("close resources: %v", err)
//		}
//	}()
//	bootstrap.MustInit(ctx, config, &resources)
//
// 示例使用标准库 context、log，以及 pixiu 的 biz/httpconfig、biz/bootstrap、
// lifecycle。初始化 panic 原样传播，已登记资源由应用的 defer 关闭。
//
// 第一版只检查参数和启动 context，不创建组件、不重复发布环境、不修改进程时区。
// 后续成功创建资源后立即登记关闭函数；登记失败须关闭尚未交付的资源。
// 共享资源只登记一次，日志先登记、最后关闭。本包不持有全局关闭栈。
// 接入后台任务后，应用必须先停止入口并等待任务退出，再关闭下游资源；
// 取消 context 不能代替等待。MustInit 仅供启动期串行调用，不承诺失败后重试。
package bootstrap
