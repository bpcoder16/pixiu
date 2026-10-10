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
//	logit.Info(ctx, "application initialized")
//
// 示例使用标准库 context、log，以及 pixiu 的 biz/httpconfig、biz/bootstrap、
// lifecycle、logit。初始化 panic 原样传播，已登记资源由应用的 defer 关闭。
//
// 日志固定使用 rotatefile，format 必须显式设置为 text/json；默认启动目录下的 log 目录、每小时轮转、
// 每个分流文件保留 48 个实际文件。config.Log 可配置 format、caller、dir 目录、names、rotate。
// dir 相对路径基于 env.RootDirPath()，绝对路径直接使用；目录自动创建并检查可写权限。
// 默认 Info 写 env.AppName()+".info.log"，Debug 写 env.AppName()+".debug.log"，
// Warn、Error、Fatal 写 env.AppName()+".wf.log"，每条日志仅写入其对应文件。
// names 中的每个名字创建独立 Logger，文件基名为 env.AppName()+"."+name，
// 同样分流为 .info.log、.debug.log、.wf.log；不允许空名、路径分隔符或文件名冲突。
// 例如 names: [worker] 时，可以通过 logit.Named("worker").Info(ctx, "worker ready")，
// 或 logit.Info(logit.WithLoggerName(ctx, "worker"), "worker ready") 写入命名日志。
// caller 由 config.Log.Caller 控制，默认关闭，对默认及命名 Logger 同时生效。
// release 输出 Info 及以上，debug/test 输出 Debug 及以上。
// 配置加载不初始化日志；每个 Logger 创建成功后向应用栈登记 logit.Close(logger)，
// 全部创建并登记成功后，再发布默认及命名 Logger。
// 构造失败仅清理当前 Logger 已打开的 Writer；此前已登记的 Logger 由 main 的 defer 收尾。
// 调用方须传入空资源栈，让后续资源关闭期间仍可写日志。
// 不重复发布环境或修改进程时区；日志时间与轮转均使用进程本地时区。
// 后续成功创建资源后立即登记关闭函数；登记失败须关闭尚未交付的资源。
// 共享资源只登记一次，日志先登记、最后关闭。本包不持有全局关闭栈。
// 接入后台任务后，应用必须先停止入口并等待任务退出，再关闭下游资源；
// 取消 context 不能代替等待。MustInit 仅供启动期串行调用，不承诺失败后重试。
package bootstrap
