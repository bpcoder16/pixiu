// Package cronx 组合 gocron v2 与 logit，提供实例式 Cron、固定和随机间隔调度。
// 不读取配置文件、不持有全局调度器，不依赖 HTTP 或其他公开 infra 包。
//
// HandlerRegistry 维护应用持有的具名函数清单，登记不读配置或启动调度：
//
//	func Heartbeat(context.Context) error {
//		return nil
//	}
//
//	var Registry = cronx.MustNewHandlerRegistry(Heartbeat)
//
// 注册表零值可用；MustRegister(handler, name) 支持显式稳定名称。
// 包级具名函数默认保留原始大小写；方法值、闭包、泛型函数必须显式命名。
// nil、非法名称、多个名称参数及重名均以带 cronx: 前缀的 error 值 panic；
// 登记失败不改变清单。Handlers 按登记顺序返回 []NamedHandler 独立副本，
// 可交给 bootstrap.NewCronScheduler 或自行绑定调度。
// 登记与快照读取可并发，首次使用后不得复制；后续登记不影响已构造的调度器。
//
// 独立构造并运行（需导入 context、errors、time 及本包）：
//
//	scheduler, err := cronx.New(cronx.Config{}, cronx.Job{
//		Name:    "heartbeat",
//		Every:   time.Second,
//		Timeout: 2 * time.Second,
//		Handler: func(ctx context.Context) error {
//			return ctx.Err()
//		},
//	})
//	if err != nil {
//		return err
//	}
//	runDone := make(chan error, 1)
//	go func() { runDone <- scheduler.Run() }()
//	time.Sleep(3 * time.Second)
//	stopErr := scheduler.Stop()
//	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
//	defer cancel()
//	waitErr := scheduler.Wait(ctx)
//	if waitErr != nil {
//		scheduler.Cancel()
//		return errors.Join(stopErr, waitErr)
//	}
//	return errors.Join(stopErr, <-runDone)
//
// Job 的 Cron、Every、RandomEvery 恰好选一个；Cron 为秒起始的六字段，不接受
// 内嵌时区或描述符。Location 默认 Asia/Shanghai，通过 time.LoadLocation 加载，
// 加载失败则返回构造错误；显式指定时区时保留原始对象，支持匿名或自定义名称的固定时区。
// 固定间隔 Every 和随机间隔 RandomEvery.Min 最短均为 100ms，小于下限时构造报错。
// 固定及随机间隔首次等待，不立即执行。
// 随机间隔为 [Min, Max)，不会按本轮完成时间重新起算；重叠触发跳过、不排队。
// 各任务独立并行，每轮有独立日志字段、耗时作用域和 logId。Handler 返回错误或
// panic 不终止 Scheduler。Timeout 只取消 context，实际返回后才释放执行位置。
// 执行结果通过日志观察，不提供状态统计接口；重叠跳过不单独输出日志。
// Handler 必须等待其子工作完成，不得使用 runtime.Goexit 或同步等待自身关闭。
//
// Config.Locker 可直接接收 gocron.Locker；所有任务共享该锁策略，并须设置正 Timeout。
// gocron-redis-lock 的连接、前缀、TTL 与续租由应用或 biz/bootstrap 配置，不扩展 lockx。
// 获取锁失败跳过本轮；解锁使用独立 1s context 并记录错误，等实际业务返回后才释放。
// 租约有效时提供同名任务互斥，不保证每个时间点集群只执行一次；各节点独立调度。
// Redis 插件失租不会通知或取消 Handler，关键业务仍须幂等和数据端约束。
//
// New 不执行业务，但会创建引擎 goroutine，构造后须 Stop 并 Wait(ctx)。
// Run 阻塞到停收，Stop 封闭新任务和获锁入口；Wait 等待任务、锁操作及日志实际结束。
// 业务 context 独立于引擎关闭通知，仅业务自身期限或显式 Cancel 才取消尚在排空的任务。
// Cancel 先停收再协作取消剩余任务，也可直接调用；随后仍可通过 Wait 确认实际收尾。
// 组件没有独立排空计时，托管运行与其他服务共享 apprun 默认 30 秒期限。
// 超时后调用 Cancel，入口继续回收共享资源，资源栈默认整体 20 秒，日志最后关闭。
// 取消不能强杀业务，也不提前解锁。构造方式与当前 API 示例见上文；文件装配由 biz/bootstrap 提供。
package cronx
