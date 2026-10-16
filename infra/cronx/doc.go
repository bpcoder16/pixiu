// Package cronx 组合 gocron v2 与 logit，提供实例式 Cron、固定和随机间隔调度。
// 不读取配置文件、不持有全局调度器，不依赖 HTTP 或其他公开 infra 包。
//
// 独立构造并运行（需导入 context、log、time 及本包）：
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
//	go func() {
//		time.Sleep(3 * time.Second)
//		if err := scheduler.Shutdown(); err != nil {
//			log.Printf("close cron: %v", err)
//		}
//	}()
//	return scheduler.Run()
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
// New 不执行业务，但会创建引擎内部 goroutine，构造后必须 Shutdown。
// Run 阻塞到有界关闭结束，只能启动一次；Shutdown 支持先于 Run、重复和并发调用。
// 默认关闭总预算 15s，前 10s 自然排空，再取消业务并在剩余时间等待返回和解锁。
// 启动和关闭日志均纳入收尾等待；慢 Writer 不延长 Shutdown 总预算或阻止 Run 返回。
// 超时后任务或日志写入可能仍进行；Wait 不发起关闭、无等待上限，等待实际工作及日志写完。
// 先关闭服务，再关闭 Redis 等共享客户端，最后关闭日志；有界超时退出不保证完整排空。
// 使用 biz/bootstrap 可从独立 cron.yaml 构造并通过 CronService 交给 apprun，
// 默认管理器预算 20s 可覆盖独立 Cron；混合运行时总预算须覆盖各组件关闭期限。
package cronx
