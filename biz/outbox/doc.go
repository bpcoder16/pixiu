// Package outbox 提供事务发件箱：同事务登记，提交后主动发送，失败由项目 cron 补偿。
// 支持 MySQL/PostgreSQL；Store 借用主库，Worker 支持同步和异步发送，不启动后台调度。
// 构建标签 pixiu_bootstrap_no_mysql / pixiu_bootstrap_no_postgresql 同时排除对应后端的驱动错误适配，
// NewStore/NewDefaultStore 拒绝被禁用的后端；默认构建保留两种后端。
//
// 应用先通过 schemamigrate 显式登记 &outbox.Record{}，再注册默认 Store 和 Worker：
//
//	_, err := outbox.NewDefaultStore(outbox.StoreConfig{
//		Backend:  outbox.MySQL,
//		MasterDB: mysqlClient.MasterDB,
//		Location: env.TimeLocation(),
//	})
//	if err != nil {
//		return err
//	}
//	_, err = outbox.NewDefaultWorker(map[outbox.HandlerKey]outbox.Handler{
//		{
//			Kind:    "project.notification",
//			Version: 1,
//		}: sendNotification,
//	})
//	if err != nil {
//		return err
//	}
//	var taskID int64
//	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
//		if err := tx.Create(&order).Error; err != nil {
//			return err
//		}
//		var err error
//		taskID, err = outbox.EnqueueTx(tx, outbox.Task{
//			Kind:      "project.notification",
//			Version:   1,
//			Payload:   payload,
//			ExpiresAt: time.Now().Add(24 * time.Hour),
//		})
//		return err
//	})
//	if err != nil {
//		return err
//	}
//	outbox.SendAsync(taskID)
//	return nil
//
// 示例需导入 outbox、infra/env、gorm.io/gorm、time；ctx、db、mysqlClient、
// order、payload 由应用提供。sendNotification 的签名为
// func(context.Context, outbox.Delivery) error，同步发送成功才返回 nil。
// db.Transaction 必须是最外层事务；提交后的发送失败不能回滚已提交业务。
//
// 每个进程只登记一个默认 Store 和 Worker，不提供 named 管理。NewDefaultWorker 自动绑定默认 Store，
// Handler 表在创建时复制，可包含多个 Kind/Version；完成全部初始化后再运行业务。
// 重复登记返回错误且不覆盖，初始化失败可重试；默认实例不支持替换、重置或运行期增改 Handler。
// DefaultStore/DefaultWorker 返回已登记实例，未登记时 panic；包级 EnqueueTx/Get/Send/SendAsync/RetryPending 同样如此。
// 可通过 outbox.Get(ctx, taskID) 查询状态。仅登记任务的进程可以只初始化默认 Store。
// NewStore/NewWorker 保留显式实例用法，不隐式设置默认实例；所有实例均不拥有数据库或 MQ 资源。
//
// SendAsync(taskID) 在调用方检查默认 Worker，再直接启动 goroutine；显式实例可调用 worker.SendAsync(taskID)。
// 每次发送从 context.Background() 派生独立的 5 秒超时 context，不携带请求 context 的值，
// Handler 和发送日志从任务记录恢复 logId。异步入口不返回错误，失败额外记录 OutboxAsyncFailed；
// pending 任务由 cron 补偿，不使用 taskpool、不排队、不限制并发，也不在内部重试。
// 5 秒是 context 期限，Handler 须响应取消；结果回写有独立收尾过程，实际结束可能超过 5 秒。
// 如需同步等待结果，使用 outbox.Send(ctx, taskID)；发送失败不能撤销已提交业务。
//
// 将补偿函数注册到已配置分布式锁的 cronx，频次由项目配置：
//
//	func retryOutbox(ctx context.Context) error {
//		return outbox.RetryPending(ctx, 100)
//	}
//
// batchSize 是查询页大小，每轮按 ID 游标扫描，每条任务最多尝试一次。
// 发送失败保留 pending 和 LastError；成功为 succeeded；发送前已过期则为 failed。
// ExpiresAt 零值表示不过期；已开始的发送按结果收尾，不会撤回。
// Send 与 RetryPending 均使用 FOR UPDATE NOWAIT；不同任务可并发发送。
// 同一任务被锁时，Send 返回 ErrBusy，RetryPending 跳过；cronx 分布式锁协调补偿轮次。
// 每条任务的事务从加锁持续到 Handler 返回并回写；Handler 应响应 context，发送期间占用数据库连接。
// 持锁读取后按应用服务器时间判断过期，次数和发送结果只更新一次。结果 UPDATE 使用独立的 3 秒 context；
// MySQL 提交/回滚还依赖连接读写超时，整个收尾过程不保证在 3 秒内返回。
// 数据库生成 int64 自增 ID，重复登记会生成新任务；重试沿用 ID，回写失败可能重发，消费者须幂等。
// 旧字符串主键须显式迁移并处理存量消息去重标识，不能直接 AutoMigrate 切换。
//
// Location 省略时为 UTC，两种数据库均保留绝对时刻，读取按配置时区表示。
// MySQL 时间列为 TIMESTAMP(6)，驱动须开启 parseTime，且驱动 Location 与会话 time_zone 一致；
// 推荐连接使用 UTC，展示时区可独立传入 env.TimeLocation()。PostgreSQL 使用 TIMESTAMPTZ(6)。
// Record 的时间字段使用 Timestamp，通过 record.CreatedAt.Time 获取原生 time.Time。
// MySQL 非空 ExpiresAt 的 UTC 整秒范围为 1 至 2147483647，越界返回 ErrInvalidArgument。
// 已有 MySQL DATETIME 表须先按原存储时区迁移为 TIMESTAMP，不能在未知时区下直接 AutoMigrate。
// 退出时先停止业务/cron，并等待同步发送及补偿调用返回，再关闭 MQ/数据库，最后关闭日志。
// SendAsync 不提供等待或排空，退出可能中断本次尝试，pending 任务由后续 cron 补偿；
// 必须在关闭底层资源前确认发送结束时，应使用 Send 并由应用等待同步调用完成。
package outbox
