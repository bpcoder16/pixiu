// Package natsoutbox 将 JetStream 发布接入 outbox，发送成功以有效 PubAck 为准。
// 构造时通过 natsx.Named 借用已登记客户端，自增任务 ID 的十进制字符串作为 Nats-Msg-Id，重复确认也算成功。
//
// 创建处理器与默认执行器（已调用 outbox.NewDefaultStore，events 已通过 bootstrap 或 natsx.NewNamed/NewDefault 登记）：
//
//	handler := natsoutbox.NewJetStreamHandler("events")
//	_, err := outbox.NewDefaultWorker(map[outbox.HandlerKey]outbox.Handler{
//		{
//			Kind:    natsoutbox.JetStreamKind,
//			Version: natsoutbox.JetStreamVersion,
//		}: handler,
//	})
//	if err != nil {
//		return err
//	}
//
// 业务事务内登记，最外层提交后可主动发送一次：
//
//	var taskID int64
//	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
//		if err := tx.Create(&order).Error; err != nil {
//			return err
//		}
//		var err error
//		taskID, err = natsoutbox.EnqueueJetStreamTx(tx, natsoutbox.JetStreamMessage{
//			Client:    "events",
//			Subject:   "orders.created",
//			Data:      payload,
//			ExpiresAt: time.Now().Add(24 * time.Hour),
//		})
//		return err
//	})
//	if err != nil {
//		return err
//	}
//	if err := outbox.Send(ctx, taskID); err != nil {
//		logit.Warn(ctx, "OutboxDeferred", logit.Str("err", err.Error()))
//	}
//	return nil
//
// 示例需导入 outbox、natsoutbox、logit、gorm.io/gorm、time，
// db、ctx、order、payload 由应用提供。项目通过配置分布式锁的 cronx 周期调用
// outbox.RetryPending(ctx, 100)，失败留到下一轮；ExpiresAt 零值表示不过期。
// EnqueueJetStreamTx(tx, msg) 使用默认 Store，未登记时 panic；登记不依赖 Worker 或 NATS 客户端初始化。
// 显式实例使用 EnqueueJetStreamTxWithStore(store, tx, msg)，不读取默认实例；旧三参数调用须改用此名称。
// Data/Header 在登记时生成快照；Subject 传原始主题，natsx 自动添加 namespace。
// Header 名由 NATS SDK 在发送时校验，值按 SDK 规则规范化。
// 重复登记会生成不同 ID；同一任务重试沿用 ID。不同数据库的 ID 可重复，须隔离到不同 Stream。
// 构造函数支持多个名称；列表为空、名称非法、未登记或注册表关闭时直接 panic。
// Handler 持有构造时取得的客户端，发送时仅按任务的 Client 在该名称列表内路由。
// 客户端名称、集群和 namespace 在存在积压时须保持稳定。PubAck 不代表消费者业务处理完成；
// 确认丢失或状态回写失败可能重发，消费端须幂等。
package natsoutbox
