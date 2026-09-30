// Package mysqlx 提供基于 GORM 的 MySQL 连接池、显式主从选择和统一查询日志。
// 详细的配置、日志与关闭语义见 docs/mysqlx-design.md。
//
// 应用启动时创建客户端，查询时传入请求 context；以下示例需导入 context、
// github.com/bpcoder16/pixiu/infra/mysqlx 和 gorm.io/gorm：
//
//	client, err := mysqlx.New(ctx, mysqlx.Config{
//	    Name: "orders",
//	    SessionTimeZone: "+08:00",
//	    LogSQL: true, // 正常查询以 Info 输出 SQL；慢查询和错误始终输出 SQL
//	    InterpolateSQL: false, // 保留占位符；设为 true 时输出展开参数后的 SQL
//	    Master: mysqlx.Endpoint{
//	        Host: "db.example.com", Port: 3306,
//	        Database: "orders", Username: dbUser, Password: dbPassword,
//	    },
//	    Slaves: []mysqlx.Endpoint{{
//	        Host: "db-read.example.com", Port: 3306,
//	        Database: "orders", Username: dbUser, Password: dbPassword,
//	    }},
//	})
//	if err != nil { return err }
//	defer client.Close() // 应用退出时在 logit.Close 前调用
//	if autoMigrate {
//	    if err := client.MasterDB(ctx).AutoMigrate(&Order{}); err != nil { return err }
//	}
//	if err := client.MasterDB(ctx).Transaction(func(tx *gorm.DB) error {
//	    return tx.Create(&order).Error
//	}); err != nil { return err }
//	return client.SlaveDB(ctx).First(&order, id).Error
//
// 多个逻辑库可用 NewNamed 按 Config.Name 创建并登记；Named 按名称取得共享客户端。
// 使用 lifecycle 统一管理资源时，由应用先登记日志关闭函数，再登记 CloseAll；
// 名称不存在或 CloseAll 开始后调用 Named 会 panic，需在启动时确认名称并先停止任务。
// 以下示例还需导入 errors、github.com/bpcoder16/pixiu/lifecycle 和
// github.com/bpcoder16/pixiu/logit，logger 已由调用方创建：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error { return logit.Close(logger) }); err != nil {
//	    return err
//	}
//	if err := stack.Register(mysqlx.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err := mysqlx.NewNamed(ctx, cfg)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	client := mysqlx.Named(cfg.Name)
//	_ = client
//	// 停止使用命名客户端的任务后，统一关闭连接池，最后关闭日志。
//	return stack.Close()
//
// 业务仓库决定是否执行自动迁移，模型只传给主库。事务和要求读己之写的查询使用
// MasterDB；其他可接受从库延迟的读取可使用 SlaveDB。GORM 的 Info、Warn、Error
// 诊断消息按对应级别记录，不要求包含 SQL；LogSQL 只控制正常查询的 SQL 日志。
// ctx 已调用 logit.WithStart 时，每次 GORM Trace 还会以 MySQL 为前缀自动编号记录下游耗时，
// 供业务调用 logit.InfoDuration 汇总；未调用 WithStart 时跳过。
// New 的 Ping 遵循 ctx，GORM 的版本探测使用 Background，不受 ctx 截止时间约束。
//
// 单个逻辑下游可显式初始化默认客户端，省去每次按名称查询。cfg 已由应用构造：
//
//	if _, err := mysqlx.NewDefault(ctx, cfg); err != nil {
//	    return err
//	}
//	return mysqlx.Default().MasterDB(ctx).Create(&order).Error
//
// 启动阶段由调用方串行调用 NewNamed 和 NewDefault，不并发初始化。
// Name 仍必填；Default() 与 Named(cfg.Name) 返回同一实例，New 和 NewNamed
// 不自动设置默认实例。重复默认初始化报错；失败可重试。未初始化或 CloseAll
// 开始后调用 Default 会 panic。应用停止查询后调用 CloseAll，默认与命名实例
// 统一关闭一次，日志最后关闭；默认实例不要单独登记 Close。
package mysqlx
