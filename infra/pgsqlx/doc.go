// Package pgsqlx 提供基于 GORM 的 PostgreSQL 连接池、显式主从选择和统一查询日志。
// 详细的配置、日志与关闭语义见 docs/pgsqlx-design.md。
//
// 应用启动时创建客户端，查询时传入请求 context；以下示例需导入 context、
// github.com/bpcoder16/pixiu/infra/pgsqlx 和 gorm.io/gorm：
//
//	client, err := pgsqlx.New(ctx, pgsqlx.Config{
//	    Name: "orders",
//	    SessionTimeZone: "UTC",
//	    LogSQL: true,
//	    Master: pgsqlx.Endpoint{
//	        Host: "db.example.com",
//	        Database: "orders",
//	        Username: dbUser,
//	        Password: dbPassword,
//	        Charset: "UTF8",
//	        Location: "UTC",
//	        SSLMode: "verify-full",
//	    },
//	})
//	if err != nil {
//	    return err
//	}
//	defer client.Close() // 应用退出时在 logit.Close 前调用
//	if autoMigrate {
//	    if err := client.MasterDB(ctx).AutoMigrate(&Order{}); err != nil {
//	        return err
//	    }
//	}
//	if err := client.MasterDB(ctx).Transaction(func(tx *gorm.DB) error {
//	    return tx.Create(&order).Error
//	}); err != nil {
//	    return err
//	}
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
//	if err := stack.Register(pgsqlx.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err := pgsqlx.NewNamed(ctx, cfg)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	client := pgsqlx.Named(cfg.Name)
//	_ = client
//	// 停止使用命名客户端的任务后，统一关闭连接池，最后关闭日志。
//	return stack.Close()
//
// 业务仓库决定是否执行自动迁移，模型只传给主库。事务和要求读己之写的查询使用
// MasterDB；其他可接受从库延迟的读取可使用 SlaveDB。GORM 的 Info、Warn、Error
// 诊断消息按对应级别记录；LogSQL 只控制正常查询的 SQL 日志。
// Charset 固定为 UTF8；Location 默认 Asia/Shanghai，只解释无时区 timestamp，不改变 timestamptz。
// ctx 已调用 logit.WithStart 时，每次 GORM Trace 以 PostgreSQL 为前缀自动编号记录下游耗时，
// 供业务调用 logit.InfoDuration 汇总；直接调用底层 *sql.DB 不经过 Trace。
//
// 单个逻辑下游可显式初始化默认客户端，省去每次按名称查询。cfg 已由应用构造：
//
//	if _, err := pgsqlx.NewDefault(ctx, cfg); err != nil {
//	    return err
//	}
//	return pgsqlx.Default().MasterDB(ctx).Create(&order).Error
//
// 启动阶段由调用方串行调用 NewNamed 和 NewDefault，不并发初始化。
// Name 仍必填；Default() 与 Named(cfg.Name) 返回同一实例，New 和 NewNamed
// 不自动设置默认实例。重复默认初始化报错；失败可重试。未初始化或 CloseAll
// 开始后调用 Default 会 panic。应用停止查询后调用 CloseAll，默认与命名实例
// 统一关闭一次，日志最后关闭；默认实例不要单独登记 Close。
package pgsqlx
