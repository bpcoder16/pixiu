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
// 业务仓库决定是否执行自动迁移，模型只传给主库。事务和要求读己之写的查询使用
// MasterDB；其他可接受从库延迟的读取可使用 SlaveDB。GORM 的 Info、Warn、Error
// 诊断消息按对应级别记录，不要求包含 SQL；LogSQL 只控制正常查询的 SQL 日志。
// ctx 已调用 logit.WithStart 时，每次 GORM Trace 还会自动编号记录 mysql 下游耗时，
// 供业务调用 logit.InfoDuration 汇总；未调用 WithStart 时跳过。
// New 的 Ping 遵循 ctx，GORM 的版本探测使用 Background，不受 ctx 截止时间约束。
package mysqlx
