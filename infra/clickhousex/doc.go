// Package clickhousex 提供基于 GORM 的 ClickHouse 连接池、显式主从选择和统一查询日志。
// 配置、日志及 ClickHouse 写入语义见 docs/clickhousex-design.md。
//
// 应用启动时创建客户端，查询时传入请求 context；以下示例需导入
// github.com/ClickHouse/clickhouse-go/v2 和
// github.com/bpcoder16/pixiu/infra/clickhousex：
//
//	client, err := clickhousex.New(ctx, clickhousex.Config{
//	    Name: "analytics",
//	    LogSQL: true,
//	    Master: clickhousex.Endpoint{
//	        Host: "clickhouse.example.com",
//	        Port: 9000,
//	        Database: "analytics",
//	        Username: dbUser,
//	        Password: dbPassword,
//	        Settings: clickhouse.Settings{
//	            "session_timezone": "UTC",
//	        },
//	    },
//	})
//	if err != nil {
//	    return err
//	}
//	defer client.Close() // 应用退出时在 logit.Close 前调用
//	return client.SlaveDB(ctx).Where("user_id = ?", id).Find(&events).Error
//
// session_timezone 控制服务端查询使用的会话时区；多个端点需分别配置。
// 时间列需要固定时区时，应在表结构中声明 DateTime('UTC') 或 DateTime64(3, 'UTC')。
// 会话设置不保证 Go 返回值的 time.Time.Location 为 UTC。
// ClickHouse 没有 MySQL 式的连接字符集配置；文本建议使用 UTF-8。
// Go 驱动默认使用服务端或列声明的时区解码时间值。
//
// 多个逻辑库可用 NewNamed 按 Config.Name 创建并登记；Named 按名称取得共享客户端。
// 使用 lifecycle 统一管理资源时，由应用先登记日志关闭函数，再登记 CloseAll；
// 名称不存在或 CloseAll 开始后调用 Named 会 panic，需在启动时确认名称并先停止任务。
// 以下示例还需导入 errors、github.com/bpcoder16/pixiu/lifecycle 和
// github.com/bpcoder16/pixiu/logit，logger 与 cfg 已由调用方创建：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error {
//	    return logit.Close(logger)
//	}); err != nil {
//	    return err
//	}
//	if err := stack.Register(clickhousex.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	if _, err := clickhousex.NewNamed(ctx, cfg); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	client := clickhousex.Named(cfg.Name)
//	_ = client
//	// 停止使用命名客户端的任务后，统一关闭连接池，最后关闭日志。
//	return stack.Close()
//
// 从库可接受延迟的查询使用 SlaveDB；写入及要求最新数据的查询使用 MasterDB。
// GORM 的 Info、Warn、Error 诊断消息按对应级别记录；慢查询和错误总会记录 SQL，
// InterpolateSQL 默认关闭，以保留占位符。ctx 已调用 logit.WithStart 时，每次
// GORM Trace 以 ClickHouse 为前缀自动编号登记下游耗时，供 logit.InfoDuration 汇总。
package clickhousex
