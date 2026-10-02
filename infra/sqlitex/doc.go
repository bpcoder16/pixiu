// Package sqlitex 提供基于 GORM 的 SQLite 单库连接池和统一查询日志。
// 配置、日志及关闭语义见 docs/sqlitex-design.md。
//
// 应用启动时创建客户端，查询时传入请求 context；以下示例需导入
// github.com/bpcoder16/pixiu/infra/sqlitex 和 time：
//
//	foreignKeys := true
//	client, err := sqlitex.New(ctx, sqlitex.Config{
//	    Name:         "local",
//	    DSN:          "file:/var/lib/app/local.db",
//	    JournalMode:  sqlitex.JournalModeWAL,
//	    Synchronous:  sqlitex.SynchronousFull,
//	    BusyTimeout:  5 * time.Second,
//	    ForeignKeys:  &foreignKeys,
//	    LogSQL:       true,
//	})
//	if err != nil {
//	    return err
//	}
//	defer client.Close() // 应用退出时在 logit.Close 前调用
//	if autoMigrate {
//	    if err := client.DB(ctx).AutoMigrate(&LocalRecord{}); err != nil {
//	        return err
//	    }
//	}
//	return client.DB(ctx).Create(&record).Error
//
// 多个逻辑库可用 NewNamed 按 Config.Name 登记，再用 Named 取得共享客户端。
// 使用 lifecycle 统一关闭时，应先登记日志关闭函数，再登记 CloseAll，使日志
// 最后关闭。以下示例还需导入 errors、github.com/bpcoder16/pixiu/lifecycle
// 和 github.com/bpcoder16/pixiu/logit；logger 已由调用方创建：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error {
//	    return logit.Close(logger)
//	}); err != nil {
//	    return err
//	}
//	if err := stack.Register(sqlitex.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err := sqlitex.NewNamed(ctx, cfg)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	client := sqlitex.Named(cfg.Name)
//	_ = client
//	// 停止使用命名客户端的任务后统一关闭。
//	return stack.Close()
//
// 业务仓库负责创建目录、决定迁移时机及配置 SQLite 参数。常用的日志模式、
// 锁等待时间、同步级别和外键可用 Config 字段设置，其他驱动参数仍可写入 DSN。
// 默认连接池只有
// 一条连接，事务回调内应继续使用传入的 tx 进行查询。LogSQL 控制正常
// 查询的 Info 日志；错误和慢查询始终记录 SQL。InterpolateSQL 默认关闭，
// 需要输出展开参数后的 SQL 时可显式开启。已调用 logit.WithStart 的请求会
// 以 SQLite_<Name> 为前缀自动编号记录下游耗时，供 logit.InfoDuration 汇总。命名客户端由
// CloseAll 关闭；独立 New 创建的客户端仍由调用方单独关闭。
//
// 单个逻辑下游可显式初始化默认客户端，省去每次按名称查询。cfg 已由应用构造：
//
//	if _, err := sqlitex.NewDefault(ctx, cfg); err != nil {
//	    return err
//	}
//	return sqlitex.Default().DB(ctx).Create(&record).Error
//
// 启动阶段由调用方串行调用 NewNamed 和 NewDefault，不并发初始化。
// Name 仍必填；Default() 与 Named(cfg.Name) 返回同一实例，New 和 NewNamed
// 不自动设置默认实例。重复默认初始化报错；失败可重试。未初始化或 CloseAll
// 开始后调用 Default 会 panic。应用停止查询后调用 CloseAll，默认与命名实例
// 统一关闭一次，日志最后关闭；默认实例不要单独登记 Close。
package sqlitex
