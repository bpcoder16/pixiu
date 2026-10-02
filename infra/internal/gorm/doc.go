// Package gorm 提供 infra 内部共用的 GORM 日志、连接池初始化和主从路由能力。
// 数据库模块负责提供 GORM Logger 入口、下游名称、端点信息及方言连接。
// 例如模块内部可以创建不带端点的日志核心：
//
//	const downstreamSQLiteMessage = "SQLite"
//	name := "local"
//	core := gorm.New(gorm.Config{
//	    Message:        downstreamSQLiteMessage,
//	    Name:           name,
//	    DurationPrefix: downstreamSQLiteMessage + "_" + name,
//	    SlowThreshold:  200 * time.Millisecond,
//	})
//	_, _ = core.ParamsFilter(ctx, "SELECT ?", 1)
//
// 模块通过 BuildCluster 创建主从连接；应用停止查询后关闭：
//
//	var cluster gorm.Cluster
//	if err := gorm.BuildCluster(ctx, &cluster, master, slaves, open); err != nil {
//	    return err
//	}
//	defer cluster.Close()
//	readDB := cluster.Slave(ctx)
//	_ = readDB
//
// Open 接收可选的 GORM 配置函数；模块可设置方言所需选项，同时沿用共享的
// Logger、禁用自动 Ping 和初始化失败时关闭连接池的约定。
//
// 上例需导入 time 和 github.com/bpcoder16/pixiu/infra/internal/gorm。
package gorm
