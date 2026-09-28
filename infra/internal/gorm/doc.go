// Package gorm 提供 infra 内部共用的 GORM 日志与连接池初始化能力。
// 数据库模块负责提供 GORM Logger 入口、下游名称和可选端点信息。
// 例如模块内部可以创建不带端点的日志核心：
//
//	core := gorm.New(gorm.Config{
//	    Message:        "SQLite",
//	    Name:           "local",
//	    DurationPrefix: "sqlite",
//	    SlowThreshold:  200 * time.Millisecond,
//	})
//	_, _ = core.ParamsFilter(ctx, "SELECT ?", 1)
//
// 上例需导入 time 和 github.com/bpcoder16/pixiu/infra/internal/gorm。
package gorm
