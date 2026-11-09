//go:build !pixiu_bootstrap_no_mysql

package bootstrap

import (
	"context"

	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"gorm.io/gorm"
)

// RunMySQLMigration 从配置目录严格读取 migrate.<name>.yaml，在命名 MySQL 的主库执行迁移策略。
// enabled 默认 false；关闭或空清单不获取客户端。timeout 默认 5 分钟，负数非法。
// 调用方在 MustBaseInit 后、业务服务启动前显式调用，错误和部分执行报告交回项目。
// 本入口内部创建启动期 Context，并通过配置限制整体迁移时间。
func RunMySQLMigration(name string, tables []schemamigrate.Table) (schemamigrate.Report, error) {
	return RunMySQLInitialization(name, tables, SeedOptions{})
}

// RunMySQLInitialization 先执行表结构迁移，再在同一主库校准种子数据。
// 沿用 migrate.<name>.yaml 的 enabled 同时控制两阶段；关闭时不获取数据库或读取 SQL。
// 配置 timeout 只限制结构迁移；种子使用 SeedOptions.Timeout，从种子阶段开始独立计时。
// tables 为空时仍可对已有表执行种子；任一阶段失败均立即返回，不应继续启动业务。
// 返回的 Report 仅记录表结构迁移结果；种子错误保留文件、行号（适用时）及底层错误链。
func RunMySQLInitialization(name string, tables []schemamigrate.Table, seeds SeedOptions) (schemamigrate.Report, error) {
	return runSQLInitialization("MySQL", name, tables, seeds, func(ctx context.Context) *gorm.DB {
		return mysqlx.Named(name).MasterDB(ctx)
	})
}
