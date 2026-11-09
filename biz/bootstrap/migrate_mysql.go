//go:build !pixiu_bootstrap_no_mysql

package bootstrap

import (
	"context"

	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"gorm.io/gorm"
)

// RunMySQLInitialization 严格读取 migrate.<name>.yaml，在命名 MySQL 的主库先迁移表结构，再校准种子。
// enabled 默认 false，同时控制两阶段；关闭时不获取数据库或读取 SQL，两份清单均为空时也不取数据库。
// 配置 timeout 只限制结构迁移，默认 5 分钟，负数非法；种子使用 SeedOptions.Timeout 独立计时。
// 仅迁移结构时传入 SeedOptions{}；tables 为空时仍可对已有表执行种子。
// 在 MustBaseInit 后、业务启动前显式调用，内部创建启动期 Context；任一阶段失败均应停止启动。
// 返回的 Report 仅记录表结构迁移结果；种子错误保留文件、行号（适用时）及底层错误链。
func RunMySQLInitialization(name string, tables []schemamigrate.Table, seeds SeedOptions) (schemamigrate.Report, error) {
	return runSQLInitialization("MySQL", name, tables, seeds, func(ctx context.Context) *gorm.DB {
		return mysqlx.Named(name).MasterDB(ctx)
	})
}
