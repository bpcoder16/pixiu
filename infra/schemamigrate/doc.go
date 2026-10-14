// Package schemamigrate 封装 GORM AutoMigrate，集中提供项目可复用的表结构迁移入口。
// 项目维护 Model 清单，在基础设施初始化后、业务服务启动前调用；不手工维护迁移版本。
//
// 以下示例需导入 context、gorm.io/gorm 和 github.com/bpcoder16/pixiu/infra/schemamigrate：
//
//	func migrate(ctx context.Context, masterDB func(context.Context) *gorm.DB) error {
//		type Card struct {
//			ID uint64 `gorm:"primaryKey"`
//			Name string `gorm:"size:100;index:idx_card_name"`
//		}
//		_, err := schemamigrate.Apply(ctx, masterDB, []schemamigrate.Table{{
//			Model: &Card{},
//			Name: "cards",
//			Comment: "卡片",
//		}})
//		return err
//	}
//
// 每次调用都会逐个执行 AutoMigrate，由 GORM 检查实际结构并处理列、索引和约束。
// 不计算 Model 哈希，不创建/读取迁移状态表，不提供迁移锁、Check/Verify 或自定义方言协议。
// 保留统一的 Table、Apply 和 bootstrap 接入，后续优化集中在 Pixiu，不要求项目直接调用 GORM。
//
// Name 为空时沿用 GORM 命名；显式 Name 只接受字母、数字和下划线，用于物理分表。
// MySQL 建表使用 InnoDB；可指定 Charset/Collation，仅用于建表，不转换已有表。
// 非空 Comment 参照 decard01 单独维护 MySQL 表注释，仅变化时执行 ALTER TABLE。
// 空 Comment 表示不管理注释，不会清空已有注释。其他方言设置这些 MySQL 选项会报错。
// 不带上述选项时直接使用连接现有的 GORM 方言；本包用 SQLite 验证通用流程、MySQL 验证专属行为。
// 关联 Model 的自动迁移遵循 GORM 配置；不要把不归项目管理的表注册为 Model 或迁移关联。
//
// 每次传入非 nil Context，主库提供函数返回无历史查询条件的会话；不接受外部事务或 DryRun。
// 空清单不获取数据库。本包不创建/关闭连接池，不写种子数据或执行业务回填。
// 错误保留底层错误链，Report 包含已处理表及首个失败表；applied 仅表示 AutoMigrate 成功，
// 不区分创建、更新或无变化。调用方处理失败，不应忽略错误后开放业务入口。
//
// 第一版接受每次启动检查所有登记表的成本。没有跨实例迁移锁，发布时避免多个实例同时迁移。
// MySQL DDL 可能部分生效、持锁或超时，不能保证整批回滚；复杂数据修复留在项目显式执行。
// 旧试验版的 pixiu_schema_states 不再参与执行，组件不会自动删除已有表或记录。
package schemamigrate
