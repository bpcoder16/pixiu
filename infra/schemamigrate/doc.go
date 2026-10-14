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
// 每次调用只按清单顺序对显式登记的表执行 AutoMigrate，由 GORM 处理列、索引及唯一/检查约束。
// 迁移会话统一忽略模型关联及其外键，不自动创建或更新关联表、多对多中间表，也不删除已有外键。
// 需要迁移的关联表和中间表须单独登记；关联外键由项目显式管理。
// Model 可以保留关联字段，关联 ID 等普通列照常迁移；本包不修改调用方的 GORM 配置。
// 不计算 Model 哈希，不创建/读取迁移状态表，不提供迁移锁、Check/Verify 或自定义方言协议。
// 保留统一的 Table、Apply 和 bootstrap 接入，后续优化集中在 Pixiu，不要求项目直接调用 GORM。
//
// Name 为空时沿用 GORM 命名；显式 Name 只接受字母、数字和下划线，用于物理分表。
// 登记表的最终表名均不得包含点号，例如 testdb.cards；即使前缀是当前库名也会在 DDL 前拒绝。
// 此限制覆盖显式 Name、Model 的 TableName 和 NamingStrategy；未登记的关联不作为迁移目标校验。
// 不支持库名或 schema 限定名，目标数据库由传入的连接选定，本包不解析库名或切库。
// MySQL 建表使用 InnoDB；可指定 Charset/Collation，仅用于建表，不转换已有表。
// 每张登记表独立使用自己的建表选项，不展开关联，也不检查或调整关联依赖顺序。
// 非空 Comment 单独维护 MySQL 表注释，仅变化时执行 ALTER TABLE。
// 空 Comment 表示不管理注释，不会清空已有注释。其他方言设置这些 MySQL 选项会报错。
// 不带上述选项时直接使用连接现有的 GORM 方言；本包用 SQLite 验证通用流程、MySQL 验证专属行为。
// 不要把不归项目管理的表登记到迁移清单。
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
