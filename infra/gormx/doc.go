// Package gormx 为 Repository 提供统一的主、从库会话依赖 Sessions 和通用查询辅助方法。
//
// 应用传入已有的会话获取函数；以下示例需导入标准库 context、
// github.com/bpcoder16/pixiu/infra/gormx 和 gorm.io/gorm：
//
//	func countUsers(ctx context.Context, masterDB, slaveDB func(context.Context) *gorm.DB) (int64, error) {
//	    sessions := gormx.NewSessions(masterDB, slaveDB)
//	    var count int64
//	    err := sessions.SlaveDB(ctx).Table("users").Count(&count).Error
//	    return count, err
//	}
//
// Sessions 通常在组装 Repository 时创建并长期共用，构造不调用获取函数或连接数据库。
// 获取函数必须非 nil、并发安全，每次返回绑定当前 Context、没有历史查询条件的可用会话。
// NewSessions 对 nil 获取函数 panic，MasterDB/SlaveDB 对 nil Context panic；零值 Sessions 不可用。
// 每次操作都会调用对应获取函数，不缓存请求 Context 或 GORM 查询会话。
// 单库可给两个参数传入相同函数；从库选择及回退由具体客户端负责。
// 写入和对一致性敏感的读取使用 MasterDB，可接受复制延迟的读取使用 SlaveDB。
// 默认逻辑库由业务项目决定；本包不提供全局默认值、连接池生命周期、CRUD 或隐式事务传播。
//
// # 按原文包含匹配
//
// WhereLikeContains(tx, queryKey, keyword) 为已有 GORM 查询追加 LIKE 包含条件，不执行查询。
// queryKey 必须是代码指定的数据库列名，通过标识符引用，不能传入 SQL 表达式。
// keyword 使用参数绑定，内部将 !、%、_ 转义后添加两侧的 %，并显式使用 ESCAPE '!'。
// 空关键词原样返回 tx；非空关键词不清理空白，原有查询条件及 Context 继续保留。
// 以下示例中的 db 为已有 GORM 会话，records 为调用方的查询结果切片：
//
//	query := db.Table("records").Where("owner_id = ?", ownerID)
//	err := gormx.WhereLikeContains(query, "title", "100%_").Find(&records).Error
//
// 该查询匹配标题中包含字面文本 100%_ 的记录，% 和 _ 不作为调用方提供的通配符。
package gormx
