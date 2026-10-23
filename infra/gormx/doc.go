// Package gormx 为 Repository 提供统一的主、从库会话依赖 Sessions。
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
package gormx
