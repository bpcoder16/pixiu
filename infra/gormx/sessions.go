package gormx

import (
	"context"

	"gorm.io/gorm"
)

// Sessions 保存主、从库会话获取函数，不持有请求会话或管理连接池。
// 必须通过 NewSessions 构造；获取函数支持并发时，Sessions 可供多个 Repository 共用。
type Sessions struct {
	masterDB func(context.Context) *gorm.DB
	slaveDB  func(context.Context) *gorm.DB
}

// NewSessions 只保存依赖，不访问数据库；任一获取函数为 nil 时 panic。
// 获取函数须并发安全，每次返回绑定当前 Context、没有历史查询条件的可用会话。
func NewSessions(masterDB, slaveDB func(context.Context) *gorm.DB) *Sessions {
	if masterDB == nil || slaveDB == nil {
		panic("gormx: master and slave session providers are required")
	}
	return &Sessions{
		masterDB: masterDB,
		slaveDB:  slaveDB,
	}
}

// MasterDB 获取本次操作的主库会话；ctx 为 nil 时 panic。
func (s *Sessions) MasterDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("gormx: nil context")
	}
	return s.masterDB(ctx)
}

// SlaveDB 获取本次操作的从库会话；ctx 为 nil 时 panic。
// 从库选择和未配置从库时的回退由调用方提供的获取函数负责。
func (s *Sessions) SlaveDB(ctx context.Context) *gorm.DB {
	if ctx == nil {
		panic("gormx: nil context")
	}
	return s.slaveDB(ctx)
}
