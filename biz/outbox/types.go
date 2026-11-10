package outbox

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Backend 指定业务事务和补偿所用的数据库方言。
type Backend string

const (
	MySQL      Backend = "mysql"
	PostgreSQL Backend = "postgresql"
)

// Status 是持久化任务状态。
type Status string

const (
	Pending   Status = "pending"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
)

var (
	ErrInvalidArgument = errors.New("outbox: invalid argument")
	ErrNotTransaction  = errors.New("outbox: transaction required")
	ErrBackendMismatch = errors.New("outbox: backend mismatch")
	ErrNotFound        = errors.New("outbox: task not found")
	ErrBusy            = errors.New("outbox: sender busy")
	ErrExpired         = errors.New("outbox: task expired")
)

// StoreConfig 借用已有主库，不管理连接生命周期。
type StoreConfig struct {
	Backend  Backend
	MasterDB func(context.Context) *gorm.DB
	// Location 为 nil 时使用 UTC，控制查询结果与 Delivery 的时间表示。
	// MySQL 驱动解析时区须与数据库会话时区一致，本配置不修改连接。
	Location *time.Location
}

// Task 保存不可变发送意图，Payload 不超过 1 MiB；登记时由数据库生成 ID。
// ExpiresAt 为绝对截止时间，零值表示不过期。
// MySQL 非空 ExpiresAt 的 UTC 整秒必须在 1 至 2147483647 范围内。
type Task struct {
	Kind      string
	Version   int
	Payload   []byte
	ExpiresAt time.Time
}

type HandlerKey struct {
	Kind    string
	Version int
}

// Delivery 是一次发送的消息快照，Attempt 包含本次尝试。
type Delivery struct {
	Task
	ID      int64
	Attempt int64
}

// Handler 同步完成一次发送；返回 nil 才标记成功，失败留给下轮 cron。
// 调用方 context 控制执行期限，Handler 应及时响应取消。
type Handler func(context.Context, Delivery) error

func validKey(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit {
		return false
	}
	for i := range len(value) {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' {
			continue
		}
		if i == 0 || c != '.' && c != '_' && c != ':' && c != '-' {
			return false
		}
	}
	return true
}
