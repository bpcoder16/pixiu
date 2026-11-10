package outbox

import (
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Key 是任务记录中的标识字符串，为迁移提供跨数据库的二进制排序语义。
type Key string

func (Key) GormDataType() string    { return "string" }
func (Status) GormDataType() string { return "string" }

func keyDBType(db *gorm.DB, field *schema.Field) string {
	if db.Dialector.Name() == "mysql" {
		return fmt.Sprintf("varchar(%d) CHARACTER SET ascii COLLATE ascii_bin", field.Size)
	}
	return fmt.Sprintf(`varchar(%d) COLLATE "C"`, field.Size)
}

func (Key) GormDBDataType(db *gorm.DB, field *schema.Field) string    { return keyDBType(db, field) }
func (Status) GormDBDataType(db *gorm.DB, field *schema.Field) string { return keyDBType(db, field) }

// Record 对应事务发件箱表 pixiu_outbox，显式交给 schemamigrate 迁移，同时作为 Get 返回的独立快照。
// 表注释在登记模型时通过 schemamigrate.Table.Comment 指定为“事务发件箱”。
// 状态修改须经 Store/Worker，发送互斥由数据库行锁保证。
// 时间默认值兼容关闭 explicit_defaults_for_timestamp 的严格模式，更新仍由 Store 写入。
type Record struct {
	ID         int64      `gorm:"primaryKey;autoIncrement;index:pixiu_outbox_ready_idx,priority:2;index:pixiu_outbox_finished_idx,priority:3;comment:数据库自增任务ID，重试保持不变"`
	Kind       Key        `gorm:"size:64;not null;comment:任务类型，与版本共同匹配处理器"`
	Version    int32      `gorm:"not null;comment:任务协议版本，正整数"`
	Payload    []byte     `gorm:"not null;comment:不可变任务内容，最大1MiB"`
	Status     Status     `gorm:"size:16;not null;index:pixiu_outbox_ready_idx,priority:1;index:pixiu_outbox_finished_idx,priority:1;comment:任务状态，pending待发送或重试、succeeded成功、failed已过期"`
	Attempts   int64      `gorm:"not null;comment:发送尝试次数，包含首次"`
	ExpiresAt  *Timestamp `gorm:"precision:6;default:null;comment:发送截止时间，空表示不过期"`
	LastError  string     `gorm:"type:text;not null;comment:最近一次失败原因，最大4KiB，成功后清空"`
	LogID      string     `gorm:"type:text;not null;comment:登记时的上下文logId"`
	CreatedAt  Timestamp  `gorm:"precision:6;not null;default:CURRENT_TIMESTAMP(6);autoCreateTime:false;comment:数据库生成的创建时间，按配置时区读取"`
	UpdatedAt  Timestamp  `gorm:"precision:6;not null;default:CURRENT_TIMESTAMP(6);autoUpdateTime:false;comment:数据库生成的更新时间，按配置时区读取"`
	FinishedAt *Timestamp `gorm:"precision:6;default:null;index:pixiu_outbox_finished_idx,priority:2;comment:成功或过期的完成时间"`
}

func (*Record) TableName() string { return "pixiu_outbox" }

type recordLocationKey struct{}

// AfterFind 将数据库返回的绝对时刻转换为 Store 配置的时区表示。
// 直接使用 GORM 查询且没有 Store 上下文时沿用 UTC，不改变共享连接。
func (r *Record) AfterFind(tx *gorm.DB) error {
	location, _ := tx.Statement.Context.Value(recordLocationKey{}).(*time.Location)
	if location == nil {
		location = time.UTC
	}
	for _, value := range []*Timestamp{&r.CreatedAt, &r.UpdatedAt, r.ExpiresAt, r.FinishedAt} {
		if value != nil {
			value.Time = value.In(location)
		}
	}
	return nil
}
