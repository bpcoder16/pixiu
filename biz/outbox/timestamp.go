package outbox

import (
	"database/sql/driver"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// Timestamp 嵌入标准时间，为迁移提供 MySQL TIMESTAMP 与 PostgreSQL TIMESTAMPTZ 映射。
// 时间方法和 JSON 表示沿用 time.Time，原生值通过 Time 字段取得。
// 数据库参数传入 *Timestamp 或原生 Time；空指针表示 SQL NULL。
type Timestamp struct {
	time.Time
}

func (*Timestamp) GormDataType() string { return "time" }

func (*Timestamp) GormDBDataType(db *gorm.DB, field *schema.Field) string {
	switch db.Dialector.Name() {
	case "mysql":
		columnType := fmt.Sprintf("timestamp(%d)", field.Precision)
		if !field.NotNull {
			// 旧式 TIMESTAMP 默认 NOT NULL，必须显式声明 NULL 才能保留空值语义。
			columnType += " NULL"
		}
		return columnType
	case "postgres":
		return fmt.Sprintf("timestamptz(%d)", field.Precision)
	default:
		return ""
	}
}

// Scan 只接收驱动已解析的时间，避免猜测无时区字符串的实际含义。
func (t *Timestamp) Scan(value any) error {
	switch typedValue := value.(type) {
	case time.Time:
		t.Time = typedValue
	case nil:
		t.Time = time.Time{}
	default:
		return fmt.Errorf("outbox: timestamp requires parsed time.Time, got %T", typedValue)
	}
	return nil
}

// Value 将原生时间交给驱动，保留实际时刻及连接的正常时区转换。
func (t *Timestamp) Value() (driver.Value, error) {
	if t == nil {
		return nil, nil
	}
	return t.Time, nil
}
