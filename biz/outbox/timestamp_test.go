package outbox

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/schemamigrate"
	"gorm.io/gorm"
)

func TestTimestampScanValueAndJSON(t *testing.T) {
	want := time.Date(2026, 11, 10, 18, 30, 0, 123456000, time.FixedZone("+08", 8*3600))
	var value Timestamp
	if err := value.Scan(want); err != nil || !value.Equal(want) {
		t.Fatalf("读取时刻变化: %v %v", value, err)
	}
	stored, err := value.Value()
	if err != nil || !stored.(time.Time).Equal(want) {
		t.Fatalf("写入时刻变化: %v %v", stored, err)
	}
	parameter, err := driver.DefaultParameterConverter.ConvertValue(&value)
	if err != nil || !parameter.(time.Time).Equal(want) {
		t.Fatalf("数据库参数转换改变时刻: %v %v", parameter, err)
	}
	var absent *Timestamp
	parameter, err = driver.DefaultParameterConverter.ConvertValue(absent)
	if err != nil || parameter != nil {
		t.Fatalf("空时间指针必须转换为 SQL NULL: %v %v", parameter, err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	wantJSON, _ := json.Marshal(want)
	if string(encoded) != string(wantJSON) {
		t.Fatalf("时间 JSON 协议变化: %s, want %s", encoded, wantJSON)
	}
	var decoded Timestamp
	if err := json.Unmarshal(encoded, &decoded); err != nil || !decoded.Equal(want) {
		t.Fatalf("JSON 往返错误: %v %v", decoded, err)
	}
	if err := value.Scan(nil); err != nil || !value.IsZero() {
		t.Fatalf("NULL 扫描未清空旧值: %v %v", value, err)
	}
	if err := value.Scan("2026-11-10"); err == nil {
		t.Fatal("未解析的时间字符串不得被猜测时区")
	}
}

// MySQL 在 UTC 连接下保留夏令时回拨两侧不同的时间点；Location 只控制返回表示。
func TestMySQLTimestampDSTAndConnections(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	for _, connection := range []*time.Location{time.UTC, shanghai} {
		t.Run(connection.String(), func(t *testing.T) {
			base, db := liveStore(t, MySQL, connection)
			cfg := base.cfg
			cfg.Location = newYork
			store, err := NewStore(cfg)
			if err != nil {
				t.Fatal(err)
			}
			instants := []time.Time{
				time.Date(2030, 11, 3, 5, 30, 0, 123456000, time.UTC),
				time.Date(2030, 11, 3, 6, 30, 0, 123456000, time.UTC),
			}
			for _, instant := range instants {
				value := task()
				value.ExpiresAt = instant.In(newYork)
				id := enqueueTask(t, store, db, value)
				record, err := store.Get(context.Background(), id)
				if err != nil || record.ExpiresAt == nil || !record.ExpiresAt.Equal(instant) || record.ExpiresAt.Location() != newYork {
					t.Fatalf("回拨时刻往返失败: %+v %v", record, err)
				}
				if record.ExpiresAt.Format("15:04") != "01:30" {
					t.Fatalf("预期回拨两侧相同墙上时间: %v", record.ExpiresAt)
				}
				if time.Since(record.CreatedAt.Time).Abs() > 10*time.Second {
					t.Fatalf("数据库时钟发生偏移: %v", record.CreatedAt)
				}
			}
		})
	}
}

func TestTimestampExpirationRange(t *testing.T) {
	eachDatabase(t, func(t *testing.T, store *Store, db *gorm.DB) {
		for _, tc := range []struct {
			id    string
			value time.Time
			valid bool
		}{
			{id: "lower", value: time.Unix(1, 123456000), valid: true},
			{id: "upper", value: time.Unix(2147483647, 123456000), valid: true},
			{id: "before", value: time.Unix(0, 0)},
			{id: "after", value: time.Unix(2147483648, 0)},
		} {
			t.Run(tc.id, func(t *testing.T) {
				value := task()
				value.ExpiresAt = tc.value.In(time.FixedZone("+08", 8*3600))
				var id int64
				err := db.Transaction(func(tx *gorm.DB) error {
					var err error
					id, err = store.EnqueueTx(tx, value)
					return err
				})
				if store.cfg.Backend == MySQL && !tc.valid {
					if !errors.Is(err, ErrInvalidArgument) {
						t.Fatalf("MySQL 越界时间必须明确拒绝: %v", err)
					}
					if id != 0 {
						t.Fatalf("越界任务不得返回 ID: %d", id)
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				record, err := store.Get(context.Background(), id)
				if err != nil || record.ExpiresAt == nil || !record.ExpiresAt.Equal(tc.value) {
					t.Fatalf("边界时刻未保留: %+v %v", record, err)
				}
			})
		}
	})
}

func TestMySQLDatetimeMigrationPreservesInstants(t *testing.T) {
	for _, oldZone := range []string{"+00:00", "+08:00"} {
		t.Run(oldZone, func(t *testing.T) {
			store, db := liveStore(t, MySQL)
			instant := time.Date(2026, 11, 10, 10, 30, 0, 123456000, time.UTC)
			location := time.UTC
			if oldZone == "+08:00" {
				location = time.FixedZone("+08", 8*3600)
			}
			err := db.Connection(func(conn *gorm.DB) error {
				// 在空表中还原旧版 DATETIME，再按旧存储时区迁移；只操作本次隔离库。
				if err := conn.Exec(`ALTER TABLE pixiu_outbox
MODIFY expires_at DATETIME(6) NULL,
MODIFY created_at DATETIME(6) NOT NULL,
MODIFY updated_at DATETIME(6) NOT NULL,
MODIFY finished_at DATETIME(6) NULL`).Error; err != nil {
					return err
				}
				wallTime := instant.In(location).Format("2006-01-02 15:04:05.000000")
				for _, id := range []int64{1, 2} {
					var optional any
					if id == 1 {
						optional = wallTime
					}
					if err := conn.Exec(`INSERT INTO pixiu_outbox
(id,kind,version,payload,status,attempts,last_error,log_id,created_at,updated_at,expires_at,finished_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)`, id, "test.publish", 1, []byte("{}"), Pending, 0, "", "", wallTime, wallTime, optional, optional).Error; err != nil {
						return err
					}
				}
				var originalZone string
				if err := conn.Raw("SELECT @@session.time_zone").Scan(&originalZone).Error; err != nil {
					return err
				}
				defer func() {
					if err := conn.Exec("SET SESSION time_zone = ?", originalZone).Error; err != nil {
						t.Error(err)
					}
				}()
				if err := conn.Exec("SET SESSION time_zone = ?", oldZone).Error; err != nil {
					return err
				}
				for range 2 {
					if _, err := schemamigrate.Apply(context.Background(), conn.WithContext, []schemamigrate.Table{{Model: &Record{}}}); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			for _, id := range []int64{1, 2} {
				record, err := store.Get(context.Background(), id)
				if err != nil || !record.CreatedAt.Equal(instant) || !record.UpdatedAt.Equal(instant) {
					t.Fatalf("迁移改变创建/更新时刻: %+v %v", record, err)
				}
				if id == 2 {
					if record.ExpiresAt != nil || record.FinishedAt != nil {
						t.Fatalf("迁移改变 NULL: %+v", record)
					}
				} else if record.ExpiresAt == nil || record.FinishedAt == nil || !record.ExpiresAt.Equal(instant) || !record.FinishedAt.Equal(instant) {
					t.Fatalf("迁移改变过期/完成时刻: %+v", record)
				}
			}
		})
	}
}
