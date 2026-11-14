package outbox

import (
	"context"
	"database/sql"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// now 已按 Store 时区表示。
func inCleanupWindow(now time.Time) bool {
	return now.Hour() >= 2 && now.Hour() < 4
}

func cleanFinished(ctx context.Context, db *gorm.DB, location *time.Location, batchSize int) error {
	cutoff := time.Now().Add(-30 * 24 * time.Hour).Truncate(time.Microsecond)
	for _, status := range []Status{Succeeded, Failed} {
		for inCleanupWindow(time.Now().In(location)) {
			// 限制单批占用连接和等待锁的时间，已提交批次在取消或后续失败时仍有效。
			batchCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			count, err := deleteFinishedBatch(db.WithContext(batchCtx), status, cutoff, batchSize)
			cancel()
			if err != nil {
				return err
			}
			if count < batchSize {
				break
			}
		}
	}
	return nil
}

func deleteFinishedBatch(db *gorm.DB, status Status, cutoff time.Time, limit int) (int, error) {
	var ids []int64
	// READ COMMITTED 避免 InnoDB 在常规范围扫描中持有间隙锁。
	err := db.Transaction(func(tx *gorm.DB) error {
		// 查询和删除分别从同一筛选条件派生，删除时再次校验终态及时间。
		eligible := clean(tx).Where("status = ? AND finished_at < ?", status, cutoff).Session(&gorm.Session{})
		err := eligible.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).
			Order("finished_at, id").Limit(limit).Pluck("id", &ids).Error
		if err != nil || len(ids) == 0 {
			return err
		}
		return eligible.Where("id IN ?", ids).Delete(&Record{}).Error
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	return len(ids), err
}
