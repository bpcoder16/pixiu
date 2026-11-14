package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func TestCleanupWindow(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	open := time.Date(2026, 11, 13, 2, 0, 0, 0, loc)
	closeAt := open.Add(2 * time.Hour)
	for _, tc := range []struct {
		now  time.Time
		want bool
	}{
		{open.Add(-time.Second), false},
		{open.UTC(), true},
		{closeAt.Add(-time.Microsecond), true},
		{closeAt, false},
		{open.AddDate(0, 0, 1), true},
	} {
		if got := inCleanupWindow(tc.now.In(loc)); got != tc.want {
			t.Errorf("Store 当地时间 %v 得到 %t，期望 %t", tc.now.In(loc), got, tc.want)
		}
	}
}

// 将当前时刻置于测试指定的当地整点附近，真实数据库测试可以在任意时段执行。
func storeHour(s *Store, hour int) {
	now := time.Now().UTC()
	target := time.Date(now.Year(), now.Month(), now.Day(), hour, 0, 0, 0, time.UTC)
	s.cfg.Location = time.FixedZone("测试时区", int(target.Sub(now)/time.Second))
}

func finishedTask(t *testing.T, s *Store, db *gorm.DB, status Status, finishedAt any) int64 {
	t.Helper()
	id := enqueue(t, s, db)
	err := db.Model(&Record{}).Where("id = ?", id).Updates(map[string]any{
		"status":      status,
		"finished_at": finishedAt,
	}).Error
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestRetryPendingCleansFinishedInBatches(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := context.Background()
		// 覆盖保留期两侧，确保按固定 30 天清理。
		now := time.Now()
		old := now.Add(-30*24*time.Hour - time.Minute)
		var expired []int64
		for range 3 {
			for _, status := range []Status{Succeeded, Failed} {
				expired = append(expired, finishedTask(t, s, db, status, old))
			}
		}
		recent := finishedTask(t, s, db, Succeeded, now.Add(-30*24*time.Hour+time.Minute))
		unfinished := finishedTask(t, s, db, Failed, nil)
		w := testWorker(t, s, func(context.Context, Delivery) error {
			t.Fatal("所有任务均已结束")
			return nil
		})

		storeHour(s, 12)
		if err := w.RetryPending(ctx, 1); err != nil {
			t.Fatal(err)
		}
		for _, id := range expired {
			if _, err := s.Get(ctx, id); err != nil {
				t.Fatalf("窗口外误删 %d: %v", id, err)
			}
		}
		storeHour(s, 3)
		for range 2 {
			if err := w.RetryPending(ctx, 1); err != nil {
				t.Fatal(err)
			}
			for _, id := range expired {
				if _, err := s.Get(ctx, id); !errors.Is(err, ErrNotFound) {
					t.Fatalf("单轮应清完多批，即使没有 pending: id=%d err=%v", id, err)
				}
			}
			for _, id := range []int64{recent, unfinished} {
				if _, err := s.Get(ctx, id); err != nil {
					t.Fatalf("误删保留记录 %d: %v", id, err)
				}
			}
		}
	})
}

func TestRetryPendingCleanupSkipsLockedAndPending(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		storeHour(s, 3)
		ctx := context.Background()
		old := time.Now().AddDate(0, -2, 0)
		locked := finishedTask(t, s, db, Succeeded, old)
		free := finishedTask(t, s, db, Succeeded, old)
		pending := finishedTask(t, s, db, Pending, old)
		sendErr := errors.New("发送失败")
		w := testWorker(t, s, func(context.Context, Delivery) error { return sendErr })

		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		defer tx.Rollback()
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&Record{}, locked).Error; err != nil {
			t.Fatal(err)
		}
		if err := w.RetryPending(ctx, 1); !errors.Is(err, sendErr) {
			t.Fatalf("应保留补偿错误: %v", err)
		}
		if _, err := s.Get(ctx, free); !errors.Is(err, ErrNotFound) {
			t.Fatalf("补偿错误或其他记录被锁不应挡住清理: %v", err)
		}
		for _, id := range []int64{locked, pending} {
			if _, err := s.Get(ctx, id); err != nil {
				t.Fatalf("误删持锁或待重试记录 %d: %v", id, err)
			}
		}
		if err := tx.Rollback().Error; err != nil {
			t.Fatal(err)
		}
		if err := w.RetryPending(ctx, 1); !errors.Is(err, sendErr) {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, locked); !errors.Is(err, ErrNotFound) {
			t.Fatalf("下轮应回收已解锁记录: %v", err)
		}
	})
}

func TestCleanupBatchKeepsCutoffAndPending(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := context.Background()
		cutoff := time.Now().Truncate(time.Microsecond)
		old := cutoff.Add(-time.Microsecond)
		oldID := finishedTask(t, s, db, Failed, old)
		equalID := finishedTask(t, s, db, Failed, cutoff)
		pendingID := finishedTask(t, s, db, Pending, old)
		if n, err := deleteFinishedBatch(db.WithContext(ctx), Failed, cutoff, 10); err != nil || n != 1 {
			t.Fatalf("应只删除截止点前的已结束记录: n=%d err=%v", n, err)
		}
		if _, err := s.Get(ctx, oldID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("截止点前一微秒的记录应删除: %v", err)
		}
		for _, id := range []int64{equalID, pendingID} {
			if _, err := s.Get(ctx, id); err != nil {
				t.Fatalf("应保留记录 %d: %v", id, err)
			}
		}
	})
}

func TestRetryPendingCleanupCommitsBeforeCancellation(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		storeHour(s, 3)
		first := finishedTask(t, s, db, Failed, time.Now().AddDate(0, -3, 0))
		second := finishedTask(t, s, db, Failed, time.Now().AddDate(0, -2, 0))
		w := testWorker(t, s, func(context.Context, Delivery) error { return nil })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		const callback = "outbox_test:cancel_second_delete"
		batches := 0
		if err := db.Callback().Delete().Before("gorm:delete").Register(callback, func(tx *gorm.DB) {
			batches++
			if batches == 2 {
				cancel()
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Delete().Remove(callback) })
		if err := w.RetryPending(ctx, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("第二批取消应结束调用: %v", err)
		}
		ctx = context.Background()
		if _, err := s.Get(ctx, first); !errors.Is(err, ErrNotFound) {
			t.Fatalf("已提交的首批应保留删除结果: %v", err)
		}
		if _, err := s.Get(ctx, second); err != nil {
			t.Fatalf("取消的批次应保留记录: %v", err)
		}
		if err := w.RetryPending(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Get(ctx, second); !errors.Is(err, ErrNotFound) {
			t.Fatalf("下轮应能继续清理: %v", err)
		}
	})
}
