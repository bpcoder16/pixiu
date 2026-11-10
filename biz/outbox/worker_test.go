package outbox

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func testWorker(t *testing.T, s *Store, handler Handler) *Worker {
	t.Helper()
	w, err := NewWorker(s, map[HandlerKey]Handler{
		{
			Kind:    "test.publish",
			Version: 1,
		}: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func TestSendAndCronRounds(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		calls := map[int64]int{}
		a := enqueue(t, s, db)
		b := enqueue(t, s, db)
		c := enqueue(t, s, db)
		w := testWorker(t, s, func(_ context.Context, d Delivery) error {
			calls[d.ID]++
			if d.ID == a || d.Attempt == 1 {
				return errors.New("发送失败")
			}
			return nil
		})
		ctx := context.Background()
		if err := w.Send(ctx, b); err == nil {
			t.Fatal("首次失败应返回错误")
		}
		r, _ := s.Get(ctx, b)
		if r.Status != Pending || r.Attempts != 1 || r.LastError == "" {
			t.Fatalf("首次结果错误: %+v", r)
		}
		for round := 1; round <= 2; round++ {
			if err := w.RetryPending(ctx, 1); err == nil {
				t.Fatal("失败应向 cron 返回错误")
			}
			if calls[a] != round {
				t.Fatalf("同轮反复执行: %v", calls)
			}
		}
		for _, id := range []int64{b, c} {
			r, _ := s.Get(ctx, id)
			if r.Status != Succeeded || r.Attempts != 2 || r.LastError != "" || r.FinishedAt == nil {
				t.Fatalf("补偿错误: %+v", r)
			}
			if err := w.Send(ctx, id); err != nil {
				t.Fatal(err)
			}
			if calls[id] != 2 {
				t.Fatal("已成功任务再次执行")
			}
		}
	})
}

func TestExpiryAndNoExpiry(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := context.Background()
		calls := map[int64]int{}
		w := testWorker(t, s, func(_ context.Context, d Delivery) error {
			calls[d.ID]++
			return errors.New("离线")
		})
		ids := map[string]int64{}
		for _, name := range []string{"expired", "later", "forever"} {
			value := task()
			if name == "expired" {
				value.ExpiresAt = time.Now().Add(-time.Hour)
			}
			if name == "later" {
				value.ExpiresAt = time.Now().Add(time.Hour)
			}
			ids[name] = enqueueTask(t, s, db, value)
		}
		if err := w.Send(ctx, ids["expired"]); !errors.Is(err, ErrExpired) {
			t.Fatalf("过期应拒绝: %v", err)
		}
		if err := w.Send(ctx, ids["later"]); err == nil {
			t.Fatal("应失败")
		}
		if err := db.Model(&Record{}).Where("id = ?", ids["later"]).Update("expires_at", time.Now().Add(-time.Hour)).Error; err != nil {
			t.Fatal(err)
		}
		for range 2 {
			_ = w.RetryPending(ctx, 1)
		}
		for _, id := range []int64{ids["expired"], ids["later"]} {
			r, _ := s.Get(ctx, id)
			if r.Status != Failed || r.LastError != ErrExpired.Error() || r.FinishedAt == nil {
				t.Fatalf("过期未标记: %+v", r)
			}
		}
		if calls[ids["expired"]] != 0 || calls[ids["later"]] != 1 || calls[ids["forever"]] != 2 {
			t.Fatalf("过期执行或重试异常: %v", calls)
		}
		r, _ := s.Get(ctx, ids["forever"])
		if r.ExpiresAt != nil || r.Status != Pending {
			t.Fatalf("零值应不过期: %+v", r)
		}
	})
}

func TestExpiryAfterLockedRead(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		value := task()
		value.ExpiresAt = time.Now().Add(time.Second).Truncate(time.Microsecond)
		id := enqueueTask(t, s, db, value)
		const callback = "outbox_test:delay_locked_read"
		if err := db.Callback().Query().After("gorm:after_query").Register(callback, func(tx *gorm.DB) {
			if _, locking := tx.Statement.Clauses["FOR"]; locking && tx.Error == nil {
				// 模拟查询结果延迟返回；与数据库一致按绝对时刻等待，唤醒后确认已到期。
				for time.Now().Before(value.ExpiresAt) {
					time.Sleep(time.Until(value.ExpiresAt))
				}
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Query().Remove(callback) })
		calls := 0
		w := testWorker(t, s, func(context.Context, Delivery) error {
			calls++
			return nil
		})
		ctx := context.Background()
		if err := w.Send(ctx, id); !errors.Is(err, ErrExpired) {
			t.Fatalf("读取期间过期的任务应拒绝发送: %v", err)
		}
		row, err := s.Get(ctx, id)
		if err != nil || row.Status != Failed || row.Attempts != 0 || calls != 0 || row.FinishedAt == nil || row.LastError != ErrExpired.Error() {
			t.Fatalf("过期任务应保存失败原因且不尝试发送: %+v calls=%d err=%v", row, calls, err)
		}
	})
}

func TestSendUsesOneReadAndOneUpdate(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		reads, writes := 0, 0
		const callback = "outbox_test:count_dispatch_sql"
		if err := db.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "pixiu_outbox" {
				reads++
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Query().Remove(callback) })
		if err := db.Callback().Update().After("gorm:update").Register(callback, func(tx *gorm.DB) {
			if tx.Statement.Table == "pixiu_outbox" {
				writes++
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Update().Remove(callback) })
		sendErr := errors.New("发送失败")
		for _, tc := range []struct {
			name       string
			expiry     time.Time
			handlerErr error
			wantErr    error
			status     Status
			attempts   int64
		}{
			{
				name:     "成功",
				expiry:   time.Now().Add(time.Hour),
				status:   Succeeded,
				attempts: 1,
			},
			{
				name:       "发送失败",
				handlerErr: sendErr,
				wantErr:    sendErr,
				status:     Pending,
				attempts:   1,
			},
			{
				name:    "已过期",
				expiry:  time.Now().Add(-time.Hour),
				wantErr: ErrExpired,
				status:  Failed,
			},
			{
				name:     "不过期",
				status:   Succeeded,
				attempts: 1,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				value := task()
				value.ExpiresAt = tc.expiry
				id := enqueueTask(t, s, db, value)
				calls := 0
				w := testWorker(t, s, func(_ context.Context, delivery Delivery) error {
					calls++
					if writes != 0 {
						t.Errorf("发送前应仅加锁读取，更新次数为 %d", writes)
					}
					if delivery.Attempt != 1 {
						t.Errorf("内存中的次数必须包含本次尝试: %d", delivery.Attempt)
					}
					return tc.handlerErr
				})
				reads, writes = 0, 0
				ctx := context.Background()
				if err := w.Send(ctx, id); !errors.Is(err, tc.wantErr) {
					t.Fatalf("发送结果错误: %v, want %v", err, tc.wantErr)
				}
				if reads != 1 || writes != 1 {
					t.Errorf("单条任务应仅查询和更新各一次: reads=%d writes=%d", reads, writes)
				}
				row, err := s.Get(ctx, id)
				if err != nil || row.Status != tc.status || row.Attempts != tc.attempts || int64(calls) != tc.attempts {
					t.Fatalf("结果与次数必须一起保存，过期不应调用 Handler: %+v calls=%d err=%v", row, calls, err)
				}
				wantError := ""
				if tc.wantErr != nil {
					wantError = tc.wantErr.Error()
				}
				if row.LastError != wantError || (row.FinishedAt != nil) != (tc.status != Pending) {
					t.Fatalf("错误原因或完成时间不符: %+v", row)
				}
			})
		}
	})
}

func TestRowLocksAcrossStoresAndConcurrentTasks(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		locked := enqueue(t, s, db)
		direct := enqueue(t, s, db)
		retried := enqueue(t, s, db)
		// 观察实际 SQL，保证主动发送与补偿均使用 NOWAIT 获取行锁。
		const callback = "outbox_test:check_nowait"
		if err := db.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
			if _, locking := tx.Statement.Clauses["FOR"]; locking && !strings.HasSuffix(tx.Statement.SQL.String(), " FOR UPDATE NOWAIT") {
				t.Errorf("行锁查询应使用 NOWAIT: %s", tx.Statement.SQL.String())
			}
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Query().Remove(callback) })
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		w := testWorker(t, s, func(_ context.Context, d Delivery) error {
			if d.ID == locked {
				close(entered)
				<-release
			}
			return nil
		})
		// 使用不同 Store，确保互斥来自数据库而非进程内共享对象。
		other, err := NewStore(s.cfg)
		if err != nil {
			t.Fatal(err)
		}
		otherWorker := testWorker(t, other, func(_ context.Context, d Delivery) error {
			if d.ID == locked {
				t.Error("已加锁任务不应进入第二个 Handler")
			}
			return nil
		})
		done := make(chan error, 1)
		go func() { done <- w.Send(ctx, locked) }()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("未进入发送")
		}
		if err := otherWorker.Send(ctx, locked); !errors.Is(err, ErrBusy) {
			t.Fatalf("同一行应立即返回 ErrBusy: %v", err)
		}
		if err := w.Send(ctx, direct); err != nil {
			t.Fatalf("同一 Store 的不同任务应可并发发送: %v", err)
		}
		if err := otherWorker.RetryPending(ctx, 1); err != nil {
			t.Fatalf("补偿应跳过已锁行并继续处理其他任务: %v", err)
		}
		if row, err := s.Get(ctx, retried); err != nil || row.Status != Succeeded || row.Attempts != 1 {
			t.Fatalf("未补偿其他任务: %+v %v", row, err)
		}
		once.Do(func() { close(release) })
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := otherWorker.Send(ctx, locked); err != nil {
			t.Fatalf("锁释放后已成功任务应直接返回: %v", err)
		}
	})
}

func TestResultWriteFailureAndHandlerErrors(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx := context.Background()
		calls := 0
		var panicID int64
		w := testWorker(t, s, func(_ context.Context, d Delivery) error {
			calls++
			if d.ID == panicID {
				panic("异常")
			}
			return nil
		})
		resultID := enqueue(t, s, db)
		const callback = "outbox_test:fail_result"
		fail := true
		err := db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
			values, ok := tx.Statement.Dest.(map[string]any)
			if ok && values["status"] == Succeeded && fail {
				fail = false
				tx.AddError(errors.New("回写失败"))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Update().Remove(callback) })
		if err := w.Send(ctx, resultID); err == nil {
			t.Fatal("回写失败应返回")
		}
		r, _ := s.Get(ctx, resultID)
		if r.Status != Pending || r.Attempts != 0 || calls != 1 {
			t.Fatalf("不能误标成功或立即重发: %+v", r)
		}
		if err := w.RetryPending(ctx, 1); err != nil {
			t.Fatal(err)
		}
		if calls != 2 {
			t.Fatal("未补偿")
		}

		panicID = enqueue(t, s, db)
		if err := w.Send(ctx, panicID); err == nil {
			t.Fatal("panic 应返回错误")
		}
		r, _ = s.Get(ctx, panicID)
		if r.Status != Pending || r.LastError == "" {
			t.Fatalf("panic 丢失: %+v", r)
		}
		value := task()
		value.Version = 2
		unknown := enqueueTask(t, s, db, value)
		if err := w.Send(ctx, unknown); err == nil {
			t.Fatal("未知版本应报错")
		}
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		if err := w.Send(cancelled, panicID); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	})
}

func TestCancelledSendHoldsLockAndSavesFailure(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		id := enqueue(t, s, db)
		cancelled := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		defer once.Do(func() { close(release) })
		w := testWorker(t, s, func(ctx context.Context, d Delivery) error {
			if d.Attempt == 1 {
				cancel()
				close(cancelled)
				<-release
				return ctx.Err()
			}
			return nil
		})
		done := make(chan error, 1)
		go func() { done <- w.Send(ctx, id) }()
		waitCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-cancelled:
		case <-waitCtx.Done():
			t.Fatal("未进入取消分支")
		}
		other, err := NewStore(s.cfg)
		if err != nil {
			t.Fatal(err)
		}
		otherWorker := testWorker(t, other, func(context.Context, Delivery) error {
			t.Error("原 Handler 返回前不得提前释放行锁")
			return nil
		})
		if err := otherWorker.Send(waitCtx, id); !errors.Is(err, ErrBusy) {
			t.Fatalf("context 取消后行锁必须保留到 Handler 返回: %v", err)
		}
		once.Do(func() { close(release) })
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("取消未返回: %v", err)
		}
		r, err := s.Get(waitCtx, id)
		if err != nil || r.Status != Pending || r.Attempts != 1 || r.LastError != context.Canceled.Error() {
			t.Fatalf("取消未提交失败原因: %+v %v", r, err)
		}
		if err := w.RetryPending(waitCtx, 10); err != nil {
			t.Fatal(err)
		}
		r, err = s.Get(waitCtx, id)
		if err != nil || r.Status != Succeeded || r.Attempts != 2 {
			t.Fatalf("取消后未补偿: %+v %v", r, err)
		}
	})
}

func TestRetryCommitsEachTaskIndependently(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		first := enqueue(t, s, db)
		second := enqueue(t, s, db)
		w := testWorker(t, s, func(ctx context.Context, d Delivery) error {
			if d.ID == second {
				row, err := s.Get(ctx, first)
				if err != nil || row.Status != Succeeded {
					t.Errorf("处理下一条前必须提交上一条: %+v %v", row, err)
				}
			}
			return nil
		})
		const callback = "outbox_test:fail_second_result"
		writes := 0
		err := db.Callback().Update().Before("gorm:update").Register(callback, func(tx *gorm.DB) {
			values, ok := tx.Statement.Dest.(map[string]any)
			if ok && values["status"] == Succeeded {
				writes++
				if writes == 2 {
					tx.AddError(errors.New("第二条回写失败"))
				}
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Callback().Update().Remove(callback) })
		ctx := context.Background()
		if err := w.RetryPending(ctx, 10); err == nil {
			t.Fatal("第二条存储失败应结束本轮")
		}
		if row, err := s.Get(ctx, first); err != nil || row.Status != Succeeded || row.Attempts != 1 {
			t.Fatalf("后续失败不得回滚已完成任务: %+v %v", row, err)
		}
		if row, err := s.Get(ctx, second); err != nil || row.Status != Pending || row.Attempts != 0 {
			t.Fatalf("失败事务必须整体回滚: %+v %v", row, err)
		}
	})
}

func TestSendCancellationWhileWaitingForConnection(t *testing.T) {
	eachDatabase(t, func(t *testing.T, s *Store, db *gorm.DB) {
		id := enqueue(t, s, db)
		pool, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		pool.SetMaxOpenConns(1)
		conn, err := pool.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		w := testWorker(t, s, func(context.Context, Delivery) error {
			t.Error("未取得连接时不得调用 Handler")
			return nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- w.Send(ctx, id) }()
		select {
		case err := <-done:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("等待连接时应响应调用方期限: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("等待连接时未响应取消")
		}
	})
}
