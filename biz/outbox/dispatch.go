package outbox

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/logit"
	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// send 每条任务独立事务，行锁覆盖发送与结果回写；区分任务错误和存储错误，供 cron 决定是否继续。
// retry 表示补偿调用，已锁或不再待发送的任务直接跳过。
func (w *Worker) send(ctx context.Context, id int64, retry bool) (taskErr, storageErr error) {
	defer func() {
		// 事务使用可分阶段取消的 context；保留调用方 DeadlineExceeded 等原始取消原因。
		if storageErr != nil && ctx.Err() != nil {
			storageErr = errors.Join(ctx.Err(), storageErr)
		}
	}()
	s := w.store
	db, masterErr := s.master(ctx)
	if masterErr != nil {
		return nil, masterErr
	}
	// BeginTx 的 context 若在 Handler 执行中取消，database/sql 会自动回滚并提前释放锁。
	// 获取锁及准备阶段跟随调用方取消，发送开始后改为等 Handler 返回，再回写并结束事务。
	txCtx, cancelTx := context.WithCancel(context.WithoutCancel(db.Statement.Context))
	defer cancelTx()
	stopCancellation := context.AfterFunc(ctx, cancelTx)
	defer stopCancellation()
	tx := db.WithContext(txCtx).Begin()
	if tx.Error != nil {
		return nil, tx.Error
	}
	defer tx.Rollback()

	query := clean(tx).Clauses(clause.Locking{
		Strength: "UPDATE",
		Options:  "NOWAIT",
	}).Where("id = ?", id)
	if retry {
		query = query.Where("status = ?", Pending)
	}
	var row Record
	if err := query.Take(&row).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			if retry {
				return nil, nil
			}
			return nil, ErrNotFound
		}
		var my *mysqldriver.MySQLError
		var pg *pgconn.PgError
		// MySQL ER_LOCK_NOWAIT / PostgreSQL lock_not_available 表示当前行正在发送。
		if errors.As(err, &my) && my.Number == 3572 || errors.As(err, &pg) && pg.Code == "55P03" {
			if retry {
				return nil, nil
			}
			return nil, errors.Join(ErrBusy, err)
		}
		return nil, err
	}
	if row.Status == Succeeded {
		return nil, nil
	}
	if row.Status == Failed {
		return ErrExpired, nil
	}
	// 持锁读取后按应用服务器时钟判断期限；比较绝对时刻，无需转换时区。
	if row.ExpiresAt != nil && !row.ExpiresAt.After(time.Now()) {
		if err := clean(tx).Where("id = ?", id).Updates(map[string]any{
			"status":      Failed,
			"last_error":  ErrExpired.Error(),
			"finished_at": gorm.Expr(s.nowSQL()),
			"updated_at":  gorm.Expr(s.nowSQL()),
		}).Error; err != nil {
			return ErrExpired, err
		}
		return ErrExpired, tx.Commit().Error
	}
	if !stopCancellation() {
		return nil, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// 已持有行锁，次数先在内存递增，随发送结果一次更新并提交。
	row.Attempts++
	ctx = logit.NewContextScope(ctx)
	if row.LogID != "" {
		logit.AddMeta(ctx, logit.Str(logit.LogId, row.LogID))
	}
	delivery := Delivery{
		Kind:    string(row.Kind),
		Version: int(row.Version),
		Payload: row.Payload,
		ID:      id,
		Attempt: row.Attempts,
	}
	if row.ExpiresAt != nil {
		delivery.ExpiresAt = row.ExpiresAt.Time
	}
	begin := time.Now()
	taskErr = invoke(ctx, w.handlers[HandlerKey{Kind: delivery.Kind, Version: delivery.Version}], delivery)
	state := Succeeded
	values := map[string]any{
		"attempts":    row.Attempts,
		"status":      state,
		"last_error":  "",
		"updated_at":  gorm.Expr(s.nowSQL()),
		"finished_at": gorm.Expr(s.nowSQL()),
	}
	if taskErr != nil {
		state = Pending
		values["status"] = state
		values["last_error"] = errorText(taskErr)
		values["finished_at"] = nil
	}
	// 即使调用方已取消，结果 UPDATE 仍有独立的 3 秒期限，并将超时转发给事务。
	// 已进入 MySQL 驱动的 Commit/Rollback 不受该 context 中断，仍需调用方配置连接读写超时。
	writeCtx, cancelWrite := context.WithTimeout(context.WithoutCancel(txCtx), 3*time.Second)
	defer cancelWrite()
	stopWrite := context.AfterFunc(writeCtx, cancelTx)
	defer stopWrite()
	storageErr = clean(tx).WithContext(writeCtx).Where("id = ?", id).Updates(values).Error
	if storageErr == nil {
		// 发送失败也须提交 Attempts/LastError，不能随 Handler 的错误回滚。
		storageErr = tx.Commit().Error
	}
	fields := []logit.Field{
		logit.Int64("task_id", id),
		logit.Str("kind", delivery.Kind),
		logit.Int("version", delivery.Version),
		logit.Int64("attempt", delivery.Attempt),
		logit.Int64("duration_ms", time.Since(begin).Milliseconds()),
		logit.Str("status", string(state)),
	}
	if taskErr != nil {
		fields = append(fields, logit.Str("err", errorText(taskErr)))
	}
	if storageErr != nil {
		logit.Error(ctx, "OutboxWriteFailed", append(fields, logit.Str("write_error", storageErr.Error()))...)
	} else if taskErr != nil {
		logit.Error(ctx, "OutboxAttempt", fields...)
	} else {
		logit.Info(ctx, "OutboxAttempt", fields...)
	}
	return taskErr, storageErr
}

func invoke(ctx context.Context, handler Handler, delivery Delivery) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("outbox: handler panic: %v", recovered)
			logit.Error(ctx, "OutboxPanic", logit.Int64("task_id", delivery.ID), logit.Str("stack", string(debug.Stack())))
		}
	}()
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	if handler == nil {
		return errors.New("outbox: unknown task kind or version")
	}
	return handler(ctx, delivery)
}

func errorText(err error) (text string) {
	defer func() {
		if recover() != nil {
			text = "outbox: error formatting panicked"
		}
	}()
	text = strings.ToValidUTF8(err.Error(), "�")
	if len(text) > 4096 {
		text = text[:4096]
		for !utf8.ValidString(text) {
			text = text[:len(text)-1]
		}
	}
	return text
}
