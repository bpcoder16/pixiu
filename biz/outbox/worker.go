package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

// Worker 执行主动发送与一轮补偿；支持同步 Send 和独立 5 秒超时的 SendAsync。
// 每条任务使用独立事务行锁；多实例补偿轮次由应用的 cron 分布式锁协调。
type Worker struct {
	store    *Store
	handlers map[HandlerKey]Handler
}

func NewWorker(store *Store, handlers map[HandlerKey]Handler) (*Worker, error) {
	if store == nil || store.cfg.MasterDB == nil || len(handlers) == 0 {
		return nil, ErrInvalidArgument
	}
	snapshot := make(map[HandlerKey]Handler, len(handlers))
	for key, handler := range handlers {
		if !validKey(key.Kind, 64) || key.Version <= 0 || int64(key.Version) > 2147483647 || handler == nil {
			return nil, fmt.Errorf("%w: handler key or function", ErrInvalidArgument)
		}
		snapshot[key] = handler
	}
	return &Worker{
		store:    store,
		handlers: snapshot,
	}, nil
}

// Send 在业务事务提交后尝试发送一次；失败保存原因，由下轮 cron 补偿。
// 已成功任务直接返回 nil；过期返回 ErrExpired；同一任务被锁定时返回 ErrBusy。
func (w *Worker) Send(ctx context.Context, id int64) error {
	if ctx == nil || id <= 0 {
		return ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	taskErr, storageErr := w.send(ctx, id, false)
	return errors.Join(taskErr, storageErr)
}

// SendAsync 在最外层业务事务提交后启动 goroutine，用独立的 5 秒超时 context 尝试发送一次。
// 不接收请求 context；Handler 和发送日志通过任务记录中的 logId 关联业务。
// 错误记录日志，pending 任务由 cron 补偿；不提供等待或排空，退出可能中断本次尝试。
// Handler 须响应取消；结果回写仍有独立收尾过程，5 秒不是 goroutine 的强制结束期限。
func (w *Worker) SendAsync(id int64) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := w.Send(ctx, id); err != nil {
			// Send 的读取或加锁阶段也可能失败，异步调用须保留这些返回错误。
			logit.Error(ctx, "OutboxAsyncFailed", logit.Int64("task_id", id), logit.Str("err", errorText(err)))
		}
	}()
}

// RetryPending 按 ID 分页补偿本表的待发送任务，每条任务本轮最多尝试一次。
// batchSize 为查询页大小；跳过已锁任务，失败继续下一条，扫描/回写错误或取消则结束本轮。
// 应挂载到配置分布式锁的 cronx，避免多实例补偿轮次重叠。
func (w *Worker) RetryPending(ctx context.Context, batchSize int) error {
	if ctx == nil || batchSize <= 0 {
		return ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	db, masterErr := w.store.master(ctx)
	if masterErr != nil {
		return masterErr
	}
	// 固定本轮上界并单向翻页，失败任务不会在同一轮再次进入处理器。
	var upper []int64
	if err := db.Where("status = ?", Pending).Order("id DESC").Limit(1).Pluck("id", &upper).Error; err != nil {
		return err
	}
	if len(upper) == 0 {
		return nil
	}
	var last int64
	var firstErr error
	for {
		var ids []int64
		if err := db.Where("status = ? AND id > ? AND id <= ?", Pending, last, upper[0]).Order("id").Limit(batchSize).Pluck("id", &ids).Error; err != nil {
			return errors.Join(firstErr, err)
		}
		if len(ids) == 0 {
			return firstErr
		}
		for _, id := range ids {
			if err := ctx.Err(); err != nil {
				return errors.Join(firstErr, err)
			}
			taskErr, storageErr := w.send(ctx, id, true)
			if storageErr != nil {
				return errors.Join(firstErr, taskErr, storageErr)
			}
			// 逐条错误已有日志；只保留首个错误，避免大批失败占用无界内存。
			if taskErr != nil && !errors.Is(taskErr, ErrExpired) && firstErr == nil {
				firstErr = fmt.Errorf("outbox: task %d: %w", id, taskErr)
			}
			last = id
		}
	}
}
