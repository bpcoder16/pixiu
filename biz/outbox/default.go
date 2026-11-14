package outbox

import (
	"context"
	"errors"
	"sync/atomic"

	"gorm.io/gorm"
)

var defaultStore atomic.Pointer[Store]
var defaultWorker atomic.Pointer[Worker]

// NewDefaultStore 创建并登记唯一的默认 Store；失败不占用默认位置，重复登记返回错误。
// 应在启动阶段登记，完成 Store 和 Worker 初始化后再运行业务；数据库仍由调用方管理。
func NewDefaultStore(cfg StoreConfig) (*Store, error) {
	if defaultStore.Load() != nil {
		return nil, errors.New("outbox: default store is already registered")
	}
	store, err := NewStore(cfg)
	if err != nil {
		return nil, err
	}
	if !defaultStore.CompareAndSwap(nil, store) {
		return nil, errors.New("outbox: default store is already registered")
	}
	return store, nil
}

// DefaultStore 返回显式登记的默认 Store；未登记时 panic。
// NewStore 不设置默认实例，登记后不能替换或重置。
func DefaultStore() *Store {
	store := defaultStore.Load()
	if store == nil {
		panic("outbox: default store is not registered")
	}
	return store
}

// NewDefaultWorker 创建并登记唯一的默认 Worker，绑定已登记的默认 Store。
// 未登记 Store、Handler 无效或重复登记均返回错误；失败可重试，不替换已有实例。
// Handler 表在创建时复制；不启动后台服务，不管理数据库或消息客户端的生命周期。
func NewDefaultWorker(handlers map[HandlerKey]Handler) (*Worker, error) {
	if defaultWorker.Load() != nil {
		return nil, errors.New("outbox: default worker is already registered")
	}
	// 默认 Store 只发布一次，保证登记与发送始终使用同一实例。
	store := defaultStore.Load()
	if store == nil {
		return nil, errors.New("outbox: default store is not registered")
	}
	worker, err := NewWorker(store, handlers)
	if err != nil {
		return nil, err
	}
	if !defaultWorker.CompareAndSwap(nil, worker) {
		return nil, errors.New("outbox: default worker is already registered")
	}
	return worker, nil
}

// DefaultWorker 返回显式登记的默认 Worker；未登记时 panic。
// NewWorker 不设置默认实例，登记后不能替换或重置。
func DefaultWorker() *Worker {
	worker := defaultWorker.Load()
	if worker == nil {
		panic("outbox: default worker is not registered")
	}
	return worker
}

// EnqueueTx 使用默认 Store 在调用方事务内登记，返回自增 ID；未登记 Store 时 panic。
// 不发布、不提交、不自动唤醒；最外层事务提交成功后才能使用 ID 发送。
func EnqueueTx(tx *gorm.DB, task Task) (int64, error) {
	return DefaultStore().EnqueueTx(tx, task)
}

// Get 使用默认 Store 查询任务；未登记 Store 时 panic。
func Get(ctx context.Context, id int64) (Record, error) {
	return DefaultStore().Get(ctx, id)
}

// Send 使用默认 Worker 主动发送一次；未登记 Worker 时 panic。
// 必须在最外层业务事务提交成功后调用，发送失败由下轮 cron 补偿。
func Send(ctx context.Context, id int64) error {
	return DefaultWorker().Send(ctx, id)
}

// SendAsync 使用默认 Worker 在后台发送一次；未登记 Worker 时在调用方 panic。
// 必须在最外层业务事务提交成功后调用；独立 5 秒超时，失败记录日志并留给 cron 补偿。
func SendAsync(id int64) {
	DefaultWorker().SendAsync(id)
}

// RetryPending 使用默认 Worker 执行一轮补偿；未登记 Worker 时 panic。
// batchSize 为正的查询页大小，应由配置分布式锁的 cronx 周期调用。
// 每条尝试有 5 秒执行期限，结果回写另计；每轮从头扫描 pending。
// 补偿后在 Store 当地 [02:00, 04:00) 内分批清理结束超过 30 天的记录。
func RetryPending(ctx context.Context, batchSize int) error {
	return DefaultWorker().RetryPending(ctx, batchSize)
}
