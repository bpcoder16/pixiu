package websocketx

import (
	"errors"
	"sync"
)

// BudgetConfig 设置多个 Endpoint 共用的逻辑积压上限。
// 字节零值默认接收 128 MiB、发送 1 GiB；条数零值不设共享条数上限。
type BudgetConfig struct {
	ReceiveBytes    int64
	SendBytes       int64
	ReceiveMessages int64
	SendMessages    int64
}

// Usage 包含读取/回调/写入中的占用，不等于进程物理内存。
type Usage struct {
	ReceiveMessages int64
	ReceiveBytes    int64
	SendMessages    int64
	SendBytes       int64
}

// Budget 并发安全，零值使用默认配置；首次使用后不得复制。
type Budget struct {
	mu     sync.Mutex
	config BudgetConfig
	usage  Usage
}

func NewBudget(cfg BudgetConfig) (*Budget, error) {
	if cfg.ReceiveBytes < 0 || cfg.SendBytes < 0 || cfg.ReceiveMessages < 0 || cfg.SendMessages < 0 {
		return nil, errors.New("websocketx: negative budget")
	}
	return &Budget{config: cfg}, nil
}

func (b *Budget) Usage() Usage {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.usage
}

// Limits 返回包含默认值的只读配置副本，供组合层判断过载恢复水位。
// 条数为零表示不限制；修改返回值不会影响正在使用的预算。
func (b *Budget) Limits() BudgetConfig {
	b.mu.Lock()
	defer b.mu.Unlock()
	cfg := b.config
	if cfg.ReceiveBytes == 0 {
		cfg.ReceiveBytes = 128 << 20
	}
	if cfg.SendBytes == 0 {
		cfg.SendBytes = 1 << 30
	}
	return cfg
}

// reserve 尝试预留 messages 条消息和 size 字节的共享预算，参数均须非负。
// receive 为 true 时使用接收预算，否则使用发送预算。
// 任一额度不足时返回 false 且不改变占用；成功返回 true，调用方须在使用结束后通过 release 归还额度。
// 调用方先锁 Session 再锁 Budget；Budget 从不反向进入 Session 或 Endpoint。
func (b *Budget) reserve(receive bool, messages, size int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	count, used, countLimit, byteLimit := b.values(receive)
	// 条数上限为零表示不限；按剩余额度比较，避免已用量与申请量相加溢出。
	if (countLimit > 0 && messages > countLimit-*count) || size > byteLimit-*used {
		return false
	}
	*count += messages
	*used += size
	return true
}

// reserveReadBytes 在读取前预留最多 maximum 字节的共享接收预算，maximum 须非负。
// 返回实际预留字节数，额度不足时可少于 maximum，耗尽时为零；不占用消息条数。
// 调用方读取后须释放未使用的预留额度，保留的正文额度在处理结束或丢弃时释放。
func (b *Budget) reserveReadBytes(maximum int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, used, _, limit := b.values(true)
	n := min(maximum, limit-*used)
	*used += n
	return n
}

// release 归还已预留的 messages 条消息和 size 字节，receive 为 true 表示接收，否则表示发送。
// 参数须非负且不得超过尚未归还的对应占用；仅更新逻辑占用，由调用方保证申请与归还配对。
func (b *Budget) release(receive bool, messages, size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	count, used, _, _ := b.values(receive)
	*count -= messages
	*used -= size
}

// values 按 receive 选择接收或发送预算，依次返回消息条数和字节占用的指针、条数上限、字节上限。
// 字节上限补齐默认值（接收 128 MiB、发送 1 GiB），条数上限为零表示不限。
// 调用方须持有 b.mu；返回的指针直接指向 b.usage，读写时必须保持锁保护。
func (b *Budget) values(receive bool) (*int64, *int64, int64, int64) {
	if receive {
		limit := b.config.ReceiveBytes
		if limit == 0 {
			limit = 128 << 20
		}
		return &b.usage.ReceiveMessages, &b.usage.ReceiveBytes, b.config.ReceiveMessages, limit
	}
	limit := b.config.SendBytes
	if limit == 0 {
		limit = 1 << 30
	}
	return &b.usage.SendMessages, &b.usage.SendBytes, b.config.SendMessages, limit
}

// fifo 按配置上限惰性分配槽位，弹出时清除引用，不关闭数据 channel。
type fifo[T any] struct {
	items  []T
	head   int
	length int
}

func (q *fifo[T]) push(value T, capacity int) {
	if q.items == nil {
		q.items = make([]T, capacity)
	}
	q.items[(q.head+q.length)%len(q.items)] = value
	q.length++
}

func (q *fifo[T]) pop() (T, bool) {
	var zero T
	if q.length == 0 {
		return zero, false
	}
	v := q.items[q.head]
	q.items[q.head] = zero
	q.head = (q.head + 1) % len(q.items)
	q.length--
	return v, true
}

func notify(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
