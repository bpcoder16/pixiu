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

// 调用方先锁 Session 再锁 Budget；Budget 从不反向进入 Session 或 Endpoint。
func (b *Budget) reserve(receive bool, messages, size int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	count, used, countLimit, byteLimit := b.values(receive)
	if (countLimit > 0 && messages > countLimit-*count) || size > byteLimit-*used {
		return false
	}
	*count += messages
	*used += size
	return true
}

func (b *Budget) reserveReadBytes(maximum int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, used, _, limit := b.values(true)
	n := min(maximum, limit-*used)
	*used += n
	return n
}

func (b *Budget) release(receive bool, messages, size int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	count, used, _, _ := b.values(receive)
	*count -= messages
	*used -= size
}

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
