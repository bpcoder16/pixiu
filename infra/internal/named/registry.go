package named

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// Registry 管理一个模块的可关闭命名客户端。
// 启动阶段可并发创建；所有创建返回后再启动业务，停止使用客户端后再关闭。
// CloseAll 不与 Create 并发，否则可能遗漏仍在构造中的客户端。
type Registry[T interface{ Close() error }] struct {
	module    string
	clients   sync.Map // name → T
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// New 创建模块专用的命名注册表。
func New[T interface{ Close() error }](module string) *Registry[T] {
	return &Registry[T]{module: module}
}

// Create 构造并登记客户端。已登记的名称不再构造；并发同名构造可能执行多次，
// 只有一个会登记，其余构造成功的客户端立即关闭。构造失败不占用名称。
// build 须返回新建且由本次调用独占的客户端；返回错误时须自行清理部分资源。
// 初始化完成后不得再调用；CloseAll 不等待正在构造的客户端。
func (r *Registry[T]) Create(name string, build func() (T, error)) (T, error) {
	var zero T
	if r.closed.Load() {
		return zero, fmt.Errorf("%s: named clients closed", r.module)
	}
	// 预检只避免已知重复名称再次建连；最终登记结果由 LoadOrStore 决定。
	if _, exists := r.clients.Load(name); exists {
		return zero, fmt.Errorf("%s: client %q is already registered", r.module, name)
	}
	client, err := build()
	if err != nil {
		return zero, err
	}
	if _, loaded := r.clients.LoadOrStore(name, client); loaded {
		duplicateErr := fmt.Errorf("%s: client %q is already registered", r.module, name)
		if closeErr := client.Close(); closeErr != nil {
			return zero, errors.Join(
				duplicateErr,
				fmt.Errorf("%s: close duplicate client %q: %w", r.module, name, closeErr),
			)
		}
		return zero, duplicateErr
	}
	return client, nil
}

// MustGet 返回命名客户端；不存在或关闭开始后 panic。
func (r *Registry[T]) MustGet(name string) T {
	value, exists := r.clients.Load(name)
	if r.closed.Load() {
		panic(r.module + ": named clients closed")
	}
	if !exists {
		panic(fmt.Sprintf("%s: client %q is not registered", r.module, name))
	}
	return value.(T)
}

// CloseAll 关闭所有客户端并汇总错误；并发及重复调用返回同一次结果。
func (r *Registry[T]) CloseAll() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		var errs []error
		r.clients.Range(func(name, value any) bool {
			r.clients.Delete(name)
			if err := value.(T).Close(); err != nil {
				errs = append(errs, fmt.Errorf("%s: close client %q: %w", r.module, name, err))
			}
			return true
		})
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
