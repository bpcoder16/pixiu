package handlerregistry

import (
	"fmt"
	"maps"
	"sync"
)

// Entry 将函数绑定到稳定名称；仅表示声明，不管理执行或生命周期。
type Entry[H any] struct {
	Name    string
	Handler H
}

// Registry 为 infra 内的强类型注册表共用登记状态，零值可用。
// H 的函数签名由公开包约束；首次使用后不得复制，登记与快照可并发调用。
type Registry[H any] struct {
	mu       sync.RWMutex
	handlers map[string]H
	order    []string
}

// Register 校验并登记函数，kind 仅用于错误上下文，可选 name 最多一个。
// 失败不改变已有清单；反射仅用于函数校验和取名，不调用业务函数。
func (r *Registry[H]) Register(handler H, kind string, name ...string) error {
	resolved, err := resolveName(handler, kind, name)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.handlers[resolved]; exists {
		return fmt.Errorf("duplicate %s handler %q", kind, resolved)
	}
	if r.handlers == nil {
		r.handlers = make(map[string]H)
	}
	// 名称索引和登记顺序在同一把锁下发布，快照不会读到半条声明。
	r.handlers[resolved] = handler
	r.order = append(r.order, resolved)
	return nil
}

// Entries 按登记顺序返回独立清单；空注册表返回 nil。
func (r *Registry[H]) Entries() []Entry[H] {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.order) == 0 {
		return nil
	}
	entries := make([]Entry[H], len(r.order))
	for i, name := range r.order {
		entries[i] = Entry[H]{
			Name:    name,
			Handler: r.handlers[name],
		}
	}
	return entries
}

// Snapshot 返回独立映射；空注册表返回 nil。
// 两种快照均只复制容器，函数捕获的业务依赖仍由调用方管理。
func (r *Registry[H]) Snapshot() map[string]H {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return maps.Clone(r.handlers)
}
