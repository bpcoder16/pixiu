package mysqlx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

var namedClients namedRegistry

type namedRegistry struct {
	clients   sync.Map // name → *Client
	closed    atomic.Bool
	closeOnce sync.Once
	closeErr  error
}

// NewNamed 创建并按 Config.Name 登记客户端。名称重复时不替换已有实例。
// 应用应在启动阶段顺序创建，不与 CloseAll 并发调用。
func NewNamed(ctx context.Context, cfg Config) (*Client, error) {
	if err := namedClients.canRegister(cfg.Name); err != nil {
		return nil, err
	}
	client, err := New(ctx, cfg)
	if err != nil {
		return nil, err
	}
	namedClients.clients.Store(cfg.Name, client)
	return client, nil
}

// Named 返回已登记的命名客户端；名称不存在或关闭开始后调用会 panic。
func Named(name string) *Client {
	return namedClients.named(name)
}

// CloseAll 关闭全部已登记客户端；重复调用返回同一次关闭结果。
// 应用应先停止使用客户端的任务，再调用 CloseAll。
func CloseAll() error {
	return namedClients.closeAll()
}

func (r *namedRegistry) canRegister(name string) error {
	if name == "" {
		return errors.New("mysqlx: empty database name")
	}
	if r.closed.Load() {
		return errors.New("mysqlx: named clients closed")
	}
	if _, exists := r.clients.Load(name); exists {
		return fmt.Errorf("mysqlx: client %q is already registered", name)
	}
	return nil
}

func (r *namedRegistry) named(name string) *Client {
	value, exists := r.clients.Load(name)
	if r.closed.Load() {
		panic("mysqlx: named clients closed")
	}
	if !exists {
		panic(fmt.Sprintf("mysqlx: client %q is not registered", name))
	}
	return value.(*Client)
}

func (r *namedRegistry) closeAll() error {
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		var errs []error
		r.clients.Range(func(name, value any) bool {
			r.clients.Delete(name)
			if err := value.(*Client).Close(); err != nil {
				errs = append(errs, fmt.Errorf("mysqlx: close client %q: %w", name, err))
			}
			return true
		})
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}
