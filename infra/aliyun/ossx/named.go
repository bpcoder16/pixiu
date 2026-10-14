package ossx

import "github.com/bpcoder16/pixiu/infra/internal/named"

var namedClients = named.New[*Client]("ossx")

// NewNamed 构造并登记命名实例；仅在启动阶段串行调用。
func NewNamed(cfg Config, opts ...Option) (*Client, error) {
	return namedClients.Create(cfg.Name, func() (*Client, error) { return New(cfg, opts...) })
}

// NewDefault 构造默认实例，并同时按 Config.Name 登记；仅在启动阶段串行调用。
func NewDefault(cfg Config, opts ...Option) (*Client, error) {
	return namedClients.CreateDefault(cfg.Name, func() (*Client, error) { return New(cfg, opts...) })
}

// Named 返回已登记实例；名称不存在或 CloseAll 开始后 panic。
func Named(name string) *Client { return namedClients.MustGet(name) }

// Default 返回默认实例；未初始化或 CloseAll 开始后 panic。
func Default() *Client { return namedClients.MustDefault() }

// CloseAll 关闭本包已登记实例，不影响独立 New 或其他包的客户端。
// 仅在使用方全部停止后调用；重复调用返回同一次结果。
func CloseAll() error { return namedClients.CloseAll() }
