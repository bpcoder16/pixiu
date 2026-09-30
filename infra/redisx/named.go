package redisx

import (
	"context"
	"errors"
	"strings"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

var namedClients = named.New[*Client]("redisx")

// NewNamed 创建并按 Config.Name 登记客户端。名称重复时不替换已有实例。
// 启动阶段由调用方串行创建；初始化完成后可并发查询。
// 初始化完成后不得再调用，所有创建返回后才开始查询，关闭须在初始化之后执行。
func NewNamed(ctx context.Context, cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("redisx: empty name")
	}
	return namedClients.Create(cfg.Name, func() (*Client, error) {
		return New(ctx, cfg)
	})
}

// Named 返回已登记的命名客户端；名称不存在或关闭开始后调用会 panic。
func Named(name string) *Client {
	return namedClients.MustGet(name)
}

// NewDefault 创建默认客户端，同时按 Config.Name 登记；已有默认实例时返回错误。
// Name 仍必填；创建失败可重试，初始化与关闭约束与 NewNamed 相同。
func NewDefault(ctx context.Context, cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("redisx: empty name")
	}
	return namedClients.CreateDefault(cfg.Name, func() (*Client, error) {
		return New(ctx, cfg)
	})
}

// Default 返回显式初始化的默认客户端，与 Named(cfg.Name) 是同一实例。
// 未初始化或 CloseAll 开始后调用会 panic；New 和 NewNamed 不设置默认实例。
func Default() *Client {
	return namedClients.MustDefault()
}

// CloseAll 关闭全部已登记的命名及默认客户端，每个实例只关闭一次。
// 重复调用返回同一次关闭结果。
// 应用应先停止使用客户端的任务和订阅，再调用 CloseAll。
func CloseAll() error {
	return namedClients.CloseAll()
}
