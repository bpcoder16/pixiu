package clickhousex

import (
	"context"
	"errors"
	"strings"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

var namedClients = named.New[*Client]("clickhousex")

// NewNamed 创建并按 Config.Name 登记客户端。名称重复时不替换已有实例。
// 启动阶段可并发创建；同名构造只登记一个，未登记的新客户端会关闭。
// 初始化完成后不得再调用，所有创建返回后才开始查询，关闭须在初始化之后执行。
func NewNamed(ctx context.Context, cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Name) == "" {
		return nil, errors.New("clickhousex: empty database name")
	}
	return namedClients.Create(cfg.Name, func() (*Client, error) {
		return New(ctx, cfg)
	})
}

// Named 返回已登记的命名客户端；名称不存在或关闭开始后调用会 panic。
func Named(name string) *Client {
	return namedClients.MustGet(name)
}

// CloseAll 关闭全部已登记客户端；重复调用返回同一次关闭结果。
// 应用应先停止使用客户端的任务，再调用 CloseAll。
func CloseAll() error {
	return namedClients.CloseAll()
}
