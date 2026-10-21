package natsx

import (
	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/nats-io/nats.go"
)

var namedClients = named.New[*Client]("natsx")

// NewNamed 创建并按 Config.Name 登记客户端，保留 New 的连接选项与错误语义。
// 仅在启动阶段串行创建，全部创建完成后再启动业务；不得与 CloseAll 并发。
// 注册表先检查关闭及重复状态，连接参数统一由 New 校验。
func NewNamed(cfg Config, options ...nats.Option) (*Client, error) {
	return namedClients.Create(cfg.Name, func() (*Client, error) {
		return New(cfg, options...)
	})
}

// Named 返回已登记的命名客户端；名称不存在或 CloseAll 开始后调用会 panic。
func Named(name string) *Client {
	return namedClients.MustGet(name)
}

// NewDefault 创建默认客户端，同时按 Config.Name 登记；已有默认实例时返回错误。
// 创建失败不占用名称或默认引用；初始化与关闭约束与 NewNamed 相同。
func NewDefault(cfg Config, options ...nats.Option) (*Client, error) {
	return namedClients.CreateDefault(cfg.Name, func() (*Client, error) {
		return New(cfg, options...)
	})
}

// Default 返回显式初始化的默认客户端，与 Named(cfg.Name) 是同一实例。
// 未初始化或 CloseAll 开始后调用会 panic；New 和 NewNamed 不设置默认实例。
func Default() *Client {
	return namedClients.MustDefault()
}

// CloseAll 排空并关闭全部默认及命名客户端，每个实例只关闭一次，重复调用返回同一次结果。
// 调用前停止并等待外部生产者；托管订阅和 worker 由 Client.Close 排空。
// 回调须使用启动时持有的客户端，排空期间不能再查询 Default/Named；其依赖须晚于 NATS 关闭。
// 多实例关闭顺序不固定，各自使用独立的 CloseTimeout，不支持关闭期跨实例依赖。
func CloseAll() error {
	return namedClients.CloseAll()
}
