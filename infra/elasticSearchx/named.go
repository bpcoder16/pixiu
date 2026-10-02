package elasticSearchx

import (
	"context"
	"errors"
	"strings"

	"github.com/bpcoder16/pixiu/infra/internal/named"
)

var namedClients = named.New[managedClient]("elasticSearchx")

// 注册表要求无参数 Close；适配器保留 Client 原有的 context 关闭接口。
type managedClient struct {
	*Client
}

func (c managedClient) Close() error {
	return c.Client.Close(context.Background())
}

// RegisterNamed 供版本适配包调用：检查名称后构造并登记客户端。
// 业务使用 v7/v8/v9.NewNamed；build 须返回新建且独占的实例，失败时自行清理资源。
// 启动阶段串行初始化，全部创建返回后才开始业务；初始化完成后不得再调用。
func RegisterNamed(name string, build func() (*Client, error)) (*Client, error) {
	return registerClient(name, build, false)
}

// RegisterDefault 供版本适配包调用：构造默认客户端并同时按 name 登记。
// 业务使用 v7/v8/v9.NewDefault；构造与生命周期约束与 RegisterNamed 相同。
func RegisterDefault(name string, build func() (*Client, error)) (*Client, error) {
	return registerClient(name, build, true)
}

func registerClient(name string, build func() (*Client, error), asDefault bool) (*Client, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("elasticSearchx: empty client name")
	}
	create := namedClients.Create
	if asDefault {
		create = namedClients.CreateDefault
	}
	client, err := create(name, func() (managedClient, error) {
		c, err := build()
		return managedClient{Client: c}, err
	})
	if err != nil {
		return nil, err
	}
	return client.Client, nil
}

// Named 返回已登记的命名客户端；名称不存在或关闭开始后调用会 panic。
// 7/8/9 版本共用名称空间，初始化完成后可并发查询。
func Named(name string) *Client {
	return namedClients.MustGet(name).Client
}

// Default 返回显式初始化的默认客户端，与 Named(cfg.Name) 是同一实例。
// 未初始化或 CloseAll 开始后调用会 panic；New 和 NewNamed 不设置默认实例。
func Default() *Client {
	return namedClients.MustDefault().Client
}

// CloseAll 关闭全部已登记的命名及默认客户端，每个实例只关闭一次。
// 使用 context.Background() 关闭并汇总错误；重复调用返回同一次关闭结果。
// 应用须先完成全部初始化，再停止使用客户端的任务，最后调用 CloseAll。
func CloseAll() error {
	return namedClients.CloseAll()
}
