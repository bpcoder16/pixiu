package realtime

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// 注册表只保存引用，运行及关闭始终由 Service 或其他显式生命周期管理器接管。
var namedServers = struct {
	sync.RWMutex
	servers map[string]*Server
}{
	servers: make(map[string]*Server),
}

// Register 登记全新且未运行的 Server。启动阶段串行登记完成后再启动服务。
// 空白名称、nil/无效实例、重复名称或同一实例的第二个名称返回错误，保留原登记。
// 不自动运行、不拥有 Server；调用方仍负责关闭未交给 Service 的实例。
func Register(name string, server *Server) error {
	if strings.TrimSpace(name) == "" || server == nil || server.endpoint == nil ||
		server.state.Load() != serverCreated || server.started.Load() {
		return errors.New("realtime: registration requires a name and a new Server")
	}
	namedServers.Lock()
	defer namedServers.Unlock()
	if _, exists := namedServers.servers[name]; exists {
		return fmt.Errorf("realtime: Server %q is already registered", name)
	}
	for registered, existing := range namedServers.servers {
		if existing == server {
			return fmt.Errorf("realtime: Server is already registered as %q", registered)
		}
	}
	namedServers.servers[name] = server
	return nil
}

// Named 返回已登记的同一实例，可并发获取。未登记或未知名称 panic。
// 服务停止后仍返回该实例，其推送方法按原生命周期返回关闭错误；不自动重建。
func Named(name string) *Server {
	namedServers.RLock()
	defer namedServers.RUnlock()
	server, exists := namedServers.servers[name]
	if !exists {
		panic(fmt.Sprintf("realtime: Server %q is not registered", name))
	}
	return server
}
