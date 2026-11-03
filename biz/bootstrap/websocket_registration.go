package bootstrap

import (
	"fmt"
	"sync"

	"github.com/bpcoder16/pixiu/biz/realtime"
)

type webSocketRegistration struct {
	name    string
	factory func() (*realtime.Server, error)
	server  *realtime.Server
}

var webSocketInstances struct {
	registrations []*webSocketRegistration
	initialized   bool
}

// MustRegisterWebSocket 声明命名实例，仅在 MustBaseInit 前串行调用。
// 名称非法、重复、nil 工厂或初始化已开始时 panic。声明不构造、不监听。
// MustBaseInit 在共享客户端就绪后调用工厂并登记到 realtime.Named。
// 工厂须返回本次新建且未运行的 Server。运行和关闭由 NewWebSocketService(name) 接管，
// 不向资源栈登记正常关闭；初始化失败则取消本轮已构造实例。
func MustRegisterWebSocket(name string, factory func() (*realtime.Server, error)) {
	if baseInitStarted {
		panic(fmt.Errorf("bootstrap: initialization has started; cannot register WebSocket %q", name))
	}
	if err := validateInstanceName("WebSocket", name); err != nil {
		panic(err)
	}
	if factory == nil {
		panic(fmt.Errorf("bootstrap: nil WebSocket factory for %q", name))
	}
	for _, registration := range webSocketInstances.registrations {
		if registration.name == name {
			panic(fmt.Errorf("bootstrap: WebSocket %q is already registered", name))
		}
	}
	webSocketInstances.registrations = append(webSocketInstances.registrations, &webSocketRegistration{
		name:    name,
		factory: factory,
	})
}

func initWebSockets() (err error) {
	// 工厂可能已返回部分对象，失败时连同之前成功构造的实例一起取消。
	// 这些实例均未运行，正常运行期的关闭仍只属于 Service。
	defer func() {
		if err != nil {
			for _, registration := range webSocketInstances.registrations {
				if registration.server != nil {
					registration.server.Cancel()
				}
			}
		}
	}()
	for _, registration := range webSocketInstances.registrations {
		err = callServiceCleanup("websocket:"+registration.name, "initialize realtime", func() error {
			var buildErr error
			registration.server, buildErr = registration.factory()
			if buildErr != nil {
				return buildErr
			}
			return realtime.Register(registration.name, registration.server)
		})
		if err != nil {
			return fmt.Errorf("instance %q: %w", registration.name, err)
		}
	}
	webSocketInstances.initialized = true
	return nil
}

func registeredWebSocket(name string) (*realtime.Server, error) {
	for _, registration := range webSocketInstances.registrations {
		if registration.name == name && !webSocketInstances.initialized {
			return nil, fmt.Errorf("bootstrap: WebSocket %q is not initialized", name)
		}
	}
	// 未声明工厂的实例也可由应用显式 Register。
	server, ok := realtime.Lookup(name)
	if !ok {
		return nil, fmt.Errorf("bootstrap: WebSocket %q is not registered", name)
	}
	return server, nil
}

// 命名表已保证名称与实例一一对应，Service 只记录已接管的名称。
var webSocketOwners = struct {
	sync.Mutex
	names map[string]struct{}
}{
	names: make(map[string]struct{}),
}

func claimWebSocket(name string) error {
	webSocketOwners.Lock()
	defer webSocketOwners.Unlock()
	if _, exists := webSocketOwners.names[name]; exists {
		return fmt.Errorf("bootstrap: WebSocket %q already belongs to a Service", name)
	}
	webSocketOwners.names[name] = struct{}{}
	return nil
}
