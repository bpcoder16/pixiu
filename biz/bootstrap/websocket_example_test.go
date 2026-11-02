package bootstrap_test

import (
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
)

// 先初始化 env/日志，并配置 websocket.app.yaml、websocket.web.yaml、websocket.admin.yaml 的不同端口。
// 示例共用业务定义；实际业务可为每个端使用不同工厂，分别设置认证、状态及连接限额。
func ExampleNewWebSocketService() {
	newServer := func() (*realtime.Server, error) {
		return realtime.NewServer(exampleRealtimeConfig())
	}
	err := bootstrap.Start(
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("app", newServer)
		},
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("web", newServer)
		},
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("admin", newServer)
		},
	)
	if err != nil {
		panic(err)
	}
}
