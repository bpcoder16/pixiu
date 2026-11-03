package bootstrap_test

import (
	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/lifecycle"
)

// 先准备 app.yaml 和 websocket.app.yaml、websocket.web.yaml、websocket.admin.yaml 的不同端口。
// 示例共用业务定义；实际业务可为每个端使用不同工厂，分别设置认证、状态及连接限额。
func ExampleNewWebSocketService() {
	newServer := func() (*realtime.Server, error) {
		return realtime.NewServer(exampleRealtimeConfig())
	}
	config := appconfig.MustLoadAppConfig("./conf/app.yaml")
	var resources lifecycle.Stack
	defer resources.Close()
	bootstrap.MustRegisterWebSocket("app", newServer)
	bootstrap.MustRegisterWebSocket("web", newServer)
	bootstrap.MustRegisterWebSocket("admin", newServer)
	bootstrap.MustBaseInit(config, &resources)

	err := bootstrap.Start(
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("app")
		},
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("web")
		},
		func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("admin")
		},
	)
	if err != nil {
		panic(err)
	}
}
