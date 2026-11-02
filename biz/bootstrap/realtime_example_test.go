package bootstrap_test

import (
	"context"
	"net/http"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/gin-gonic/gin"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func exampleRealtimeConfig() realtime.Config {
	return realtime.Config{
		Authenticate: func(context.Context, *http.Request) (realtime.ClientInfo, error) {
			// 仅供示例：业务应在此校验凭据并提取可信身份。
			return realtime.ClientInfo{}, nil
		},
		OnMessage: func(ctx context.Context, session *realtime.Session, message *realtime.Message) error {
			var request wrapperspb.StringValue
			if err := message.Decode(&request); err != nil {
				return session.ReplyError(ctx, message, "INVALID_ARGUMENT", "请求参数错误")
			}
			return session.Reply(ctx, message, wrapperspb.String("hello "+request.Value))
		},
	}
}

// 应用先初始化 env/日志并提供 http.yaml，再调用此启动链路。
func Example_realtimeServeMux() {
	var server *realtime.Server
	err := bootstrap.Start(
		func() (apprun.Service, error) {
			var err error
			server, err = realtime.NewServer(exampleRealtimeConfig())
			if err != nil {
				return apprun.Service{}, err
			}
			return apprun.Service{
				Name:   "realtime:app",
				Run:    server.Run,
				Stop:   server.Stop,
				Wait:   server.Wait,
				Cancel: server.Cancel,
			}, nil
		},
		func() (apprun.Service, error) {
			mux := http.NewServeMux()
			mux.Handle("GET /ws", server)
			return bootstrap.NewHTTPService(mux)
		},
	)
	if err != nil {
		panic(err)
	}
}

// 共用 Gin 端口时还需 gin.yaml；业务普通路由在同一工厂内注册。
func Example_realtimeGin() {
	var server *realtime.Server
	err := bootstrap.Start(
		func() (apprun.Service, error) {
			var err error
			server, err = realtime.NewServer(exampleRealtimeConfig())
			if err != nil {
				return apprun.Service{}, err
			}
			return apprun.Service{
				Name:   "realtime:app",
				Run:    server.Run,
				Stop:   server.Stop,
				Wait:   server.Wait,
				Cancel: server.Cancel,
			}, nil
		},
		func() (apprun.Service, error) {
			return bootstrap.NewGinHTTPService(func(engine *gin.Engine) {
				engine.GET("/ready", func(c *gin.Context) {
					if !server.Ready() {
						c.Status(http.StatusServiceUnavailable)
						return
					}
					c.Status(http.StatusNoContent)
				})
				ginx.RegisterIgnoredRoute(
					&engine.RouterGroup,
					http.MethodGet,
					"/ws/app",
					gin.WrapH(server),
				)
			})
		},
	)
	if err != nil {
		panic(err)
	}
}
