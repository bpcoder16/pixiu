package bootstrap

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

// NewHTTPService 严格读取配置并构造名为 http 的服务，监听在 Run 时才启动。
// 调用方负责注册；注册失败时调用返回服务的 Shutdown 清理。
// HTTP 关闭期限由配置指定，零值默认 15 秒，超时后不额外等待 handler。
func NewHTTPService(configFile string, handler http.Handler) (apprun.Service, error) {
	config, err := configx.Parse[httpserver.Config](configFile)
	if err != nil {
		return apprun.Service{}, fmt.Errorf("bootstrap: load HTTP config: %w", err)
	}
	server, err := httpserver.New(*config, handler)
	if err != nil {
		return apprun.Service{}, fmt.Errorf("bootstrap: create HTTP server: %w", err)
	}
	return apprun.Service{
		Name: "http",
		Run: func() error {
			runErr := server.Run()
			// 停机可先于 Run 进入；适配预期关闭错误，保留真实监听和排空错误。
			if errors.Is(runErr, httpserver.ErrClosed) {
				return nil
			}
			return runErr
		},
		Shutdown: server.Shutdown,
	}, nil
}
