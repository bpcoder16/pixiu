package bootstrap

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

// NewHTTPService 从 env.ConfigDirPath() 下严格读取 http.yaml 并构造名为 http 的服务。
// 调用前须初始化 env；监听在 Run 时才启动。
// 调用方负责注册；注册失败时通过 Stop、Wait 与 Cancel 清理。
// HTTP 排空期限由管理器统一提供，组件不设置独立关闭期限。
func NewHTTPService(handler http.Handler) (apprun.Service, error) {
	configFile := filepath.Join(env.ConfigDirPath(), "http.yaml")
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
		Stop:   server.Stop,
		Wait:   server.Wait,
		Cancel: server.Cancel,
	}, nil
}
