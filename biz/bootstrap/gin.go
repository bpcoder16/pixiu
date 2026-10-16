package bootstrap

import (
	"fmt"
	"path/filepath"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/gin-gonic/gin"
)

// ginConfig 只定义文件参数，运行时中间件和回调不从配置文件解析。
type ginConfig struct {
	TrustedProxies   []string `mapstructure:"trustedProxies"`
	LoggerName       string   `mapstructure:"loggerName"`
	DisableAccessLog bool     `mapstructure:"disableAccessLog"`
	LogRequestInfo   bool     `mapstructure:"logRequestInfo"`
	LogResponseInfo  bool     `mapstructure:"logResponseInfo"`
}

// NewGinEngine 从 env.ConfigDirPath() 下严格读取一次 gin.yaml 并构造独立 Engine。
// 调用前须初始化 env；不注册全局配置，运行期间不重新读取文件。
// debug/test 按 env.RunMode() 自动注册 OpenAPI 与 Elements，须先 blank import 应用生成的 Swag 文档包；
// 未注册文档时返回错误，release 不注册文档路由，也不要求生成文档。
// 可通过 modules 声明命名文档及导航顺序；不传时沿用默认 Swag 文档。
// Gin 模式、业务路由及业务中间件由应用在启动阶段显式设置；本函数不修改全局 Validator。
func NewGinEngine(modules ...ginx.OpenAPIModule) (*gin.Engine, error) {
	configFile := filepath.Join(env.ConfigDirPath(), "gin.yaml")
	config, err := configx.Parse[ginConfig](configFile)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: load Gin config: %w", err)
	}
	engine, err := ginx.New(ginx.Config{
		TrustedProxies:   config.TrustedProxies,
		LoggerName:       config.LoggerName,
		DisableAccessLog: config.DisableAccessLog,
		LogRequestInfo:   config.LogRequestInfo,
		LogResponseInfo:  config.LogResponseInfo,
	})
	if err != nil {
		return nil, fmt.Errorf("bootstrap: create Gin engine from %q: %w", configFile, err)
	}
	if env.RunMode() != env.RunModeRelease {
		if err := ginx.RegisterOpenAPI(&engine.RouterGroup, modules...); err != nil {
			return nil, fmt.Errorf("bootstrap: enable OpenAPI: generate Swag documentation (e.g. swag init -o ./openapi; use --instanceName for modules), blank import the generated package and rebuild the application: %w", err)
		}
	}
	return engine, nil
}

// NewGinHTTPService 设置 Gin 模式，读取 gin.yaml 和 http.yaml，并构造名为 http 的服务。
// 调用前须初始化 env；应在启动阶段串行调用，运行期间不得切换 Gin 全局模式。
// Engine 和文档路由创建成功后同步调用一次 registerRoutes；nil 表示不注册业务路由。
// 构造不监听端口；调用方负责登记服务，登记失败时调用返回服务的 Shutdown 清理。
func NewGinHTTPService(registerRoutes func(*gin.Engine), modules ...ginx.OpenAPIModule) (apprun.Service, error) {
	gin.SetMode(env.RunMode())
	engine, err := NewGinEngine(modules...)
	if err != nil {
		return apprun.Service{}, err
	}
	if registerRoutes != nil {
		registerRoutes(engine)
	}
	return NewHTTPService(engine)
}
