// Package bootstrap 根据应用配置创建独立的 Gin Engine。
//
// 在 env.Init 成功后构造服务，debug/test 环境需提前导入生成的 Swag 文档包。
//
//	func newHTTPServer() (*httpserver.Server, error) {
//		gin.SetMode(env.RunMode())
//		engine, err := bootstrap.NewGinEngine()
//		if err != nil {
//			return nil, err
//		}
//		engine.GET("/healthz", func(c *gin.Context) {
//			c.Status(http.StatusNoContent)
//		})
//		return httpserver.New(httpserver.Config{Addr: ":8888"}, engine)
//	}
//
// NewGinEngine 从 env.ConfigDirPath() 下的 gin.yaml 读取一次配置，再调用 ginx.New。
// 文件必需，{} 沿用默认值：不信任代理、记录访问日志、不记录请求或响应详情。
// 支持 trustedProxies、loggerName、disableAccessLog、logRequestInfo、logResponseInfo；
// 不接受运行时回调，不注册全局配置或重读文件；缺失、未知字段、错误类型和非法代理均返回错误。
// 模板见 conf.example/gin.yaml。
// Engine 创建后仅按 env.RunMode() 决定文档路由：debug/test 调用 ginx.RegisterOpenAPI，release 跳过。
// 不传参数时提供默认文档 /openapi/doc.json、/docs 和内嵌静态资源。
// 需要多个模块时，由应用显式声明名称和标题（另需导入 infra/ginx）：
//
//	engine, err := bootstrap.NewGinEngine(
//		ginx.OpenAPIModule{Name: "app", Title: "App"},
//		ginx.OpenAPIModule{Name: "admin", Title: "Admin"},
//	)
//
// 此时每个模块提供 /openapi/<Name>.json 与 /docs/<Name>，/docs 展示首个模块。
// 页面共用静态资源并提供顶部模块导航；模块列表由代码提供，不读取独立文档配置，不自动增加认证。
// 应用负责生成文档并 blank import 注册同名 Swag 文档实例，元信息须在构造前设置；
// 多模块使用 swag init 的 --instanceName 与 --tags 等参数分别生成所需文档。
// 生成目录由应用决定，例如 swag init -o ./openapi。debug/test 未注册文档时返回 nil Engine 和错误，
// 提示生成、导入文档包并保留底层错误链；release 不要求生成文档。
// 文档路由在构造时已注册，之后添加的业务中间件不作用于这些路由。
// 需要自定义文档注册策略的应用可直接使用 ginx.New 和 ginx.RegisterOpenAPI。
// 不修改 Gin 全局模式或 Validator，不监听端口或自动注册业务路由。应用构造一次后持有 Engine，
// 完成业务路由和中间件注册，将 Engine 作为 http.Handler 传给 httpserver.New；模式由应用启动时显式设置。
// disableAccessLog 覆盖详情开关；详情完整记录且不脱敏，开启前须在 Engine 外层约束请求体规模及读取期限。
// 全部文档 GET 路由自动忽略访问日志、详情及 Observe，保留 Recovery。
// 包括文档页面、首页别名、JSON 端点和三个内嵌静态资源。
package bootstrap
