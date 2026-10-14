// Package ginx 提供独立 Gin Engine、请求日志和 Recovery。
//
// 以下示例需导入 github.com/gin-gonic/gin 和 github.com/bpcoder16/pixiu/infra/ginx：
//
//	router, err := ginx.New(ginx.Config{
//		TrustedProxies:  []string{"127.0.0.1"},
//		LogRequestInfo:  true,
//		LogResponseInfo: true,
//	})
//	if err != nil {
//		return err
//	}
//	router.POST("/items", func(c *gin.Context) {
//		// 使用 c.Request.Context() 传递日志与取消;绑定错误由业务映射响应。
//		c.Status(204)
//	})
//	// router 实现 http.Handler,交给应用选用的 HTTP 服务端。
//
// New 不修改 Gin 全局模式或 Validator,不自动注册管理端点。
// 默认开启 ContextWithFallback,兼容直接传 c 时透传请求的取消、deadline 与日志上下文。
// 业务仍推荐使用 c.Request.Context(),不要将可复用的 *gin.Context 交给后台任务。
// 默认关闭自动重定向,尾斜杠不匹配按 404 处理;默认 404/405 在结果采集前写出完整正文。
// 需要可观测的重定向时可在显式路由中调用 c.Redirect。
// 自行重新开启 Gin 自动重定向会绕过内置中间件,不产生访问日志、Observe 或日志 ID 响应头。
// 通过 TraceHeader(PIXIU-Log-Id)接收并回写日志 ID;非空值原样使用,缺失或为空时生成。
// 访问日志统一使用 logit.InfoDuration,不随状态码改变 Info 级别。
// LogRequestInfo、LogResponseInfo 默认关闭,分别追加 request_info、response_info 详情;
// DisableAccessLog 为 true 时两者均不生效,Observe 回调和独立 panic 错误日志不受影响。
// 进入观察中间件时请求 Logger 未启用 Info,本次访问日志及详情采集均关闭,中途开启不补记。
// 入口允许记录时,输出仍遵循请求结束时 Logger 的 Info 级别过滤。
// 请求详情包含 method、Header、body;响应详情包含 Header、body。
// 正文不截断、不脱敏;UTF-8 文本直接记录,二进制以 Base64 记录并标明 body_encoding。
// 请求正文在业务中间件之前完整预读并还原,读取失败会记录 body_error 并向业务保留原错误。
// 响应正文随写出复制,保留 Flush;Header 记录应用提交的快照,不承诺包含传输层自动添加的值。
// 开启详情会保存完整正文;请求体大小与读取期限由应用约束。
//
// 直接使用本包时，OpenAPI 文档与 Stoplight Elements 由调用方显式注册，New 不自动启用；先 blank import 应用生成的文档包：
//
//	if err := ginx.RegisterOpenAPI(&router.RouterGroup); err != nil {
//		return err
//	}
//
// 不传模块时，根路由组提供 /docs，页面读取默认实例的 /openapi/doc.json。
// 需要多个模块时，传入命名文档（由 swag init --instanceName 生成）：
//
//	if err := ginx.RegisterOpenAPI(&router.RouterGroup,
//		ginx.OpenAPIModule{Name: "app", Title: "App"},
//		ginx.OpenAPIModule{Name: "admin", Title: "Admin"},
//	); err != nil {
//		return err
//	}
//
// 模块模式提供 /openapi/<Name>.json、/docs/<Name> 和顶部切换导航，/docs 展示首个模块。
// 模块名非空且只允许 ASCII 字母、数字、下划线和连字符，名称重复时返回错误。
// Title 为空时使用 Name；页面文字和链接均按 HTML 转义。
// Elements 资源及加载脚本位于 /docs/assets/ 下的 web-components.min.js、styles.min.css 和 openapi.js。
// 不注册旧 /elements 路径，也不提供重定向。
// Elements 使用 hash 导航，刷新详情页不需要服务端路径回退。
// 也可传 router.Group("/internal")，文档与资源 URL 随组前缀调整；组中间件负责访问控制。
// 全部模块通过校验且读取成功后才注册路由，文档缺失时返回错误且不添加路由；生成物须由应用自行维护。
// 文档内容在注册路由时固定，应用须在此之前完成文档元信息设置。
// 页面按浏览器获取文档的地址补齐 Swagger 2.0 缺省的 host/schemes，保留 basePath 与显式配置；
// 页面还将 Swagger 2.0 的 formData/file 参数适配为 string/binary，启用文件选择控件；
// 覆盖操作级、路径级及根级公共参数定义，保留 consumes、必填标记和文档内引用。
// 上传接口继续使用 @Accept mpfd 和 @Param file formData file true "上传文件"。
// JSON 端点仍返回原始文档，补齐及文件适配仅影响页面内的副本；OpenAPI 3 保持原样，外部引用文件不做适配。
// Elements 的静态资源均内嵌，不依赖运行时 CDN。
// Elements 沿用上游默认认证持久化，会在浏览器 localStorage 保存并恢复认证输入。
// Swagger 2.0 的 Authorization apiKey 方案可用扩展 x-pixiu-bearer: true（也接受字符串 "true"）启用前缀补齐。
// Swag 对应注释为 @x-pixiu-bearer true；仅影响本项目 Elements 的调试请求与代码示例。
// 输入纯 Token 或完整 Bearer Header 均可，空输入不发送 Authorization；其他 API Key 保持原样。
// 文档请求沿用所在 Engine 的日志策略；LogResponseInfo 会复制并记录完整静态资源，建议关闭。
package ginx
