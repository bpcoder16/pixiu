// Package ginauth 将命名认证方案接入 Gin，负责身份传递和失败请求的中止。
//
// 假设 appAuthenticator 和 adminAuthenticator 已实现 httpauth.Authenticator：
//
//	registry := httpauth.NewRegistry()
//	registry.MustRegister("app", appAuthenticator)
//	registry.MustRegister("admin", adminAuthenticator)
//	guard := ginauth.New(registry, ginauth.WithFailureHandler(func(c *gin.Context, err error) {
//		if errors.Is(err, httpauth.ErrUnauthenticated) {
//			c.Status(http.StatusUnauthorized)
//			return
//		}
//		c.Status(http.StatusInternalServerError)
//	}))
//	router := gin.New()
//	router.GET("/app/me", guard.Require("app"), func(c *gin.Context) {
//		principal, _ := httpauth.PrincipalFromContext(c.Request.Context())
//		c.JSON(http.StatusOK, gin.H{"userId": principal.UserID})
//	})
//	router.Group("/admin", guard.Require("admin"))
//
// 示例需导入 errors、net/http、github.com/gin-gonic/gin，以及 Pixiu 的
// biz/httpauth 和 biz/httpauth/ginauth。注册应在首次 Require 之前完成，未知方案构造时 panic。
// 应用也可在启动阶段安装默认实例，路由无需逐层传递 guard：
//
//	ginauth.SetDefault(guard)
//	router.Group("/app", ginauth.Default().Require("app"))
//
// SetDefault 拒绝 nil，Default 在未初始化时 panic。应在构造应用路由前安装默认实例；
// 已绑定路由不会随默认值变化，不支持运行时热切换。使用默认值的测试须串行组装路由。
// New 始终创建独立实例，不改变默认值，适合测试或同进程内需要独立配置的 HTTP 服务。
// 认证器通过标准 *http.Request 读取凭证及 Context；实现和失败回调均须并发安全。
// 失败回调执行前已 Abort，不需要重复中止请求；错误和身份同时返回时拒绝访问。
// 默认无效凭证为 401，运行错误为 500，均为空正文。
// 认证器负责身份有效性，成功时须返回非 nil 的 Principal；中间件原样传递身份。
// 本包不要求 ginx 日志作用域；请求恢复、访问日志和授权策略由应用另行组装。
package ginauth
