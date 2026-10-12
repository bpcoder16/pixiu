// Package httpauth 提供命名 HTTP 认证方案、统一用户身份和标准 Context 传递。
// 具体 JWT、Session 或其他凭证的提取、验证和撤销检查由应用实现。
//
// 假设 appAuthenticator 和 adminAuthenticator 已实现 Authenticator：
//
//	registry := httpauth.NewRegistry()
//	registry.MustRegister("app", appAuthenticator)
//	registry.MustRegister("admin", adminAuthenticator)
//	app, err := registry.Resolve("app")
//	if err != nil {
//		return err
//	}
//	principal, err := app.Authenticate(request)
//	if err != nil {
//		return err
//	}
//	_ = principal
//
// 示例需导入 github.com/bpcoder16/pixiu/biz/httpauth；request 为 *http.Request。
// Resolve 首次调用即冻结注册表，应在路由构造前完成所有注册。
// Gin 中间件由子包 ginauth 提供，按认证器返回的错误中止请求，成功时将身份注入 Context。
// 业务通过 PrincipalFromContext 和 SchemeFromContext 读取身份及账号体系。
// Principal 按值复制，只有 UserID；角色、权限、登录、签发和刷新留在业务层。
// 没有包级默认实例，不自动尝试多个方案，也不缓存认证结果或持有资源关闭责任。
package httpauth
