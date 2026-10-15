package ginx

import "github.com/gin-gonic/gin"

type ignoredObservationKey struct{}

// RegisterIgnoredRoute 在启动阶段登记跳过 ginx 访问日志、两侧详情采集及 Observe 的路由。
// method、relativePath 和 handlers 沿用 Gin Handle 的约定，路径随 group 前缀调整。
// 请求日志作用域、原组和业务中间件、Recovery 及独立 panic 日志仍保留。
// 仅影响本次登记的路由，不按后缀或响应状态推断；服务开始处理请求后不再注册。
func RegisterIgnoredRoute(group *gin.RouterGroup, method, relativePath string, handlers ...gin.HandlerFunc) {
	route := group.Group("")
	// 独立中间件链在结果观察之前标记忽略，避免先预读正文，也不污染原组。
	route.Handlers = append(gin.HandlersChain{ignoreObservation}, route.Handlers...)
	route.Handle(method, relativePath, handlers...)
}

func ignoreObservation(c *gin.Context) {
	c.Set(ignoredObservationKey{}, true)
	c.Next()
}
