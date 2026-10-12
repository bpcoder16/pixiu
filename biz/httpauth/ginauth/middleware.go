package ginauth

import (
	"errors"
	"net/http"

	"github.com/bpcoder16/pixiu/biz/httpauth"
	"github.com/gin-gonic/gin"
)

// FailureHandler 在请求已中止后同步处理认证错误，须支持并发调用。
// 可自定义响应和日志；不应向客户端输出内部错误或凭证。
type FailureHandler func(*gin.Context, error)

type Option func(*Middleware)

// Middleware 将命名认证方案适配为 Gin 中间件，构造后可并发使用。
type Middleware struct {
	registry  *httpauth.Registry
	onFailure FailureHandler
}

// WithFailureHandler 自定义错误响应，nil 回调在 New 时 panic。
func WithFailureHandler(handler FailureHandler) Option {
	return func(m *Middleware) {
		if handler == nil {
			panic("ginauth: nil failure handler")
		}
		m.onFailure = handler
	}
}

// New 创建适配器；nil 注册表或 Option 在构造时 panic。
// 默认未认证返回空正文 401，其他错误返回空正文 500。
func New(registry *httpauth.Registry, options ...Option) *Middleware {
	if registry == nil {
		panic("ginauth: nil registry")
	}
	m := &Middleware{
		registry:  registry,
		onFailure: defaultFailureHandler,
	}
	for _, option := range options {
		if option == nil {
			panic("ginauth: nil option")
		}
		option(m)
	}
	return m
}

// Require 在路由构造时解析并绑定方案，未知方案 panic，首次解析冻结注册表。
// 每个请求只调用所选实现；失败时先中止请求再执行错误回调。
func (m *Middleware) Require(scheme string) gin.HandlerFunc {
	authenticator, err := m.registry.Resolve(scheme)
	if err != nil {
		panic(err)
	}
	// 捕获构造时的策略，请求过程中不再访问注册表或可变选项。
	onFailure := m.onFailure
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		principal, authErr := authenticator.Authenticate(c.Request)
		if authErr != nil {
			c.Abort()
			onFailure(c, authErr)
			return
		}
		c.Request = c.Request.WithContext(httpauth.WithPrincipal(ctx, scheme, *principal))
		c.Next()
	}
}

func defaultFailureHandler(c *gin.Context, err error) {
	status := http.StatusInternalServerError
	if errors.Is(err, httpauth.ErrUnauthenticated) {
		status = http.StatusUnauthorized
	}
	c.Status(status)
}
