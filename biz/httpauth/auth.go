package httpauth

import (
	"errors"
	"net/http"
)

// Principal 是认证成功后的用户身份；业务结合认证方案解释 UserID 所属的账号体系。
type Principal struct {
	UserID string
}

// ErrUnauthenticated 表示缺少或无效凭证，支持 errors.Is 判断。
var ErrUnauthenticated = errors.New("auth: unauthenticated")

// Authenticator 提取并验证请求凭证，确认身份后才返回 Principal。
// 成功时须返回非 nil 的已验证身份；UserID 的有效性由实现保证。
// 实现须支持并发，通过 r.Context() 传递取消和期限；运行错误应保留原始错误链。
type Authenticator interface {
	Authenticate(r *http.Request) (*Principal, error)
}

// AuthenticatorFunc 将函数适配为认证器。
type AuthenticatorFunc func(*http.Request) (*Principal, error)

func (f AuthenticatorFunc) Authenticate(r *http.Request) (*Principal, error) {
	return f(r)
}
