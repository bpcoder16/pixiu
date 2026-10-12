package httpauth

import "context"

type identityKey struct{}

type identity struct {
	principal Principal
	scheme    string
}

// WithPrincipal 写入已验证身份及方案，供适配器或受信任应用代码使用。
// 本函数不执行认证；调用方保证身份和方案有效。按值复制，不保存可变身份指针。
func WithPrincipal(ctx context.Context, scheme string, principal Principal) context.Context {
	return context.WithValue(ctx, identityKey{}, identity{
		principal: principal,
		scheme:    scheme,
	})
}

// PrincipalFromContext 读取身份副本；没有认证结果时返回 false。
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	value, ok := ctx.Value(identityKey{}).(identity)
	return value.principal, ok
}

// SchemeFromContext 读取产生当前身份的方案名；没有认证结果时返回 false。
func SchemeFromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(identityKey{}).(identity)
	return value.scheme, ok
}
