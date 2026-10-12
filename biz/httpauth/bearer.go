package httpauth

import (
	"net/http"
	"strings"
)

// BearerToken 从唯一的 Authorization Header 提取凭证，不执行验证。
// scheme 大小写不敏感；重复头和多余字段均拒绝，不读取查询参数或 Cookie。
func BearerToken(r *http.Request) (string, error) {
	headers := r.Header.Values("Authorization")
	if len(headers) != 1 {
		return "", ErrUnauthenticated
	}
	parts := strings.Fields(headers[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", ErrUnauthenticated
	}
	return parts[1], nil
}
