package validx

import (
	"net/mail"
	"strings"
)

// IsEmail 检查常用的 ASCII 邮箱格式，不验证邮箱是否存在。
// 仅接受未加引号的 dot-atom 本地部分和至少两段的 ASCII 域名；
// 本地部分最多 64 字节，完整地址最多 254 字节。
// 不接受显示名称、注释、地址列表、IP 字面量域名或原始 Unicode 邮箱。
func IsEmail(s string) bool {
	if len(s) == 0 || len(s) > 254 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] >= 128 {
			return false
		}
	}
	local, domain, ok := strings.Cut(s, "@")
	if !ok || len(local) == 0 || len(local) > 64 || strings.ContainsAny(local, "\"\\") {
		return false
	}
	if !strings.Contains(domain, ".") || !validHostname(domain) {
		return false
	}
	address, err := mail.ParseAddress(s)
	// 邮件头解析器允许显示名称和注释；必须保证它没有清理或提取输入中的地址。
	return err == nil && address.Name == "" && address.Address == s
}
