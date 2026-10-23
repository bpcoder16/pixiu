package validx

import (
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// IsIP 检查纯 IPv4 或 IPv6 地址，不接受端口、CIDR、方括号或 zone。
func IsIP(s string) bool {
	addr, err := netip.ParseAddr(s)
	return err == nil && addr.Zone() == ""
}

// IsIPv4 检查点分十进制 IPv4 地址，不接受前导零或 IPv4 映射 IPv6。
func IsIPv4(s string) bool {
	addr, err := netip.ParseAddr(s)
	return err == nil && addr.Is4()
}

// IsIPv6 检查无 zone 的纯 IPv6 地址，IPv4 映射形式仍归 IPv6。
func IsIPv6(s string) bool {
	addr, err := netip.ParseAddr(s)
	return err == nil && addr.Is6() && addr.Zone() == ""
}

// IsHTTPURL 检查绝对 HTTP/HTTPS 地址格式，不验证可达性或访问权限。
// 主机支持 ASCII 域名、单段主机名、IPv4 和方括号 IPv6（不含 zone），
// 主机名可带一个尾部根点；显式端口须为十进制 1～65535。
// 接受标准库支持的 userinfo、Unicode 路径、查询和 fragment；
// 拒绝非法 UTF-8、未转义空白或控制字符，以及不完整或非法的百分号转义。
func IsHTTPURL(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return false
	}
	host := u.Hostname()
	if strings.HasPrefix(u.Host, "[") {
		if !IsIPv6(host) {
			return false
		}
	} else {
		// IPv4 必须以数字开头；跳过普通域名的 IP 解析，也不重复扫描合法 IPv4。
		ipv4 := host != "" && host[0] >= '0' && host[0] <= '9' && IsIPv4(host)
		// 数字地址必须符合 IPv4 语法，不能让畸形或简写 IPv4 回退为主机名。
		if !ipv4 && (strings.Trim(host, "0123456789.") == "" || !validHostname(strings.TrimSuffix(host, "."))) {
			return false
		}
	}
	if strings.HasSuffix(u.Host, ":") {
		return false
	}
	if port := u.Port(); port != "" {
		value, portErr := strconv.ParseUint(port, 10, 16)
		if portErr != nil || value == 0 {
			return false
		}
	}
	// Parse 不检查 RawQuery 转义；只校验格式，避免为未使用的解码结果分配内存。
	for query := u.RawQuery; ; {
		i := strings.IndexByte(query, '%')
		if i < 0 {
			return true
		}
		if i+2 >= len(query) || !isHex(query[i+1]) || !isHex(query[i+2]) {
			return false
		}
		query = query[i+3:]
	}
}

func isHex(c byte) bool {
	return c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'
}

// validHostname 检查无尾部根点的 ASCII 主机名；是否要求多段由调用方决定。
func validHostname(s string) bool {
	if len(s) == 0 || len(s) > 253 {
		return false
	}
	for label := range strings.SplitSeq(s, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' {
				continue
			}
			return false
		}
	}
	return true
}
