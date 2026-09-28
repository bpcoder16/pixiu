// Package netx 提供小型、可复用的网络技术功能，当前支持查询本机 IPv4 和 IPv6 地址。
// 地址选择规则见 docs/netx-design.md。
//
// 以下示例需导入 fmt 和 github.com/bpcoder16/pixiu/netx：
//
//	if ipv4, err := netx.LocalIPv4(); err == nil {
//	    fmt.Println(ipv4)
//	}
//	if ipv6, err := netx.LocalIPv6(); err == nil {
//	    fmt.Println(ipv6)
//	}
//
// 一台机器可能有多个可用地址；方法按系统返回的网卡和地址顺序选择首个地址。
// 找不到对应版本的地址时返回错误。
package netx
