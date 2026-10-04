// Package netx 提供小型、可复用的网络技术功能，当前支持查询本机 IPv4 和 IPv6 地址。
//
// 查询按系统返回的网卡和地址顺序，选择已启用、非回环网卡上的首个对应版本的
// 全局单播地址，遵循 net.IP.IsGlobalUnicast 的定义：允许私有 IPv4 和 IPv6 ULA，
// 排除回环、链路本地、未指定和组播地址。VPN、容器或其他虚拟网卡同样可能被选中。
//
// “可用”仅表示满足筛选规则，不检测网络连通性，不保证公网可达，
// 也不保证是主网卡地址或访问某个目标时使用的出站地址。
// IPv4 与 IPv6 独立查询，可能来自不同网卡，也可能只有一种查询成功。
//
// 枚举网卡失败时返回错误；单个网卡读取地址失败时继续检查其他网卡。
// 最终找不到匹配地址时返回错误，不使用回环地址兜底。完整规则见 docs/netx-design.md。
//
// 以下示例需导入 fmt 和 github.com/bpcoder16/pixiu/netx：
//
//	if ipv4, err := netx.LocalIPv4(); err == nil {
//	    fmt.Println(ipv4)
//	}
//	if ipv6, err := netx.LocalIPv6(); err == nil {
//	    fmt.Println(ipv6)
//	}
package netx
