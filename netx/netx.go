package netx

import (
	"fmt"
	"net"
)

// LocalIPv4 按系统顺序返回已启用、非回环网卡上的首个 IPv4 全局单播地址。
// 结果可以是私有地址，也可能来自 VPN 或容器网卡；不保证是主网卡或默认出站地址，
// 不检测连通性，也不保证公网可达。查询失败或无匹配地址时返回空字符串及错误。
func LocalIPv4() (string, error) {
	return localIP(4, net.Interfaces, (*net.Interface).Addrs)
}

// LocalIPv6 按系统顺序返回已启用、非回环网卡上的首个 IPv6 全局单播地址。
// 结果可以是 ULA，也可能来自 VPN 或容器网卡；不保证是主网卡或默认出站地址，
// 不检测连通性，也不保证公网可达。查询失败或无匹配地址时返回空字符串及错误。
func LocalIPv6() (string, error) {
	return localIP(6, net.Interfaces, (*net.Interface).Addrs)
}

func localIP(
	family int,
	listInterfaces func() ([]net.Interface, error),
	listAddrs func(*net.Interface) ([]net.Addr, error),
) (string, error) {
	interfaces, err := listInterfaces()
	if err != nil {
		return "", fmt.Errorf("netx: list network interfaces: %w", err)
	}

	var addressErr error
	for i := range interfaces {
		iface := &interfaces[i]
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := listAddrs(iface)
		if err != nil {
			// 单个网卡读取失败时，其他网卡仍可能有可用地址。
			addressErr = fmt.Errorf("netx: list addresses of %s: %w", iface.Name, err)
			continue
		}
		if ip := firstUsableIP(addrs, family); ip != nil {
			return ip.String(), nil
		}
	}

	if addressErr != nil {
		return "", fmt.Errorf("netx: no usable IPv%d address: %w", family, addressErr)
	}
	return "", fmt.Errorf("netx: no usable IPv%d address", family)
}

func firstUsableIP(addrs []net.Addr, family int) net.IP {
	for _, addr := range addrs {
		var ip net.IP
		switch value := addr.(type) {
		case *net.IPNet:
			ip = value.IP
		case *net.IPAddr:
			ip = value.IP
		default:
			continue
		}
		if !ip.IsGlobalUnicast() {
			continue
		}
		if family == 4 {
			if ip4 := ip.To4(); ip4 != nil {
				return ip4
			}
			continue
		}
		if ip.To4() == nil {
			return ip
		}
	}
	return nil
}
