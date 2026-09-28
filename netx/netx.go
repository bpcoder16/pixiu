package netx

import (
	"fmt"
	"net"
)

// LocalIPv4 返回首个已启用、非回环网卡上的可用 IPv4 地址。
func LocalIPv4() (string, error) {
	return localIP(4)
}

// LocalIPv6 返回首个已启用、非回环网卡上的可用 IPv6 地址。
func LocalIPv6() (string, error) {
	return localIP(6)
}

func localIP(family int) (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("netx: list network interfaces: %w", err)
	}

	var addressErr error
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
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
