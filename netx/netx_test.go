package netx

import (
	"net"
	"testing"
)

func TestFirstUsableIP(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("127.0.0.1")},
		&net.IPNet{IP: net.ParseIP("169.254.1.2")},
		&net.IPNet{IP: net.ParseIP("::1")},
		&net.IPNet{IP: net.ParseIP("fe80::1")},
		&net.IPNet{IP: net.ParseIP("10.0.0.2")},
		&net.IPNet{IP: net.ParseIP("fd12::1")},
		&net.IPNet{IP: net.ParseIP("192.168.1.5")},
		&net.IPNet{IP: net.ParseIP("2001:db8::1")},
	}
	for _, tc := range []struct {
		name   string
		family int
		want   string
	}{
		{name: "IPv4", family: 4, want: "10.0.0.2"},
		{name: "IPv6", family: 6, want: "fd12::1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ip := firstUsableIP(addrs, tc.family)
			if ip == nil || ip.String() != tc.want {
				t.Fatalf("firstUsableIP() = %v, want %s", ip, tc.want)
			}
		})
	}
}

func TestFirstUsableIPNoMatch(t *testing.T) {
	addrs := []net.Addr{
		&net.IPNet{IP: net.ParseIP("0.0.0.0")},
		&net.IPNet{IP: net.ParseIP("224.0.0.1")},
		&net.IPNet{IP: net.ParseIP("fe80::1")},
		&net.IPNet{IP: net.ParseIP("::1")},
	}
	for _, family := range []int{4, 6} {
		if ip := firstUsableIP(addrs, family); ip != nil {
			t.Fatalf("firstUsableIP(family=%d) = %v, want nil", family, ip)
		}
	}
}

func TestFirstUsableIPAcceptsIPAddr(t *testing.T) {
	addrs := []net.Addr{&net.IPAddr{IP: net.ParseIP("2001:db8::2")}}
	if ip := firstUsableIP(addrs, 6); ip == nil || ip.String() != "2001:db8::2" {
		t.Fatalf("firstUsableIP() = %v, want 2001:db8::2", ip)
	}
}
