package netx

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
)

func TestLocalIP(t *testing.T) {
	t.Parallel()

	listErr := errors.New("list interfaces failed")
	addressErr := errors.New("list addresses failed")
	down := net.Interface{
		Index: 1,
		Name:  "down",
	}
	loopback := net.Interface{
		Index: 2,
		Name:  "loopback",
		Flags: net.FlagUp | net.FlagLoopback,
	}
	otherFamily := net.Interface{
		Index: 3,
		Name:  "other-family",
		Flags: net.FlagUp,
	}
	firstMatch := net.Interface{
		Index: 4,
		Name:  "first-match",
		Flags: net.FlagUp,
	}
	laterMatch := net.Interface{
		Index: 5,
		Name:  "later-match",
		Flags: net.FlagUp,
	}
	broken := net.Interface{
		Index: 6,
		Name:  "broken",
		Flags: net.FlagUp,
	}
	empty := net.Interface{
		Index: 7,
		Name:  "empty",
		Flags: net.FlagUp,
	}
	for _, family := range []int{4, 6} {
		match, other := "10.0.0.2", "fd12::1"
		if family == 6 {
			match, other = other, match
		}
		matchingAddrs := []net.Addr{&net.IPNet{IP: net.ParseIP(match)}}
		otherAddrs := []net.Addr{&net.IPNet{IP: net.ParseIP(other)}}
		noMatchErr := fmt.Sprintf("no usable IPv%d address", family)

		for _, tc := range []struct {
			name        string
			interfaces  []net.Interface
			listErr     error
			addresses   map[int][]net.Addr
			addressErrs map[int]error
			wantQueries []int
			wantIP      string
			wantErr     string
			wantCause   error
		}{
			{
				name:       "skip_ineligible_and_stop_at_first_match",
				interfaces: []net.Interface{down, loopback, otherFamily, firstMatch, laterMatch},
				addresses: map[int][]net.Addr{
					otherFamily.Index: otherAddrs,
					firstMatch.Index:  matchingAddrs,
				},
				wantQueries: []int{otherFamily.Index, firstMatch.Index},
				wantIP:      match,
			},
			{
				name:       "continue_after_address_error",
				interfaces: []net.Interface{broken, firstMatch},
				addresses: map[int][]net.Addr{
					firstMatch.Index: matchingAddrs,
				},
				addressErrs: map[int]error{
					broken.Index: addressErr,
				},
				wantQueries: []int{broken.Index, firstMatch.Index},
				wantIP:      match,
			},
			{
				name:      "interface_enumeration_error",
				listErr:   listErr,
				wantErr:   "list network interfaces",
				wantCause: listErr,
			},
			{
				name:    "no_interfaces",
				wantErr: noMatchErr,
			},
			{
				name:       "no_eligible_interfaces",
				interfaces: []net.Interface{down, loopback},
				wantErr:    noMatchErr,
			},
			{
				name:       "no_matching_address",
				interfaces: []net.Interface{empty, otherFamily},
				addresses: map[int][]net.Addr{
					empty.Index:       nil,
					otherFamily.Index: otherAddrs,
				},
				wantQueries: []int{empty.Index, otherFamily.Index},
				wantErr:     noMatchErr,
			},
			{
				name:       "preserve_address_error_when_no_match",
				interfaces: []net.Interface{broken, otherFamily},
				addresses: map[int][]net.Addr{
					otherFamily.Index: otherAddrs,
				},
				addressErrs: map[int]error{
					broken.Index: addressErr,
				},
				wantQueries: []int{broken.Index, otherFamily.Index},
				wantErr:     noMatchErr + ": netx: list addresses of broken",
				wantCause:   addressErr,
			},
		} {
			t.Run(fmt.Sprintf("IPv%d/%s", family, tc.name), func(t *testing.T) {
				t.Parallel()

				var queries []int
				ip, err := localIP(family, func() ([]net.Interface, error) {
					return tc.interfaces, tc.listErr
				}, func(iface *net.Interface) ([]net.Addr, error) {
					queries = append(queries, iface.Index)
					if err := tc.addressErrs[iface.Index]; err != nil {
						return nil, err
					}
					addrs, ok := tc.addresses[iface.Index]
					if !ok {
						t.Fatalf("unexpected address query for interface %s", iface.Name)
					}
					return addrs, nil
				})
				if ip != tc.wantIP {
					t.Fatalf("localIP() address = %q, want %q", ip, tc.wantIP)
				}
				if tc.wantErr == "" {
					if err != nil {
						t.Fatalf("localIP() error = %v, want nil", err)
					}
				} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("localIP() error = %v, want containing %q", err, tc.wantErr)
				}
				if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
					t.Fatalf("localIP() error = %v, want wrapping %v", err, tc.wantCause)
				}
				if !slices.Equal(queries, tc.wantQueries) {
					t.Fatalf("queried interfaces = %v, want %v", queries, tc.wantQueries)
				}
			})
		}
	}
}

func TestLocalIPSystemInterfaces(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query func() (string, error)
		ipv4  bool
	}{
		{
			name:  "IPv4",
			query: LocalIPv4,
			ipv4:  true,
		},
		{
			name:  "IPv6",
			query: LocalIPv6,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			value, err := tc.query()
			if err != nil {
				if value != "" {
					t.Fatalf("address on error = %q, want empty", value)
				}
				// 测试机可能没有某一地址族，或不允许枚举网卡。
				t.Logf("local address unavailable: %v", err)
				return
			}
			ip := net.ParseIP(value)
			if ip == nil || !ip.IsGlobalUnicast() || (ip.To4() != nil) != tc.ipv4 {
				t.Fatalf("address = %q, want global unicast %s", value, tc.name)
			}
		})
	}
}

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
