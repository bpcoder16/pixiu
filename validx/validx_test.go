package validx

import (
	"net/url"
	"strings"
	"testing"
	"unicode/utf8"
)

func checkStrings(t *testing.T, check func(string) bool, want bool, inputs []string) {
	t.Helper()
	for _, s := range inputs {
		if got := check(s); got != want {
			t.Errorf("输入 %q: 得到 %t，期望 %t", s, got, want)
		}
	}
}

func TestIsMobileCN(t *testing.T) {
	t.Parallel()
	checkStrings(t, IsMobileCN, true, []string{
		"13800138000",
		"19912345678",
		"14000000000", // 仅做格式初筛，不查询号段是否已分配。
	})
	checkStrings(t, IsMobileCN, false, []string{
		"", "1380013800", "138001380000", "23800138000", "12800138000",
		"+8613800138000", "8613800138000", "138 0013 8000", "138-0013-8000",
		" 13800138000", "13800138000\n", "1380013800a", "１３８００１３８０００",
		"1380013800\xff", "1380013800\x00",
	})
}

func TestIsDigits(t *testing.T) {
	t.Parallel()
	checkStrings(t, IsDigits, true, []string{"0", "00123", "1234567890"})
	checkStrings(t, IsDigits, false, []string{
		"", " 1", "1 ", "+1", "-1", "1.0", "1e3", "1_000", "1\n", "１２３", "١٢٣", "1\xff",
	})
}

func TestIsEmail(t *testing.T) {
	t.Parallel()
	label := strings.Repeat("a", 63)
	local := strings.Repeat("u", 64)
	// 完整地址恰好 254 字节，同时覆盖本地部分与域名段长度上限。
	longest := local + "@" + label + "." + label + "." + strings.Repeat("b", 61)
	checkStrings(t, IsEmail, true, []string{
		"user@example.com", "User.Name+tag@EXAMPLE.CO.UK", "a@b.c",
		"!#$%&'*+-/=?^_`{|}~@example.com", "a.b@sub-domain.example",
		"user@xn--fsqu00a.xn--0zwm56d", longest,
	})
	checkStrings(t, IsEmail, false, []string{
		"", "user", "@example.com", "user@", "user@@example.com", "user@localhost",
		" user@example.com", "user@example.com ", "user@example.com\n", "us\ter@example.com",
		"Alice <alice@example.com>", "<alice@example.com>", "a@example.com (Alice)",
		"a(comment)@example.com", "a@(comment)example.com", "a@example.com,b@example.com",
		"\"user\"@example.com", "\"a b\"@example.com", "\"\"@example.com", "a\\b@example.com",
		".a@example.com", "a.@example.com", "a..b@example.com",
		"a@.example.com", "a@example..com", "a@example.com.",
		"a@-example.com", "a@example-.com", "a@exam_ple.com",
		"a@[127.0.0.1]", "a@[IPv6:::1]", "用户@example.com", "a@例子.中国",
		"a\x00@example.com", "a\xff@example.com", "a@exam\x7fple.com",
		strings.Repeat("u", 65) + "@example.com",
		"a@" + strings.Repeat("b", 64) + ".com", longest + "b",
	})
}

func TestIsHTTPURL(t *testing.T) {
	t.Parallel()
	label := strings.Repeat("a", 63)
	longestHost := label + "." + label + "." + label + "." + strings.Repeat("b", 61)
	checkStrings(t, IsHTTPURL, true, []string{
		"http://example.com", "HTTPS://EXAMPLE.COM/path?q=value#part",
		"http://localhost", "http://my-host/", "http://example.com./",
		"http://123.example/", "http://127.0.0.1.example/", "http://123abc/",
		"http://127.0.0.1:8080/", "http://10.0.0.1", "http://0.0.0.0",
		"https://[2001:db8::1]/", "http://[::1]:1/", "http://[::ffff:192.0.2.1]:65535",
		"http://example.com:00080/", "http://user:pass@example.com/",
		"http://example.com/中文?q=查询#片段", "http://xn--fsqu00a.xn--0zwm56d/",
		"http://example.com/a%20b?x=%2f+%20#%23", "http://example.com/?x=a;b&x=c",
		"http://example.com/?", "http://example.com/#", "http://" + longestHost + ".",
	})
	checkStrings(t, IsHTTPURL, false, []string{
		"", "example.com", "/path", "//example.com", "ftp://example.com", "http:example.com",
		"http:///path", "http://", "http://:80", "http://?q=x", "http://#x",
		"http://example.com:", "http://example.com:0", "http://example.com:65536",
		"http://example.com:999999999999999999999999", "http://example.com:http",
		"http://example.com:+80", "http://example.com:-1", "http://example.com:８０",
		"http://[::1]:", "http://[::1]:0", "http://[::1]:65536",
		"http://::1", "http://[127.0.0.1]", "http://[example.com]", "http://[v1.a]",
		"http://[fe80::1%25en0]", "http://[::1", "http://[::1]extra",
		"http://256.1.1.1", "http://127.000.0.1", "http://127.1", "http://2130706433",
		"http://123", "http://127.0.0.1.", "http://.",
		"http://.example.com", "http://example..com", "http://example.com..",
		"http://-example.com", "http://example-.com", "http://exam_ple.com",
		"http://例子.中国", "http://%65xample.com", "http://" + strings.Repeat("a", 64) + ".com",
		"http://" + longestHost + "b", "http://" + longestHost + "b.",
		" http://example.com", "http://example.com ", "http://example.com/a b",
		"http://example.com/?x=a b", "http://example.com/#a b", "http://example.com/\u00a0",
		"http://example.com/\n", "http://example.com/\x7f", "http://example.com/\u0085",
		"http://example.com/\xff", "http://example.com/?x=\xff", "http://example.com/#\xff",
		"http://example.com/%", "http://example.com/?x=%", "http://example.com/?x=%2",
		"http://example.com/?x=%gg", "http://example.com/#%xy",
	})
}

func TestIsHTTPURLQueryEscapes(t *testing.T) {
	t.Parallel()
	for _, query := range []string{
		"", "q=a+b", "q=%00%09%AF%af%FF", "q=%2520&x=a;b",
		"q=%", "q=%0", "q=%/0", "q=%:0", "q=%@0", "q=%G0", "q=%`0", "q=%g0",
		"q=%0/", "q=%0:", "q=%0@", "q=%0G", "q=%0`", "q=%0g", "q=%20%",
	} {
		_, err := url.QueryUnescape(query)
		want := err == nil
		if got := IsHTTPURL("https://example.com/?" + query); got != want {
			t.Errorf("查询串 %q: 得到 %t，标准库转义校验结果 %t", query, got, want)
		}
	}
}

func TestIsHTTPURLQueryAllocations(t *testing.T) {
	const base = "https://example.com/"
	baseAllocs := testing.AllocsPerRun(100, func() {
		if !IsHTTPURL(base) {
			t.Fatal("基础 URL 应通过校验")
		}
	})
	for _, query := range []string{
		"q=a%20b+c%2fd",
		"q=" + strings.Repeat("a", 65535) + "+",
	} {
		input := base + "?" + query
		allocs := testing.AllocsPerRun(100, func() {
			if !IsHTTPURL(input) {
				t.Fatal("带合法转义的查询串应通过校验")
			}
		})
		// 查询转义只做格式判断，不应为未使用的解码结果增加分配。
		if allocs > baseAllocs {
			t.Errorf("%d 字节查询串增加了分配: 基础 %g 次，带查询串 %g 次", len(query), baseAllocs, allocs)
		}
	}
}

func TestIPFamilies(t *testing.T) {
	t.Parallel()
	ipv4 := []string{"0.0.0.0", "127.0.0.1", "192.0.2.1", "255.255.255.255"}
	ipv6 := []string{"::", "::1", "2001:db8::1", "FE80::1", "::ffff:192.0.2.1", "::ffff:c000:201"}
	invalid := []string{
		"", "localhost", "127.1", "0127.0.0.1", "127.00.0.1", "256.0.0.1",
		"192.0.2.1:80", "192.0.2.1/24", "[::1]", "[::1]:80", "::1/128", "fe80::1%en0",
		"::1%", "1::2::3", "::ffff:192.000.2.1", " 127.0.0.1", "::1\n", "::1\xff",
	}
	checkStrings(t, IsIP, true, ipv4)
	checkStrings(t, IsIP, true, ipv6)
	checkStrings(t, IsIP, false, invalid)
	checkStrings(t, IsIPv4, true, ipv4)
	checkStrings(t, IsIPv4, false, ipv6)
	checkStrings(t, IsIPv4, false, invalid)
	checkStrings(t, IsIPv6, true, ipv6)
	checkStrings(t, IsIPv6, false, ipv4)
	checkStrings(t, IsIPv6, false, invalid)
}

func TestRuneLenBetween(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		s    string
		min  int
		max  int
		want bool
	}{
		{
			name: "允许空字符串",
			want: true,
		},
		{
			name: "不满足最小长度",
			min:  1,
			max:  2,
		},
		{
			name: "按码点计算中文",
			s:    "昵称",
			min:  2,
			max:  2,
			want: true,
		},
		{
			name: "超过最大长度",
			s:    "昵称长",
			min:  1,
			max:  2,
		},
		{
			name: "组合字符不归一化",
			s:    "e\u0301",
			min:  2,
			max:  2,
			want: true,
		},
		{
			name: "组合emoji包含多个码点",
			s:    "👨‍👩‍👧‍👦",
			min:  7,
			max:  7,
			want: true,
		},
		{
			name: "不剔除空白或控制字符",
			s:    " \x00\n",
			max:  3,
			want: true,
		},
		{
			name: "真实替换字符合法",
			s:    "\ufffd",
			max:  1,
			want: true,
		},
		{
			name: "四字节码点恰好达到上界",
			s:    "😀😀",
			min:  2,
			max:  2,
			want: true,
		},
		{
			name: "四字节码点后增加一个字节",
			s:    "😀😀a",
			max:  2,
		},
		{
			name: "字节长度不足下界",
			s:    "a",
			min:  2,
			max:  20,
		},
		{
			name: "非空字符串不满足零上界",
			s:    "a",
		},
		{
			name: "极大上界不溢出",
			s:    "😀",
			min:  1,
			max:  int(^uint(0) >> 1),
			want: true,
		},
		{
			name: "长输入超过小上界",
			s:    strings.Repeat("a", 1<<20),
			max:  20,
		},
		{
			name: "非法UTF8",
			s:    "\xff",
			max:  1,
		},
		{
			name: "截断UTF8",
			s:    "\xe4\xb8",
			max:  2,
		},
		{
			name: "负数下界",
			min:  -1,
			max:  2,
		},
		{
			name: "负数上界",
			max:  -1,
		},
		{
			name: "上下界倒置",
			min:  2,
			max:  1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RuneLenBetween(tc.s, tc.min, tc.max); got != tc.want {
				t.Errorf("RuneLenBetween(%q, %d, %d) = %t，期望 %t", tc.s, tc.min, tc.max, got, tc.want)
			}
		})
	}
}

func FuzzValidators(f *testing.F) {
	for _, s := range []string{
		"", "13800138000", "user@example.com", "http://[::1]:80/?q=%20",
		"http://example.com/?q=%", "::ffff:192.0.2.1", "昵称", "\xff", "\ufffd",
	} {
		f.Add(s, 0, 20)
	}
	f.Fuzz(func(t *testing.T, s string, min, max int) {
		mobile := IsMobileCN(s)
		digits := IsDigits(s)
		if mobile && (!digits || len(s) != 11 || s[0] != '1' || s[1] < '3' || s[1] > '9') {
			t.Fatalf("手机号与数字串契约不一致: %q", s)
		}
		if IsIP(s) != (IsIPv4(s) || IsIPv6(s)) || (IsIPv4(s) && IsIPv6(s)) {
			t.Fatalf("地址族分类不一致: %q", s)
		}
		count := utf8.RuneCountInString(s)
		wantLength := min >= 0 && max >= min && utf8.ValidString(s) && count >= min && count <= max
		if RuneLenBetween(s, min, max) != wantLength {
			t.Fatalf("字符长度契约不一致: %q [%d, %d]", s, min, max)
		}
		// 所有格式检查在任意输入下都应安全返回，不因解析失败 panic。
		IsEmail(s)
		IsHTTPURL(s)
	})
}
