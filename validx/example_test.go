package validx_test

import (
	"fmt"

	"github.com/bpcoder16/pixiu/validx"
)

func Example() {
	fmt.Println(validx.IsMobileCN("13800138000"))
	fmt.Println(validx.IsEmail("user.name+tag@example.com"))
	fmt.Println(validx.IsHTTPURL("https://example.com/path?q=value"))
	fmt.Println(validx.IsIP("::1"))
	fmt.Println(validx.IsIPv4("192.0.2.1"))
	fmt.Println(validx.IsIPv6("::ffff:192.0.2.1"))
	fmt.Println(validx.IsDigits("00123"))
	fmt.Println(validx.RuneLenBetween("昵称", 2, 20))
	fmt.Println(validx.IsMobileCN("+8613800138000"))
	// Output:
	// true
	// true
	// true
	// true
	// true
	// true
	// true
	// true
	// false
}
