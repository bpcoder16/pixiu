package bootstrap

import (
	"fmt"
	"testing"
)

func TestInstanceNameValidation(t *testing.T) {
	if err := validateInstanceName("Redis", "Cache_09-a"); err != nil {
		t.Fatalf("合法实例名被拒绝: %v", err)
	}
	for _, name := range []string{"", "实例", "bad.name", "../name", `a\b`, "a b"} {
		// 无须初始化环境或实例；各入口应先返回统一格式的名称校验错误。
		detail := ""
		if name != "" {
			detail = ": only ASCII letters, digits, '_' and '-' are allowed"
		}
		want := fmt.Sprintf("bootstrap: invalid Redis name %q", name) + detail
		if err := validateInstanceName("Redis", name); err == nil || err.Error() != want {
			t.Errorf("注册校验错误 = %v，预期 %q", err, want)
		}
	}
}
