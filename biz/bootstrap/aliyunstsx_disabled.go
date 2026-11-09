//go:build pixiu_bootstrap_no_aliyun_sts

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterAliyunSTS 在禁用 AliyunSTS 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterAliyunSTS(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register AliyunSTS instance %q with build tag pixiu_bootstrap_no_aliyun_sts", name))
}

func initAliyunSTS(*lifecycle.Stack) error {
	return nil
}
