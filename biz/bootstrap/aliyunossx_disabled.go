//go:build pixiu_bootstrap_no_aliyun_oss

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterAliyunOSS 在禁用 AliyunOSS 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterAliyunOSS(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register AliyunOSS instance %q with build tag pixiu_bootstrap_no_aliyun_oss", name))
}

func initAliyunOSS(*lifecycle.Stack) error {
	return nil
}
