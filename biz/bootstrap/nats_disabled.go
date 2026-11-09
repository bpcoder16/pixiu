//go:build pixiu_bootstrap_no_nats

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterNATS 在禁用 NATS 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterNATS(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register NATS instance %q with build tag pixiu_bootstrap_no_nats", name))
}

func initNATS(*lifecycle.Stack) error {
	return nil
}
