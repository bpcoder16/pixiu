//go:build pixiu_bootstrap_no_mysql

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterMySQL 在禁用 MySQL 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterMySQL(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register MySQL instance %q with build tag pixiu_bootstrap_no_mysql", name))
}

func initMySQL(*lifecycle.Stack) error {
	return nil
}
