//go:build pixiu_bootstrap_no_postgresql

package bootstrap

import (
	"fmt"

	"github.com/bpcoder16/pixiu/lifecycle"
)

// MustRegisterPostgreSQL 在禁用 PostgreSQL 装配的构建中立即 panic，避免遗漏显式声明的实例。
func MustRegisterPostgreSQL(name string, isDefault bool) {
	panic(fmt.Errorf("bootstrap: cannot register PostgreSQL instance %q with build tag pixiu_bootstrap_no_postgresql", name))
}

func initPostgreSQL(*lifecycle.Stack) error {
	return nil
}
