//go:build pixiu_bootstrap_no_mysql && !pixiu_bootstrap_no_postgresql

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/outbox"
)

func TestBootstrapWithoutMySQLOutbox(t *testing.T) {
	isolatedLog(t, func() {
		requirePostgreSQLPanic(t, "pixiu_bootstrap_no_mysql", func() {
			bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", func() map[outbox.HandlerKey]outbox.Handler { return nil })
		})
		// 禁用一个后端不应阻止另一个后端声明。
		bootstrap.MustRegisterOutbox(outbox.PostgreSQL, "orders", func() map[outbox.HandlerKey]outbox.Handler { return nil })
	})
}
