//go:build pixiu_bootstrap_no_postgresql && !pixiu_bootstrap_no_mysql

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/outbox"
)

func TestBootstrapWithoutPostgreSQLOutbox(t *testing.T) {
	isolatedLog(t, func() {
		requireMySQLPanic(t, "pixiu_bootstrap_no_postgresql", func() {
			bootstrap.MustRegisterOutbox(outbox.PostgreSQL, "orders", outboxHandlers)
		})
		bootstrap.MustRegisterOutbox(outbox.MySQL, "orders", outboxHandlers)
	})
}
