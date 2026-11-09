//go:build pixiu_bootstrap_no_pgsql

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutPostgreSQL(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_pgsql", func() {
		bootstrap.MustRegisterPostgreSQL("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/pgsqlx",
		"github.com/jackc/pgx/",
		"gorm.io/driver/postgres",
	)
}
