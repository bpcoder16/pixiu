//go:build pixiu_bootstrap_no_mysql

package bootstrap_test

import (
	"testing"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
)

func TestBootstrapWithoutMySQL(t *testing.T) {
	assertBootstrapModuleDisabled(t, "pixiu_bootstrap_no_mysql", func() {
		bootstrap.MustRegisterMySQL("disabled", true)
	},
		"github.com/bpcoder16/pixiu/infra/mysqlx",
		"github.com/bpcoder16/pixiu/infra/schemamigrate",
		"github.com/go-sql-driver/mysql",
		"gorm.io/",
	)
}
