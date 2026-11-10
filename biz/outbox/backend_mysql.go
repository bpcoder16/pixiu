//go:build !pixiu_bootstrap_no_mysql

package outbox

import (
	"errors"

	mysqldriver "github.com/go-sql-driver/mysql"
)

const mysqlBackendEnabled = true

func isMySQLLockConflict(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	return errors.As(err, &mysqlErr) && mysqlErr.Number == 3572
}
