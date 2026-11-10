//go:build pixiu_bootstrap_no_mysql

package outbox

const mysqlBackendEnabled = false

func isMySQLLockConflict(error) bool {
	return false
}
