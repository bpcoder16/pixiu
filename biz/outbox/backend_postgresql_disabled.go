//go:build pixiu_bootstrap_no_postgresql

package outbox

const postgresqlBackendEnabled = false

func isPostgreSQLLockConflict(error) bool {
	return false
}
