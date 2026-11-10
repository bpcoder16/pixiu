//go:build !pixiu_bootstrap_no_postgresql

package outbox

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

const postgresqlBackendEnabled = true

func isPostgreSQLLockConflict(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}
