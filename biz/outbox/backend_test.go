package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

func TestBackendLockConflictAdapters(t *testing.T) {
	my := &mysqldriver.MySQLError{Number: 3572}
	pg := &pgconn.PgError{Code: "55P03"}
	for _, wrap := range []func(error) error{
		func(err error) error { return err },
		func(err error) error { return fmt.Errorf("数据库操作失败: %w", err) },
		func(err error) error { return errors.Join(errors.New("其他错误"), err) },
	} {
		if isMySQLLockConflict(wrap(my)) != mysqlBackendEnabled || isPostgreSQLLockConflict(wrap(pg)) != postgresqlBackendEnabled {
			t.Fatal("可用后端必须识别原始、包装和合并的 NOWAIT 错误")
		}
		if isMySQLLockConflict(wrap(pg)) || isPostgreSQLLockConflict(wrap(my)) {
			t.Fatal("不同驱动的错误不能混用")
		}
	}
	for _, err := range []error{
		nil,
		errors.New("3572 55P03"),
		&mysqldriver.MySQLError{Number: 1213},
		&pgconn.PgError{Code: "40001"},
	} {
		if isMySQLLockConflict(err) || isPostgreSQLLockConflict(err) {
			t.Fatalf("非 NOWAIT 错误不能被当作忙碌跳过: %v", err)
		}
	}
}

func TestBackendBuildAvailability(t *testing.T) {
	for backend, enabled := range map[Backend]bool{
		MySQL:      mysqlBackendEnabled,
		PostgreSQL: postgresqlBackendEnabled,
	} {
		store, err := NewStore(StoreConfig{
			Backend:  backend,
			MasterDB: func(context.Context) *gorm.DB { panic("构造不得访问数据库") },
		})
		if enabled && (store == nil || err != nil) || !enabled && (store != nil || !errors.Is(err, ErrInvalidArgument)) {
			t.Fatalf("后端 %s 的构建可用性不符: enabled=%v store=%p err=%v", backend, enabled, store, err)
		}
	}
}
