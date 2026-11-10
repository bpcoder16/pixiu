//go:build !pixiu_bootstrap_no_postgresql

package bootstrap

import (
	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/postgresqlx"
)

const outboxPostgreSQLDisabledTag = ""

func outboxPostgreSQLConfig(name string) outbox.StoreConfig {
	return outbox.StoreConfig{
		Backend:  outbox.PostgreSQL,
		MasterDB: postgresqlx.Named(name).MasterDB,
	}
}
