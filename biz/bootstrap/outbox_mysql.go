//go:build !pixiu_bootstrap_no_mysql

package bootstrap

import (
	"github.com/bpcoder16/pixiu/biz/outbox"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
)

const outboxMySQLDisabledTag = ""

func outboxMySQLConfig(name string) outbox.StoreConfig {
	return outbox.StoreConfig{
		Backend:  outbox.MySQL,
		MasterDB: mysqlx.Named(name).MasterDB,
	}
}
