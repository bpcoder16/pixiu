//go:build pixiu_bootstrap_no_mysql && !pixiu_bootstrap_no_postgresql

// 仅在 PostgreSQL 启用时保留 MySQL 禁用占位实现，供 outbox.go 使用。
// 两种数据库均禁用时由 outbox_disabled.go 接管，排除此文件以避免引入 biz/outbox 及其依赖。

package bootstrap

import "github.com/bpcoder16/pixiu/biz/outbox"

const outboxMySQLDisabledTag = "pixiu_bootstrap_no_mysql"

func outboxMySQLConfig(string) outbox.StoreConfig {
	panic("bootstrap: Outbox MySQL is disabled by " + outboxMySQLDisabledTag)
}
