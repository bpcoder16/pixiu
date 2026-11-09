package schemamigrate

import (
	"database/sql"
	"strings"

	"gorm.io/gorm"
)

// 单独维护表注释；只有注释变化时才执行 COMMENT ON TABLE。
func ensurePostgreSQLComment(db *gorm.DB, name, comment string) error {
	// regclass 和 DDL 使用同一引用形式，保留表名大小写并沿用连接的 search_path。
	quotedName := db.Statement.Quote(name)
	var current sql.NullString
	if err := db.Raw("SELECT obj_description(?::regclass, 'pg_class')", quotedName).Scan(&current).Error; err != nil {
		return err
	}
	if current.Valid && current.String == comment {
		return nil
	}
	// COMMENT 不接受绑定参数；E 字符串显式转义，不依赖 standard_conforming_strings。
	literal := strings.ReplaceAll(comment, `\`, `\\`)
	literal = strings.ReplaceAll(literal, "'", "''")
	// 不传 Exec 参数，保留注释中的 ?、@Name、$1 等原文。
	return db.Exec("COMMENT ON TABLE " + quotedName + " IS E'" + literal + "'").Error
}
