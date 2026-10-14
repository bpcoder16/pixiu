package schemamigrate

import (
	"database/sql"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

func tableOptions(db *gorm.DB, table Table) (string, error) {
	if db.Dialector.Name() != "mysql" {
		if table.Comment != "" || table.Charset != "" || table.Collation != "" {
			return "", fmt.Errorf("table comments, charset and collation currently require MySQL")
		}
		return "", nil
	}
	if table.Collation != "" && table.Charset == "" {
		return "", fmt.Errorf("collation requires charset")
	}
	var options strings.Builder
	options.WriteString("ENGINE=InnoDB")
	for _, item := range []struct{ value, prefix string }{
		{table.Charset, " DEFAULT CHARSET="},
		{table.Collation, " COLLATE="},
	} {
		if item.value == "" {
			continue
		}
		if !identifier.MatchString(item.value) {
			return "", fmt.Errorf("invalid table option %q", item.value)
		}
		options.WriteString(item.prefix + item.value)
	}
	return options.String(), nil
}

// 与 decard01 一样单独维护表注释；只有注释变化时才执行 ALTER TABLE。
func ensureMySQLComment(db *gorm.DB, name, comment string) error {
	update := func(conn *gorm.DB) error {
		var rows []struct {
			Comment string `gorm:"column:comment"`
		}
		if err := conn.Raw(`SELECT TABLE_COMMENT AS comment FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?`, name).Scan(&rows).Error; err != nil {
			return err
		}
		if len(rows) == 1 && rows[0].Comment == comment {
			return nil
		}
		// MySQL COMMENT DDL 不接受绑定参数；固定连接并按该会话的 SQL mode 转义。
		var mode string
		if err := conn.Raw("SELECT @@SESSION.sql_mode").Scan(&mode).Error; err != nil {
			return err
		}
		literal := comment
		if !strings.Contains(strings.ToUpper(mode), "NO_BACKSLASH_ESCAPES") {
			literal = strings.ReplaceAll(literal, `\`, `\\`)
		}
		literal = strings.ReplaceAll(literal, "'", "''")
		quotedName := "`" + strings.ReplaceAll(name, "`", "``") + "`"
		return conn.Exec("ALTER TABLE " + quotedName + " COMMENT = '" + literal + "'").Error
	}
	if _, pinned := db.Statement.ConnPool.(*sql.Conn); pinned {
		return update(db)
	}
	return db.Connection(update)
}
