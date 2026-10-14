package schemamigrate

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
)

// Table 声明项目维护的表。Name 为空时沿用 GORM 命名；非空时用于指定物理分表。
// 只迁移显式登记的表；最终表名不得包含点号，不支持库名或 schema 限定名。
// Comment、Charset、Collation 为可选 MySQL 设置；空 Comment 表示不管理表注释。
// Charset/Collation 仅用于建表，不转换已有表。
type Table struct {
	// Model 为必填的 GORM 模型，用于解析待迁移的表结构。
	Model any
	// Name 指定物理表名，仅允许字母、数字和下划线；为空时沿用 GORM 命名。
	Name string
	// Comment 指定 MySQL 表注释；为空时不管理或清空已有注释。
	Comment string
	// Charset 指定 MySQL 建表字符集，不转换已有表；为空时使用数据库默认值。
	Charset string
	// Collation 指定 MySQL 建表排序规则，需同时设置 Charset，不转换已有表。
	Collation string
}

// TableResult 的 Action 为 applied 或 failed；applied 表示 AutoMigrate 成功，不保证实际执行了 DDL。
type TableResult struct {
	Name   string
	Action string
}

// Report 保留按登记顺序完成的结果；失败时前面的 DDL 可能已经生效。
type Report struct{ Tables []TableResult }

// declaration 保存迁移前完成校验和解析的表声明，供 Apply 逐表执行。
type declaration struct {
	// table 保留原始表声明，包括调用方是否显式指定 Name。
	table Table
	// resolvedName 为 GORM 解析后的实际表名，用于迁移结果和表注释更新。
	resolvedName string
	// options 为预先校验的 MySQL 建表选项，其他方言为空。
	options string
}

var identifier = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

// Apply 通过已有主库执行 GORM AutoMigrate，不创建连接池、状态表或迁移锁。
// 仅按清单顺序迁移登记表，忽略模型关联及其外键，不修改调用方的 GORM 配置。
// 空清单不调用 masterDB；错误保留底层错误链，调用方负责整体超时和启动失败处理。
func Apply(ctx context.Context, masterDB func(context.Context) *gorm.DB, tables []Table) (Report, error) {
	if ctx == nil {
		return Report{}, fmt.Errorf("schemamigrate: nil context")
	}
	if len(tables) == 0 {
		return Report{}, nil
	}
	if masterDB == nil {
		return Report{}, fmt.Errorf("schemamigrate: nil master provider")
	}
	base := masterDB(ctx)
	if base == nil {
		return Report{}, fmt.Errorf("schemamigrate: nil master session")
	}
	if base.Error != nil {
		return Report{}, base.Error
	}
	if base.DryRun {
		return Report{}, fmt.Errorf("schemamigrate: DryRun cannot confirm migration success")
	}
	if _, ok := base.Statement.ConnPool.(gorm.TxCommitter); ok {
		return Report{}, fmt.Errorf("schemamigrate: external transaction is not supported")
	}
	db := base.Session(&gorm.Session{NewDB: true, Context: ctx})
	// Session 已复制 Config；仅在本次迁移中禁止关联展开和关联外键 DDL。
	db.IgnoreRelationshipsWhenMigrating = true
	declarations, err := prepare(db, tables)
	if err != nil {
		return Report{}, err
	}
	var report Report
	for _, d := range declarations {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return report, ctxErr
		}
		// 每张表使用独立会话，避免表名和建表选项泄漏给下一张表。
		tx := db.Session(&gorm.Session{NewDB: true})
		if d.table.Name != "" {
			tx = tx.Table(d.table.Name)
		}
		if d.options != "" {
			tx = tx.Set("gorm:table_options", d.options)
		}
		migrateErr := tx.AutoMigrate(d.table.Model)
		if migrateErr == nil && d.table.Comment != "" {
			migrateErr = ensureMySQLComment(db, d.resolvedName, d.table.Comment)
		}
		result := TableResult{Name: d.resolvedName, Action: "applied"}
		if migrateErr != nil {
			result.Action = "failed"
		}
		report.Tables = append(report.Tables, result)
		if migrateErr != nil {
			return report, fmt.Errorf("schemamigrate: table %s: %w", d.resolvedName, migrateErr)
		}
	}
	return report, nil
}

func prepare(db *gorm.DB, tables []Table) ([]declaration, error) {
	var declarations []declaration
	seen := make(map[string]bool, len(tables))
	for _, table := range tables {
		if table.Model == nil {
			return nil, fmt.Errorf("schemamigrate: nil model")
		}
		if table.Name != "" && !identifier.MatchString(table.Name) {
			return nil, fmt.Errorf("schemamigrate: invalid physical table name %q", table.Name)
		}
		stmt := &gorm.Statement{DB: db}
		if err := stmt.ParseWithSpecialTableName(table.Model, table.Name); err != nil {
			return nil, fmt.Errorf("schemamigrate: parse model: %w", err)
		}
		resolvedName := stmt.Schema.Table
		if strings.ContainsRune(resolvedName, '.') {
			return nil, fmt.Errorf("schemamigrate: qualified table name %q is not supported", resolvedName)
		}
		if seen[resolvedName] {
			return nil, fmt.Errorf("schemamigrate: duplicate table %q", resolvedName)
		}
		seen[resolvedName] = true
		options, err := tableOptions(db, table)
		if err != nil {
			return nil, fmt.Errorf("schemamigrate: table %s: %w", resolvedName, err)
		}
		declarations = append(declarations, declaration{
			table:        table,
			resolvedName: resolvedName,
			options:      options,
		})
	}
	return declarations, nil
}
