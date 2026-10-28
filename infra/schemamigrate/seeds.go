package schemamigrate

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"gorm.io/gorm"
)

// Seed 声明一项按记录数校准的 SQL 种子，SQL 必须来自可信部署文件。
type Seed struct {
	// Model 仅用于解析物理表名，不使用其字段值、软删除过滤或模型钩子。
	Model any
	// Where 指定计数及物理删除的条件；为空时作用于整张表。
	Where map[string]any
	// File 为 fs.FS 内的文件路径；每个有效行是一条以分号结束、插入一行的 INSERT INTO。
	File string
}

type seedStatement struct {
	sql  string
	line int
}

// ApplySeeds 比较有效 SQL 行数与 Where 范围内的物理记录数，相同则保留现有数据。
// 不同则在单项事务内删除该范围并重新插入；失败回滚本项并停止，之前的项保持已提交。
// 空清单不获取数据库；调用方用 Context 限制整批期限，并负责单实例执行。
// SQL 的实际写入范围由调用方保证，本包不解析 SQL 或提供权限隔离。
func ApplySeeds(ctx context.Context, masterDB func(context.Context) *gorm.DB, files fs.FS, seeds []Seed) error {
	if ctx == nil {
		return fmt.Errorf("schemamigrate: nil context")
	}
	if len(seeds) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if files == nil {
		return fmt.Errorf("schemamigrate: nil seed filesystem")
	}
	db, err := masterSession(ctx, masterDB)
	if err != nil {
		return err
	}
	for _, seed := range seeds {
		if seedErr := applySeed(ctx, db, files, seed); seedErr != nil {
			// 数据库可能在超时后的提交阶段返回 ErrTxDone，同时保留取消语义与原始错误。
			if ctx.Err() != nil {
				seedErr = errors.Join(seedErr, ctx.Err())
			}
			return fmt.Errorf("schemamigrate: seed %s: %w", seed.File, seedErr)
		}
	}
	return nil
}

func applySeed(ctx context.Context, db *gorm.DB, files fs.FS, seed Seed) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if seed.Model == nil {
		return fmt.Errorf("nil model")
	}
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(seed.Model); err != nil {
		return fmt.Errorf("parse model: %w", err)
	}
	name := stmt.Schema.Table
	if strings.ContainsRune(name, '.') {
		return fmt.Errorf("qualified table name %q is not supported", name)
	}
	statements, readErr := readSeedStatements(files, seed.File)
	if readErr != nil {
		return readErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return db.Transaction(func(tx *gorm.DB) error {
		// 直接使用物理表，避免软删除过滤和 Model 主键缩小声明的范围。
		scoped := func() *gorm.DB {
			return tx.Table(name).
				Where(seed.Where)
		}
		var count int64
		if err := scoped().Count(&count).Error; err != nil {
			return err
		}
		if count == int64(len(statements)) {
			return nil
		}
		// DELETE 可随本项插入一起回滚；不用会隐式提交的 TRUNCATE，也不重置自增序列。
		if err := scoped().
			Session(&gorm.Session{AllowGlobalUpdate: len(seed.Where) == 0}).
			Delete(nil).Error; err != nil {
			return err
		}
		for _, statement := range statements {
			result := tx.Exec(statement.sql)
			if result.Error != nil {
				return fmt.Errorf("line %d: %w", statement.line, result.Error)
			}
			if result.RowsAffected != 1 {
				return fmt.Errorf("line %d: INSERT affected %d rows, want 1", statement.line, result.RowsAffected)
			}
		}
		if err := scoped().Count(&count).Error; err != nil {
			return err
		}
		if count != int64(len(statements)) {
			return fmt.Errorf("inserted scope contains %d rows, want %d", count, len(statements))
		}
		return nil
	})
}

func readSeedStatements(files fs.FS, file string) ([]seedStatement, error) {
	data, err := fs.ReadFile(files, file)
	if err != nil {
		return nil, err
	}
	var statements []seedStatement
	lineNumber := 0
	for line := range strings.SplitSeq(string(data), "\n") {
		lineNumber++
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		words := strings.Fields(line)
		if len(words) < 3 || !strings.EqualFold(words[0], "INSERT") || !strings.EqualFold(words[1], "INTO") || !strings.HasSuffix(line, ";") {
			return nil, fmt.Errorf("line %d: expected one complete INSERT INTO ending with a semicolon", lineNumber)
		}
		statements = append(statements, seedStatement{sql: line, line: lineNumber})
	}
	// 空文件视为配置错误，避免误把现有数据全部清空。
	if len(statements) == 0 {
		return nil, fmt.Errorf("no INSERT statements")
	}
	return statements, nil
}
