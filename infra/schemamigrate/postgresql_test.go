package schemamigrate

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type postgresqlCommentModel struct {
	ID int64 `gorm:"primaryKey;autoIncrement:false"`
}

func (postgresqlCommentModel) TableName() string { return `Mixed"Cards` }

// 驱动桩只返回预设结果；实际的方言、AutoMigrate 和注释调用链照常执行，不建立网络连接。
type postgresqlStep struct {
	query string
	args  []driver.Value
	value driver.Value
	err   error
}

type postgresqlStub struct {
	t     *testing.T
	steps []postgresqlStep
}

func (s *postgresqlStub) Connect(context.Context) (driver.Conn, error) { return s, nil }
func (s *postgresqlStub) Driver() driver.Driver                        { return s }
func (s *postgresqlStub) Open(string) (driver.Conn, error)             { return s, nil }
func (s *postgresqlStub) Close() error                                 { return nil }
func (s *postgresqlStub) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("不应准备语句")
}
func (s *postgresqlStub) Begin() (driver.Tx, error) {
	return nil, errors.New("不应开启事务")
}

func (s *postgresqlStub) next(ctx context.Context, query string, args []driver.NamedValue) postgresqlStep {
	s.t.Helper()
	if err := ctx.Err(); err != nil {
		return postgresqlStep{err: err}
	}
	if len(s.steps) == 0 {
		s.t.Fatalf("非预期 SQL: %s", query)
	}
	step := s.steps[0]
	s.steps = s.steps[1:]
	var values []driver.Value
	for _, arg := range args {
		values = append(values, arg.Value)
	}
	if query != step.query || !reflect.DeepEqual(values, step.args) {
		s.t.Fatalf("SQL = %q, 参数 = %v；预期 %q, %v", query, values, step.query, step.args)
	}
	return step
}

func (s *postgresqlStub) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	step := s.next(ctx, query, args)
	return driver.RowsAffected(0), step.err
}

func (s *postgresqlStub) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	step := s.next(ctx, query, args)
	return &postgresqlRow{value: step.value}, step.err
}

type postgresqlRow struct {
	value driver.Value
	done  bool
}

func (*postgresqlRow) Columns() []string { return []string{"value"} }
func (*postgresqlRow) Close() error      { return nil }
func (r *postgresqlRow) Next(values []driver.Value) error {
	if r.done {
		return io.EOF
	}
	values[0] = r.value
	r.done = true
	return nil
}

func postgresqlDB(t *testing.T, steps []postgresqlStep) *gorm.DB {
	t.Helper()
	stub := &postgresqlStub{t: t, steps: steps}
	pool := sql.OpenDB(stub)
	t.Cleanup(func() {
		_ = pool.Close()
		if len(stub.steps) != 0 {
			t.Errorf("仍有 %d 条 SQL 未执行，首条: %s", len(stub.steps), stub.steps[0].query)
		}
	})
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: pool}), &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func postgresqlCreateSteps(name, quotedName string) []postgresqlStep {
	return []postgresqlStep{
		{
			query: "SELECT count(*) FROM information_schema.tables WHERE table_schema = CURRENT_SCHEMA() AND table_name = $1 AND table_type = $2",
			args:  []driver.Value{name, "BASE TABLE"},
			value: int64(0),
		},
		{query: "CREATE TABLE " + quotedName + " (\"id\" bigint,PRIMARY KEY (\"id\"))"},
	}
}

func TestPostgreSQLTableComments(t *testing.T) {
	for _, tc := range []struct {
		name    string
		current driver.Value
		comment string
		ddl     string
	}{
		{name: "新增", comment: "卡片", ddl: ` IS E'卡片'`},
		{name: "不变", current: "卡片", comment: "卡片"},
		{
			name:    "更新及转义",
			current: "旧注释",
			comment: "卡片'\\路径\\n? @Name $1; --\n下一行",
			ddl:     " IS E'卡片''\\\\路径\\\\n? @Name $1; --\n下一行'",
		},
		{name: "空值不管理", current: "已有注释"},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/显式表名=%t", tc.name, explicit), func(t *testing.T) {
				name, quotedName := `Mixed"Cards`, `"Mixed""Cards"`
				table := Table{Model: &postgresqlCommentModel{}, Comment: tc.comment}
				if explicit {
					table.Name = "Cards_01"
					name, quotedName = "Cards_01", `"Cards_01"`
				}
				steps := postgresqlCreateSteps(name, quotedName)
				if tc.comment != "" {
					steps = append(steps, postgresqlStep{
						query: "SELECT obj_description($1::regclass, 'pg_class')",
						args:  []driver.Value{quotedName},
						value: tc.current,
					})
				}
				if tc.ddl != "" {
					steps = append(steps, postgresqlStep{query: "COMMENT ON TABLE " + quotedName + tc.ddl})
				}
				db := postgresqlDB(t, steps)
				report, err := Apply(context.Background(), db.WithContext, []Table{table})
				if err != nil || !reflect.DeepEqual(report.Tables, []TableResult{{Name: name, Action: "applied"}}) {
					t.Fatalf("注释迁移失败: %+v, %v", report, err)
				}
			})
		}
	}
}

func TestPostgreSQLCommentFailureStopsMigration(t *testing.T) {
	want := errors.New("模拟 PostgreSQL 错误")
	for _, stage := range []string{"建表", "读取注释", "写入注释"} {
		t.Run(stage, func(t *testing.T) {
			steps := postgresqlCreateSteps("first", `"first"`)
			steps = append(steps, postgresqlCreateSteps("broken", `"broken"`)...)
			if stage != "建表" {
				steps = append(steps, postgresqlStep{
					query: "SELECT obj_description($1::regclass, 'pg_class')",
					args:  []driver.Value{`"broken"`},
				})
			}
			if stage == "写入注释" {
				steps = append(steps, postgresqlStep{query: `COMMENT ON TABLE "broken" IS E'注释'`})
			}
			steps[len(steps)-1].err = want
			db := postgresqlDB(t, steps)
			report, err := Apply(context.Background(), db.WithContext, []Table{
				{Model: &postgresqlCommentModel{}, Name: "first"},
				{Model: &postgresqlCommentModel{}, Name: "broken", Comment: "注释"},
				{Model: &postgresqlCommentModel{}, Name: "later"},
			})
			if !errors.Is(err, want) || !reflect.DeepEqual(report.Tables, []TableResult{
				{Name: "first", Action: "applied"},
				{Name: "broken", Action: "failed"},
			}) {
				t.Fatalf("应保留错误链和部分报告，并停止后续迁移: %+v, %v", report, err)
			}
		})
	}
}

func TestPostgreSQLRejectsMySQLTableOptionsBeforeDDL(t *testing.T) {
	for _, table := range []Table{
		{Model: &postgresqlCommentModel{}, Charset: "utf8mb4"},
		{Model: &postgresqlCommentModel{}, Collation: "utf8mb4_0900_ai_ci"},
	} {
		db := postgresqlDB(t, nil)
		report, err := Apply(context.Background(), db.WithContext, []Table{
			{Model: &postgresqlCommentModel{}, Name: "untouched", Comment: "合法声明"},
			table,
		})
		if err == nil || len(report.Tables) != 0 {
			t.Fatalf("应在整批 DDL 前拒绝 MySQL 专属选项: %+v, %v", report, err)
		}
	}
}
