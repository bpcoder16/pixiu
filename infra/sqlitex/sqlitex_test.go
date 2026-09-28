package sqlitex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type testRecord struct {
	ID    int `gorm:"primaryKey"`
	Token string
}

func testDSN(t *testing.T) string {
	t.Helper()
	u := url.URL{Scheme: "file", Path: filepath.Join(t.TempDir(), "test.db")}
	return u.String() + "?_foreign_keys=on&_busy_timeout=5000"
}

func captureRecords(t *testing.T) (*bytes.Buffer, context.Context) {
	t.Helper()
	buf := &bytes.Buffer{}
	l := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(buf)),
	)
	previous := logit.Default()
	logit.SetDefault(l)
	t.Cleanup(func() {
		logit.SetDefault(previous)
		_ = logit.Close(l)
	})
	ctx := logit.WithContext(context.Background())
	logit.AddField(ctx, logit.Str("request_id", "request-1"))
	return buf, ctx
}

func parseRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	if buf.Len() == 0 {
		return nil
	}
	var records []map[string]any
	for _, line := range bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte{'\n'}) {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("解析日志: %v", err)
		}
		records = append(records, record)
	}
	return records
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	cases := []Config{
		{},
		{Name: "local"},
		{Name: "local", DSN: ":memory:", SlowThreshold: -time.Second},
		{Name: "local", DSN: ":memory:", Pool: Pool{MaxOpenConns: -1}},
		{Name: "local", DSN: ":memory:", Pool: Pool{ConnMaxLifetime: -time.Second}},
		{Name: "local", DSN: ":memory:", Pool: Pool{ConnMaxIdleTime: -time.Second}},
		{Name: "local", DSN: ":memory:", Pool: Pool{MaxOpenConns: 1, MaxIdleConns: 2}},
		{Name: "local", DSN: ":memory:", Pool: Pool{MaxOpenConns: 2}},
		{Name: "local", DSN: ":memory:", Pool: Pool{ConnMaxLifetime: time.Second}},
		{Name: "local", DSN: "file:sqlitex-test-shared?mode=memory&cache=shared", Pool: Pool{ConnMaxIdleTime: time.Second}},
	}
	for _, cfg := range cases {
		client, err := New(context.Background(), cfg)
		if client != nil || err == nil {
			t.Fatalf("无效配置被接受: cfg=%+v client=%v err=%v", cfg, client, err)
		}
	}
	if client, err := New(nil, Config{Name: "local", DSN: ":memory:"}); client != nil || err == nil {
		t.Fatalf("nil context 被接受: client=%v err=%v", client, err)
	}
}

func TestFilePoolLifetime(t *testing.T) {
	client, err := New(context.Background(), Config{
		Name: "local",
		DSN:  testDSN(t),
		Pool: Pool{
			ConnMaxLifetime: 5 * time.Millisecond,
			ConnMaxIdleTime: time.Minute,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sqlDB, err := client.DB(context.Background()).DB()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if err := sqlDB.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sqlDB.Stats().MaxLifetimeClosed == 0 {
		t.Fatal("文件库的连接寿命配置未生效")
	}
}

func TestNewCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(ctx, Config{Name: "local", DSN: testDSN(t)})
	if client != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消启动: client=%v err=%v", client, err)
	}
}

func TestFileDatabaseLifecycleAndTransaction(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	client, err := New(ctx, Config{Name: "local", DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := client.DB(ctx).DB()
	if err != nil {
		t.Fatal(err)
	}
	if got := sqlDB.Stats().MaxOpenConnections; got != 1 {
		t.Fatalf("默认最大连接数=%d, want 1", got)
	}
	if err := client.DB(ctx).AutoMigrate(&testRecord{}); err != nil {
		t.Fatal(err)
	}
	if err := client.DB(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&testRecord{Token: "rolled-back"}).Error; err != nil {
			return err
		}
		return errors.New("rollback")
	}); err == nil || err.Error() != "rollback" {
		t.Fatalf("事务回滚错误=%v", err)
	}
	if err := client.DB(ctx).Create(&testRecord{Token: "persisted"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}
	other, err := New(ctx, Config{Name: "local", DSN: dsn})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var count int64
	if err := other.DB(ctx).Model(&testRecord{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("文件库重开后记录数=%d, want 1", count)
	}
}

func TestDSNOptionsApplyToEveryConnection(t *testing.T) {
	client, err := New(context.Background(), Config{
		Name: "local",
		DSN:  testDSN(t),
		Pool: Pool{MaxOpenConns: 2, MaxIdleConns: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sqlDB, err := client.DB(context.Background()).DB()
	if err != nil {
		t.Fatal(err)
	}
	connections := make([]*sql.Conn, 0, 2)
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := sqlDB.Conn(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var foreignKeys int
		if err := conn.QueryRowContext(context.Background(), "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil {
			t.Fatal(err)
		}
		if foreignKeys != 1 {
			t.Fatalf("第 %d 条连接未启用外键: %d", i+1, foreignKeys)
		}
	}
}

func TestTypedOptionsApplyToEveryConnection(t *testing.T) {
	ctx := context.Background()
	foreignKeys := true
	client, err := New(ctx, Config{
		Name:        "local",
		DSN:         testDSN(t) + "&_journal=DELETE&_sync=NORMAL&_fk=off&_timeout=100",
		JournalMode: JournalModeWAL,
		Synchronous: SynchronousFull,
		BusyTimeout: 2 * time.Second,
		ForeignKeys: &foreignKeys,
		Pool: Pool{
			MaxOpenConns: 2,
			MaxIdleConns: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	sqlDB, err := client.DB(ctx).DB()
	if err != nil {
		t.Fatal(err)
	}
	connections := make([]*sql.Conn, 0, 2)
	defer func() {
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	for i := 0; i < 2; i++ {
		conn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, conn)
		var journalMode string
		var synchronous, busyTimeout, enabled int
		if err := conn.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil {
			t.Fatal(err)
		}
		if err := conn.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&enabled); err != nil {
			t.Fatal(err)
		}
		if journalMode != "wal" || synchronous != 2 || busyTimeout != 2000 || enabled != 1 {
			t.Fatalf("第 %d 条连接配置未生效: journal=%s synchronous=%d busy=%d foreign_keys=%d", i+1, journalMode, synchronous, busyTimeout, enabled)
		}
	}
}

func TestTypedOptionsCanDisableForeignKeys(t *testing.T) {
	foreignKeys := false
	client, err := New(context.Background(), Config{
		Name:        "local",
		DSN:         testDSN(t),
		ForeignKeys: &foreignKeys,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var enabled int
	if err := client.DB(context.Background()).Raw("PRAGMA foreign_keys").Scan(&enabled).Error; err != nil {
		t.Fatal(err)
	}
	if enabled != 0 {
		t.Fatalf("显式关闭外键后 foreign_keys=%d, want 0", enabled)
	}
}

func TestTypedOptionsRejectInvalidConfiguration(t *testing.T) {
	cases := []struct {
		name string
		cfg  Config
	}{
		{
			name: "journal mode",
			cfg: Config{
				Name:        "local",
				DSN:         testDSN(t),
				JournalMode: "invalid",
			},
		},
		{
			name: "synchronous",
			cfg: Config{
				Name:        "local",
				DSN:         testDSN(t),
				Synchronous: "invalid",
			},
		},
		{
			name: "busy timeout",
			cfg: Config{
				Name:        "local",
				DSN:         testDSN(t),
				BusyTimeout: -time.Second,
			},
		},
		{
			name: "fractional millisecond timeout",
			cfg: Config{
				Name:        "local",
				DSN:         testDSN(t),
				BusyTimeout: time.Millisecond + time.Microsecond,
			},
		},
		{
			name: "overflow timeout",
			cfg: Config{
				Name:        "local",
				DSN:         testDSN(t),
				BusyTimeout: (1 << 31) * time.Millisecond,
			},
		},
		{
			name: "memory WAL",
			cfg: Config{
				Name:        "local",
				DSN:         ":memory:",
				JournalMode: JournalModeWAL,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client, err := New(context.Background(), tc.cfg)
			if client != nil || err == nil {
				t.Fatalf("无效配置被接受: client=%v err=%v", client, err)
			}
		})
	}
}

func TestNamedSharedMemoryUsesOneDatabase(t *testing.T) {
	ctx := context.Background()
	client, err := New(ctx, Config{
		Name: "memory",
		DSN:  "file:sqlitex-test-shared?mode=memory&cache=shared",
		Pool: Pool{
			MaxOpenConns: 2,
			MaxIdleConns: 2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.DB(ctx).Exec("CREATE TABLE shared_records (id INTEGER PRIMARY KEY)").Error; err != nil {
		t.Fatal(err)
	}
	sqlDB, err := client.DB(ctx).DB()
	if err != nil {
		t.Fatal(err)
	}
	first, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := first.ExecContext(ctx, "INSERT INTO shared_records (id) VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := second.QueryRowContext(ctx, "SELECT count(*) FROM shared_records").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("共享内存库中的记录数=%d, want 1", count)
	}
}

func TestDBRejectsNilContext(t *testing.T) {
	client, err := New(context.Background(), Config{Name: "local", DSN: ":memory:"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	defer func() {
		if recover() == nil {
			t.Fatal("DB(nil) 应 panic")
		}
	}()
	client.DB(nil)
}

func TestTracePolicyDurationAndContext(t *testing.T) {
	buf, baseCtx := captureRecords(t)
	ctx := logit.WithStart(baseCtx)
	l := newTraceLogger(Config{Name: "local", SlowThreshold: time.Hour})
	query := func() (string, int64) {
		t.Fatal("未启用 SQL 日志时不应生成 SQL")
		return "", 0
	}
	l.Trace(ctx, time.Now(), query, nil)
	if buf.Len() != 0 {
		t.Fatalf("普通查询产生了日志: %s", buf.String())
	}
	logit.InfoDuration(ctx, "request done")
	records := parseRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("耗时汇总日志数=%d, want 1", len(records))
	}
	if _, ok := records[0]["sqlite_1_duration_ms"].(float64); !ok {
		t.Fatalf("缺少 SQLite 请求耗时: %v", records[0])
	}
	buf.Reset()
	l = newTraceLogger(Config{Name: "local", SlowThreshold: time.Millisecond})
	l.Trace(ctx, time.Now().Add(-time.Second), func() (string, int64) {
		return "SELECT * FROM records WHERE token = ?", -1
	}, nil)
	l.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT * FROM records WHERE token = ?", 0
	}, errors.New("query failed"))
	l.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT * FROM records WHERE token = ?", 0
	}, gorm.ErrRecordNotFound)
	records = parseRecords(t, buf)
	if len(records) != 3 {
		t.Fatalf("查询日志数=%d, want 3: %s", len(records), buf.String())
	}
	for i, want := range []string{"WARN", "ERROR", "ERROR"} {
		record := records[i]
		if record["level"] != want || record["msg"] != "SQLite" ||
			record["request_id"] != "request-1" ||
			record[logit.DownstreamTypeKey] != "SQLite" ||
			record[logit.DownstreamIDKey] != "local" {
			t.Fatalf("第 %d 条查询日志不正确: %v", i, record)
		}
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		if details["sql"] != "SELECT * FROM records WHERE token = ?" {
			t.Fatalf("SQL 缺失: %v", details)
		}
		if _, exists := details["endpoint_type"]; exists {
			t.Fatalf("SQLite 查询日志不应包含端点类型: %v", details)
		}
		if _, exists := details["endpoint"]; exists {
			t.Fatalf("SQLite 查询日志不应包含端点名称: %v", details)
		}
	}
	if details := records[1][logit.DownstreamDetailsKey].(map[string]any); details["err"] != "query failed" {
		t.Fatalf("错误原文缺失: %v", details)
	}
}

func TestGORMDiagnosticsAndLogMode(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "local"})
	warn := l.LogMode(logger.Warn)
	warn.Info(ctx, "hidden")
	warn.Warn(ctx, "duplicate callback %s", "audit")
	warn.Error(ctx, "invalid model")
	l.Info(ctx, "original remains enabled")
	records := parseRecords(t, buf)
	if len(records) != 3 {
		t.Fatalf("诊断日志数=%d, want 3: %s", len(records), buf.String())
	}
	for i, want := range []string{"WARN", "ERROR", "INFO"} {
		if records[i]["level"] != want || records[i][logit.DownstreamTypeKey] != "SQLite" {
			t.Fatalf("第 %d 条诊断日志不正确: %v", i, records[i])
		}
	}
	if details := records[0][logit.DownstreamDetailsKey].(map[string]any); details["msg"] != "duplicate callback audit" || len(details) != 1 {
		t.Fatalf("诊断详情不正确: %v", details)
	}
}

func TestRealGORMQueryLoggingAndInterpolation(t *testing.T) {
	buf, ctx := captureRecords(t)
	client, err := New(ctx, Config{
		Name:          "local",
		DSN:           ":memory:",
		LogSQL:        true,
		SlowThreshold: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.DB(ctx).AutoMigrate(&testRecord{}); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	var rows []testRecord
	if err := client.DB(ctx).Where("token = ?", "hidden-secret").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	records := parseRecords(t, buf)
	if len(records) != 1 || records[0]["level"] != "INFO" {
		t.Fatalf("正常查询日志不正确: %v", records)
	}
	details := records[0][logit.DownstreamDetailsKey].(map[string]any)
	if sql, ok := details["sql"].(string); !ok || !strings.Contains(sql, "token = ?") {
		t.Fatalf("SQL 未保留占位符: %v", details)
	}
	if strings.Contains(buf.String(), "hidden-secret") {
		t.Fatalf("默认插值泄露参数: %s", buf.String())
	}
	buf.Reset()
	interpolated, err := New(ctx, Config{
		Name:           "local",
		DSN:            ":memory:",
		SlowThreshold:  time.Hour,
		LogSQL:         true,
		InterpolateSQL: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer interpolated.Close()
	if err := interpolated.DB(ctx).AutoMigrate(&testRecord{}); err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if err := interpolated.DB(ctx).Where("token = ?", "visible-value").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "visible-value") {
		t.Fatalf("显式插值未输出参数: %s", buf.String())
	}
}
