package pgsqlx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	gormpostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func testClusterClient(t *testing.T, master *gorm.DB, masterPool *sql.DB, slaves ...*gorm.DB) *Client {
	t.Helper()
	client := &Client{}
	err := gormcore.BuildCluster(context.Background(), &client.cluster, master, slaves, func(_ context.Context, db *gorm.DB, role string) (*gorm.DB, *sql.DB, error) {
		if role == "master" {
			return db, masterPool, nil
		}
		return db, nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func captureRecords(t *testing.T) (*bytes.Buffer, context.Context) {
	t.Helper()
	buf := &bytes.Buffer{}
	l := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(buf)),
	)
	old := logit.Default()
	logit.SetDefault(l)
	t.Cleanup(func() {
		logit.SetDefault(old)
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

func validEndpoint() Endpoint {
	return Endpoint{
		Host:     "127.0.0.1",
		Database: "orders",
		Username: "reader",
		Password: "secret-marker",
		SSLMode:  "disable",
	}
}

func TestPrepareConnectionAndPool(t *testing.T) {
	endpoint := Endpoint{
		Host:        "::1",
		Port:        5433,
		Database:    "orders/test",
		Username:    "a@b",
		Password:    "p@ss:word",
		Charset:     "utf-8",
		Location:    "Asia/Shanghai",
		SSLMode:     "disable",
		DialTimeout: 2 * time.Second,
		Pool:        Pool{MaxOpenConns: 3},
	}
	prepared, err := prepare(endpoint, "master", "Asia/Shanghai", true)
	if err != nil {
		t.Fatal(err)
	}
	cfg := prepared.driver
	if prepared.name != "master" || cfg.Host != "::1" || cfg.Port != 5433 ||
		cfg.Database != "orders/test" || cfg.User != "a@b" || cfg.Password != "p@ss:word" ||
		cfg.ConnectTimeout != 2*time.Second || cfg.RuntimeParams["timezone"] != "Asia/Shanghai" ||
		cfg.RuntimeParams["client_encoding"] != "UTF8" || prepared.location.String() != "Asia/Shanghai" ||
		cfg.DefaultQueryExecMode != pgx.QueryExecModeExec || cfg.TLSConfig != nil {
		t.Fatalf("连接配置映射错误: host=%q port=%d db=%q user=%q mode=%v", cfg.Host, cfg.Port, cfg.Database, cfg.User, cfg.DefaultQueryExecMode)
	}
	if prepared.pool.MaxOpenConns != 3 || prepared.pool.MaxIdleConns != 3 ||
		prepared.pool.ConnMaxLifetime != 3*time.Minute || prepared.pool.ConnMaxIdleTime != time.Minute {
		t.Fatalf("连接池默认值错误: %+v", prepared.pool)
	}
	defaults, err := prepare(validEndpoint(), "slave-1", "", false)
	if err != nil || defaults.driver.Port != 5432 || defaults.driver.DefaultQueryExecMode != pgx.QueryExecModeCacheStatement ||
		defaults.driver.RuntimeParams["client_encoding"] != "UTF8" || defaults.location == nil ||
		defaults.location.String() != "Asia/Shanghai" ||
		defaults.pool.MaxOpenConns != 100 || defaults.pool.MaxIdleConns != 10 {
		t.Fatalf("默认连接配置错误: %+v, %v", defaults, err)
	}
	custom := validEndpoint()
	custom.Location = "UTC"
	overridden, err := prepare(custom, "slave-1", "", false)
	if err != nil || overridden.location != time.UTC {
		t.Fatalf("自定义时区未生效: %+v, %v", overridden, err)
	}
	secure := validEndpoint()
	secure.Host = "db.example.com"
	secure.SSLMode = "verify-full"
	verified, err := prepare(secure, "master", "", false)
	if err != nil || verified.driver.TLSConfig == nil ||
		verified.driver.TLSConfig.ServerName != secure.Host || verified.driver.TLSConfig.InsecureSkipVerify {
		t.Fatalf("TLS 主机名验证配置错误: %+v, %v", verified.driver, err)
	}
}

func TestTimestampLocationCodec(t *testing.T) {
	location, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	typeMap := pgtype.NewMap()
	timestamptzBefore, _ := typeMap.TypeForOID(pgtype.TimestamptzOID)
	registerTimestampLocation(typeMap, location)
	timestampType, ok := typeMap.TypeForOID(pgtype.TimestampOID)
	if !ok {
		t.Fatal("timestamp 类型未注册")
	}
	value, err := timestampType.Codec.DecodeDatabaseSQLValue(typeMap, pgtype.TimestampOID, pgtype.TextFormatCode, []byte("2024-01-02 03:04:05"))
	if err != nil {
		t.Fatal(err)
	}
	got, ok := value.(time.Time)
	want := time.Date(2024, 1, 2, 3, 4, 5, 0, location)
	if !ok || !got.Equal(want) || got.Location() != location {
		t.Fatalf("无时区 timestamp 解码=%v, want %v", value, want)
	}
	binary, err := typeMap.Encode(pgtype.TimestampOID, pgtype.BinaryFormatCode, time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC), nil)
	if err != nil {
		t.Fatal(err)
	}
	value, err = timestampType.Codec.DecodeDatabaseSQLValue(typeMap, pgtype.TimestampOID, pgtype.BinaryFormatCode, binary)
	if err != nil {
		t.Fatal(err)
	}
	got, ok = value.(time.Time)
	if !ok || !got.Equal(want) || got.Location() != location {
		t.Fatalf("二进制 timestamp 解码=%v, want %v", value, want)
	}
	timestamptzAfter, _ := typeMap.TypeForOID(pgtype.TimestamptzOID)
	if timestamptzAfter != timestamptzBefore {
		t.Fatal("Location 不应修改 timestamptz 解码器")
	}
}

func TestPrepareRejectsUnsupportedCharsetAndLocation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		endpoint Endpoint
		want     string
	}{
		{
			name: "charset",
			endpoint: Endpoint{
				Host: "127.0.0.1", Database: "orders", Username: "reader", Charset: "LATIN1",
			},
			want: "unsupported charset",
		},
		{
			name: "location",
			endpoint: Endpoint{
				Host: "127.0.0.1", Database: "orders", Username: "reader", Location: "Invalid/Location",
			},
			want: "invalid location",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := prepare(tc.endpoint, "master", "", false)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("无效配置错误=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestNewRejectsInvalidConfigurationWithoutSecrets(t *testing.T) {
	valid := validEndpoint()
	cases := []Config{
		{},
		{Name: " \t "},
		{Name: "orders", Master: Endpoint{Database: "orders", Username: "reader", Password: "secret-marker"}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "reader", Password: "secret-marker", Port: 65536}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "reader", Password: "secret-marker", DialTimeout: -time.Second}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "reader", Password: "secret-marker", Charset: "LATIN1"}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "reader", Password: "secret-marker", Location: "Invalid/Location"}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "reader", Password: "secret-marker", SSLMode: "invalid"}},
		{Name: "orders", Master: valid, Slaves: []Endpoint{{Host: "127.0.0.1", Username: "reader"}}},
		{Name: "orders", Master: valid, SlowThreshold: -time.Millisecond},
	}
	for _, cfg := range cases {
		client, err := New(context.Background(), cfg)
		if client != nil || err == nil || strings.Contains(err.Error(), "secret-marker") {
			t.Fatalf("无效配置或密码泄露: client=%v err=%v", client, err)
		}
	}
	if _, err := New(context.Background(), Config{Name: " \t "}); err == nil || !strings.Contains(err.Error(), "empty database name") {
		t.Fatalf("全空白名称未按空名称拒绝: %v", err)
	}
}

func TestNewLabelsEndpointsByRoleAndPosition(t *testing.T) {
	valid := validEndpoint()
	_, err := New(context.Background(), Config{
		Name: "orders",
		Master: Endpoint{
			Database: "orders",
			Username: "reader",
		},
	})
	if err == nil || !strings.Contains(err.Error(), `endpoint "master"`) {
		t.Fatalf("主库配置错误缺少端点标识: %v", err)
	}
	_, err = New(context.Background(), Config{
		Name:   "orders",
		Master: valid,
		Slaves: []Endpoint{
			valid,
			{Host: "127.0.0.1", Username: "reader"},
		},
	})
	if err == nil || !strings.Contains(err.Error(), `endpoint "slave-2"`) {
		t.Fatalf("从库配置错误缺少端点序号: %v", err)
	}
}

func TestNewHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(ctx, Config{Name: "orders", Master: validEndpoint()})
	if client != nil || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret-marker") {
		t.Fatalf("已取消启动: client=%v err=%v", client, err)
	}
}

func TestTracePolicyAndSQLState(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "orders", SlowThreshold: 200 * time.Millisecond}, "master", "master")
	called := 0
	query := func() (string, int64) {
		called++
		return "SELECT * FROM orders WHERE token = $1", 1
	}
	l.Trace(ctx, time.Now(), query, nil)
	l.Trace(ctx, time.Now().Add(-time.Second), query, nil)
	l.Trace(ctx, time.Now(), query, &pgconn.PgError{Code: "23505", Message: "duplicate secret"})
	l.Trace(ctx, time.Now(), query, gorm.ErrRecordNotFound)
	l.Trace(ctx, time.Now(), query, fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23503"}))
	records := parseRecords(t, buf)
	if called != 4 || len(records) != 4 || records[0]["level"] != "WARN" ||
		records[1]["level"] != "ERROR" || records[2]["level"] != "ERROR" || records[3]["level"] != "ERROR" {
		t.Fatalf("查询日志分级错误: called=%d records=%v", called, records)
	}
	details := records[1][logit.DownstreamDetailsKey].(map[string]any)
	if records[1]["msg"] != "PostgreSQL" || records[1]["request_id"] != "request-1" ||
		records[1][logit.DownstreamTypeKey] != "PostgreSQL" || records[1][logit.DownstreamIDKey] != "orders" ||
		details["endpoint_type"] != "master" || details["endpoint"] != "master" ||
		details["rows"] != float64(1) || details["sqlstate"] != "23505" ||
		!strings.Contains(details["err"].(string), "duplicate secret") {
		t.Fatalf("错误日志内容错误: %v", records[1])
	}
	if _, ok := records[2][logit.DownstreamDetailsKey].(map[string]any)["sqlstate"]; ok {
		t.Fatalf("普通 GORM 错误不应有 SQLSTATE: %v", records[2])
	}
	if got := records[3][logit.DownstreamDetailsKey].(map[string]any)["sqlstate"]; got != "23503" {
		t.Fatalf("包装后的 PostgreSQL 错误缺少 SQLSTATE: %v", records[3])
	}
}

func TestDiagnosticsAndDuration(t *testing.T) {
	buf, baseCtx := captureRecords(t)
	ctx := logit.WithStart(baseCtx)
	l := newTraceLogger(Config{Name: "orders", SlowThreshold: time.Hour}, "slave", "slave-1")
	l.Info(ctx, "notice %s", "ready")
	l.Warn(ctx, "warning %s", "slow")
	l.Error(ctx, "failure %s", "closed")
	l.LogMode(logger.Warn).Info(ctx, "hidden")
	for range 2 {
		l.Trace(ctx, time.Now().Add(-time.Millisecond), func() (string, int64) {
			t.Fatal("未启用 SQL 日志时不应格式化 SQL")
			return "", 0
		}, nil)
	}
	logit.InfoDuration(ctx, "done")
	records := parseRecords(t, buf)
	if len(records) != 4 || records[0]["level"] != "INFO" || records[1]["level"] != "WARN" ||
		records[2]["level"] != "ERROR" {
		t.Fatalf("诊断日志分级错误: %v", records)
	}
	for i, want := range []string{"notice ready", "warning slow", "failure closed"} {
		details := records[i][logit.DownstreamDetailsKey].(map[string]any)
		if records[i][logit.DownstreamDurationMSKey] != float64(0) ||
			details["endpoint_type"] != "slave" || details["endpoint"] != "slave-1" || details["msg"] != want {
			t.Fatalf("诊断详情错误: %v", records[i])
		}
	}
	for _, key := range []string{"PostgreSQL_1_duration_ms", "PostgreSQL_2_duration_ms"} {
		if _, ok := records[3][key].(float64); !ok {
			t.Fatalf("缺少请求级耗时 %q: %v", key, records[3])
		}
	}
}

func TestGORMDiagnosticCallerAndLogMode(t *testing.T) {
	buf := &bytes.Buffer{}
	l := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(buf)),
		logit.OptCaller(true),
	)
	old := logit.Default()
	logit.SetDefault(l)
	t.Cleanup(func() {
		logit.SetDefault(old)
		_ = logit.Close(l)
	})

	diagnostic := newTraceLogger(Config{Name: "orders", LogSQL: true}, "master", "master")
	ctx := context.Background()
	warn := diagnostic.LogMode(logger.Warn)
	warn.Info(ctx, "hidden info")
	warn.Warn(ctx, "visible warn")
	warn.Error(ctx, "visible error")
	diagnostic.Info(ctx, "original remains info")
	warn.LogMode(logger.Silent).Error(ctx, "hidden error")
	diagnostic.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)

	records := parseRecords(t, buf)
	if len(records) != 4 {
		t.Fatalf("GORM LogMode 日志数=%d, want 4: %v", len(records), records)
	}
	seen := make(map[string]bool, len(records))
	for i, want := range []string{"visible warn", "visible error", "original remains info"} {
		record := records[i]
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		caller, ok := record["caller"].(string)
		if details["msg"] != want || !ok || !strings.HasPrefix(caller, "infra/pgsqlx/log.go:") || seen[caller] {
			t.Fatalf("GORM LogMode 或诊断 caller 错误: %v", records)
		}
		seen[caller] = true
	}
	if caller, ok := records[3]["caller"].(string); !ok || !strings.HasPrefix(caller, "infra/pgsqlx/log.go:") || seen[caller] {
		t.Fatalf("查询日志 caller 未指向 Trace 入口: %v", records[3])
	}
}

type testRow struct{ ID int }

func dryRunDB(t *testing.T, l logger.Interface) *gorm.DB {
	t.Helper()
	conn, err := pgx.ParseConfig("postgres://reader@localhost/orders?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := stdlib.OpenDB(*conn)
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gorm.Open(gormpostgres.New(gormpostgres.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               l,
	})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGORMQueryRoutingAndInterpolation(t *testing.T) {
	buf, ctx := captureRecords(t)
	cfg := Config{Name: "orders", SlowThreshold: time.Hour, LogSQL: true}
	master := dryRunDB(t, newTraceLogger(cfg, "master", "master"))
	slaveA := dryRunDB(t, newTraceLogger(cfg, "slave", "slave-1"))
	slaveB := dryRunDB(t, newTraceLogger(cfg, "slave", "slave-2"))
	client := testClusterClient(t, master, nil, slaveA, slaveB)
	for _, db := range []*gorm.DB{client.MasterDB(ctx), client.SlaveDB(ctx), client.SlaveDB(ctx)} {
		if err := db.Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	records := parseRecords(t, buf)
	if len(records) != 3 || strings.Contains(buf.String(), "hidden-secret") {
		t.Fatalf("占位符日志或路由错误: %v", records)
	}
	for i, name := range []string{"master", "slave-1", "slave-2"} {
		details := records[i][logit.DownstreamDetailsKey].(map[string]any)
		if records[i]["level"] != "INFO" || details["endpoint"] != name ||
			!strings.Contains(details["sql"].(string), "token = $1") {
			t.Fatalf("第 %d 条查询日志错误: %v", i, records[i])
		}
	}

	interpolated := dryRunDB(t, newTraceLogger(Config{
		Name:           "orders",
		SlowThreshold:  time.Hour,
		LogSQL:         true,
		InterpolateSQL: true,
	}, "master", "master"))
	if err := interpolated.WithContext(ctx).Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
		t.Fatal(err)
	}
	records = parseRecords(t, buf)
	if len(records) != 4 || !strings.Contains(records[3][logit.DownstreamDetailsKey].(map[string]any)["sql"].(string), "hidden-secret") {
		t.Fatalf("显式插值未生效: %v", records)
	}
}

func TestSlaveFallbackAndConcurrentSelection(t *testing.T) {
	master := dryRunDB(t, newTraceLogger(Config{Name: "orders"}, "master", "master"))
	slaveA := dryRunDB(t, newTraceLogger(Config{Name: "orders"}, "slave", "slave-1"))
	slaveB := dryRunDB(t, newTraceLogger(Config{Name: "orders"}, "slave", "slave-2"))
	ctx := context.Background()
	client := testClusterClient(t, master, nil)
	if got := client.SlaveDB(ctx); got.Config.Logger != master.Config.Logger || got.Statement.Context != ctx {
		t.Fatalf("无从库未回退主库: %v", got)
	}
	client = testClusterClient(t, master, nil, slaveA, slaveB)
	const count = 100
	selected := make(chan logger.Interface, count)
	var workers sync.WaitGroup
	for range count {
		workers.Go(func() {
			selected <- client.SlaveDB(ctx).Config.Logger
		})
	}
	workers.Wait()
	close(selected)
	seen := map[logger.Interface]int{}
	for source := range selected {
		seen[source]++
	}
	if seen[slaveA.Config.Logger] != count/2 || seen[slaveB.Config.Logger] != count/2 {
		t.Fatalf("并发轮询不均: %v", seen)
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	conn, err := pgx.ParseConfig("postgres://reader@localhost/orders?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool := stdlib.OpenDB(*conn)
	client := testClusterClient(t, nil, pool)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}
	if err := pool.PingContext(context.Background()); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("连接池未关闭: %v", err)
	}
}
