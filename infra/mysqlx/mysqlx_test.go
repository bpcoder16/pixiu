package mysqlx

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	mysqldriver "github.com/go-sql-driver/mysql"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func captureRecords(t *testing.T) (*bytes.Buffer, context.Context) {
	t.Helper()
	buf := &bytes.Buffer{}
	l := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)))
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

func TestGORMDiagnostics(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "orders"}, "slave", "slave-1")
	l.Info(ctx, "replacing callback %s", "audit")
	l.Warn(ctx, "duplicated callback %s", "audit")
	l.Error(ctx, "failed to parse model: %v", errors.New("invalid field"))

	records := parseRecords(t, buf)
	want := []struct{ level, message string }{
		{"INFO", "replacing callback audit"},
		{"WARN", "duplicated callback audit"},
		{"ERROR", "failed to parse model: invalid field"},
	}
	if len(records) != len(want) {
		t.Fatalf("GORM 诊断日志数=%d, want %d: %q", len(records), len(want), buf.String())
	}
	for i, record := range records {
		if record["level"] != want[i].level || record["msg"] != "MySQL" ||
			record["request_id"] != "request-1" || record[logit.DownstreamTypeKey] != "MySQL" ||
			record[logit.DownstreamIDKey] != "orders" || record[logit.DownstreamDurationMSKey] != float64(0) {
			t.Fatalf("第 %d 条诊断日志不正确: %v", i, record)
		}
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok || len(details) != 3 || details["endpoint_type"] != "slave" ||
			details["endpoint"] != "slave-1" || details["msg"] != want[i].message {
			t.Fatalf("第 %d 条诊断详情不正确: %v", i, record)
		}
	}
}

func TestGORMCaller(t *testing.T) {
	buf := &bytes.Buffer{}
	l := logit.MustNew(logit.OptEncoder(logit.DefaultJSONEncoder), logit.OptWriter(logit.NewWriter(buf)), logit.OptCaller(true))
	old := logit.Default()
	logit.SetDefault(l)
	t.Cleanup(func() {
		logit.SetDefault(old)
		_ = logit.Close(l)
	})

	diagnostic := newTraceLogger(Config{
		Name:          "orders",
		SlowThreshold: time.Hour,
		LogSQL:        true,
	}, "master", "master")
	ctx := context.Background()
	diagnostic.Info(ctx, "info")
	diagnostic.Warn(ctx, "warn")
	diagnostic.Error(ctx, "error")
	diagnostic.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)

	records := parseRecords(t, buf)
	if len(records) != 4 {
		t.Fatalf("GORM 日志数=%d, want 4", len(records))
	}
	seen := make(map[string]bool, len(records))
	for _, record := range records {
		caller, ok := record["caller"].(string)
		if !ok || !strings.HasPrefix(caller, "infra/mysqlx/log.go:") || seen[caller] {
			t.Fatalf("GORM 日志 caller 未指向各级别及 Trace 入口: %v", records)
		}
		seen[caller] = true
	}
}

func TestGORMDiagnosticLogMode(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "orders"}, "master", "master")
	warn := l.LogMode(logger.Warn)
	warn.Info(ctx, "hidden info")
	warn.Warn(ctx, "visible warn")
	warn.Error(ctx, "visible error")
	l.Info(ctx, "original logger remains info")
	warn.LogMode(logger.Silent).Error(ctx, "hidden error")

	records := parseRecords(t, buf)
	want := []string{"visible warn", "visible error", "original logger remains info"}
	if len(records) != len(want) {
		t.Fatalf("GORM LogMode 未按会话过滤诊断消息: %v", records)
	}
	for i, record := range records {
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok || details["msg"] != want[i] {
			t.Fatalf("GORM LogMode 未按会话过滤诊断消息: %v", records)
		}
	}
}

func TestTracePolicyAndContext(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "orders", SlowThreshold: 200 * time.Millisecond}, "master", "master")
	called := 0
	query := func() (string, int64) {
		called++
		return "SELECT * FROM orders WHERE token = ?", 1
	}
	l.Trace(ctx, time.Now(), query, nil)
	if called != 0 || len(parseRecords(t, buf)) != 0 {
		t.Fatalf("正常查询触发了 SQL 格式化或日志: called=%d logs=%q", called, buf.String())
	}

	l.Trace(ctx, time.Now().Add(-time.Second), query, nil)
	l.Trace(ctx, time.Now().Add(-time.Second), query, errors.New("secret error value"))
	l.Trace(ctx, time.Now(), query, gorm.ErrRecordNotFound)
	records := parseRecords(t, buf)
	if len(records) != 3 || called != 3 {
		t.Fatalf("日志数=%d SQL 格式化次数=%d, want 3: %q", len(records), called, buf.String())
	}
	if records[0]["level"] != "WARN" || records[1]["level"] != "ERROR" || records[2]["level"] != "ERROR" {
		t.Fatalf("日志级别不正确: %v", records)
	}
	for i, record := range records {
		if record["msg"] != "MySQL" || record["request_id"] != "request-1" ||
			record[logit.DownstreamTypeKey] != "MySQL" || record[logit.DownstreamIDKey] != "orders" {
			t.Fatalf("请求字段或下游字段丢失: %v", record)
		}
		details, ok := record[logit.DownstreamDetailsKey].(map[string]any)
		if !ok || details["endpoint_type"] != "master" || details["endpoint"] != "master" || details["rows"] != float64(1) || details["sql"] != "SELECT * FROM orders WHERE token = ?" {
			t.Fatalf("查询详情不正确: %v", record)
		}
		if _, exists := details["role"]; exists {
			t.Fatalf("日志仍包含旧 role 字段: %v", details)
		}
		if i == 0 {
			if _, ok := details["err"]; ok {
				t.Fatalf("慢查询不应包含错误: %v", details)
			}
		}
	}
	if details := records[1][logit.DownstreamDetailsKey].(map[string]any); details["err"] != "secret error value" {
		t.Fatalf("错误日志缺少原文: %v", details)
	}
	if details := records[2][logit.DownstreamDetailsKey].(map[string]any); details["err"] != gorm.ErrRecordNotFound.Error() {
		t.Fatalf("记录不存在未按错误输出: %v", details)
	}
}

func TestTraceDurationWithoutSQLLog(t *testing.T) {
	buf, baseCtx := captureRecords(t)
	ctx := logit.WithStart(baseCtx)
	l := newTraceLogger(Config{Name: "orders", SlowThreshold: time.Hour}, "master", "master")
	query := func() (string, int64) {
		t.Fatal("禁用 SQL 日志时不应生成 SQL")
		return "", 0
	}
	for i := 0; i < 2; i++ {
		l.Trace(ctx, time.Now().Add(-time.Millisecond), query, nil)
	}
	if buf.Len() != 0 {
		t.Fatalf("禁用 SQL 日志时不应输出查询日志: %q", buf.String())
	}
	logit.InfoDuration(ctx, "request done")
	records := parseRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("耗时汇总日志数=%d, want 1: %v", len(records), records)
	}
	for _, key := range []string{"mysql_1_duration_ms", "mysql_2_duration_ms"} {
		if _, ok := records[0][key].(float64); !ok {
			t.Errorf("缺少下游耗时 %q: %v", key, records[0])
		}
	}
}

func TestTraceInfoAndParameterFilter(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{
		Name: "orders", SlowThreshold: 200 * time.Millisecond, LogSQL: true,
	}, "slave", "slave-1")
	l = l.LogMode(logger.Info).(*traceLogger)
	sql, vars := l.ParamsFilter(ctx, "SELECT * FROM orders WHERE id = ?", "secret")
	if sql != "SELECT * FROM orders WHERE id = ?" || len(vars) != 0 {
		t.Fatalf("SQL 参数没有被清除: sql=%q vars=%v", sql, vars)
	}
	l.Trace(ctx, time.Now(), func() (string, int64) { return sql, -1 }, nil)
	records := parseRecords(t, buf)
	if len(records) != 1 || records[0]["level"] != "INFO" {
		t.Fatalf("正常 SQL 未按 Info 输出: %v", records)
	}
	details := records[0][logit.DownstreamDetailsKey].(map[string]any)
	if details["endpoint_type"] != "slave" || details["sql"] != sql {
		t.Fatalf("Info 日志缺少 SQL: %v", details)
	}
	expanded := newTraceLogger(Config{Name: "orders", InterpolateSQL: true}, "slave", "slave-1")
	sql, vars = expanded.ParamsFilter(ctx, sql, "secret")
	if len(vars) != 1 || vars[0] != "secret" {
		t.Fatalf("插值开关未保留参数: sql=%q vars=%v", sql, vars)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	valid := Endpoint{Host: "127.0.0.1", Port: 3306, Database: "orders", Username: "root", Password: "secret"}
	cases := []Config{
		{},
		{Name: "orders", Master: Endpoint{Database: "orders", Username: "root", Password: "secret"}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "root", Password: "secret", Port: 65536}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "root", Password: "secret", Location: "Invalid/Location"}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "root", Password: "secret", DialTimeout: -time.Second}},
		{Name: "orders", Master: Endpoint{Host: "127.0.0.1", Database: "orders", Username: "root", Password: "secret", Pool: Pool{MaxOpenConns: -1}}},
		{Name: "orders", Master: valid, Slaves: []Endpoint{{Host: "127.0.0.1", Username: "root"}}},
	}
	for _, cfg := range cases {
		client, err := New(context.Background(), cfg)
		if err == nil || client != nil {
			t.Fatalf("无效配置被接受: client=%v err=%v", client, err)
		}
		if strings.Contains(err.Error(), "secret") {
			t.Fatalf("配置错误泄露密码: %v", err)
		}
	}
}

func TestNewRejectsWhitespaceName(t *testing.T) {
	for _, name := range []string{"", " \t"} {
		client, err := New(context.Background(), Config{Name: name})
		if client != nil || err == nil || err.Error() != "mysqlx: empty database name" {
			t.Fatalf("空名称 %q: client=%v err=%v", name, client, err)
		}
	}
}

func TestNewLabelsEndpointsByRoleAndPosition(t *testing.T) {
	valid := Endpoint{Host: "127.0.0.1", Database: "orders", Username: "root"}
	_, err := New(context.Background(), Config{Name: "orders", Master: Endpoint{Database: "orders", Username: "root"}})
	if err == nil || !strings.Contains(err.Error(), `endpoint "master"`) {
		t.Fatalf("主库配置错误缺少端点标识: %v", err)
	}
	_, err = New(context.Background(), Config{Name: "orders", Master: valid, Slaves: []Endpoint{valid, {Host: "127.0.0.1", Username: "root"}}})
	if err == nil || !strings.Contains(err.Error(), `endpoint "slave-2"`) {
		t.Fatalf("从库配置错误缺少端点序号: %v", err)
	}
}

func TestNewHonorsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(ctx, Config{Name: "orders", Master: Endpoint{
		Host: "127.0.0.1", Port: 1, Database: "orders", Username: "root", Password: "secret",
	}})
	if client != nil || !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("已取消启动: client=%v err=%v", client, err)
	}
}

func TestPrepareMapsConnectionFields(t *testing.T) {
	prepared, err := prepare(Endpoint{
		Host: "::1", Port: 3307, Database: "orders",
		Username: "user", Password: "p@ss:word", Charset: "utf8mb4",
		Location: "UTC", TLSConfig: "true",
		DialTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 4 * time.Second,
	}, "master", "")
	if err != nil {
		t.Fatal(err)
	}
	driver := prepared.driver
	if prepared.name != "master" || driver.Net != "tcp" || driver.Addr != "[::1]:3307" || driver.DBName != "orders" ||
		driver.User != "user" || driver.Passwd != "p@ss:word" || !driver.ParseTime ||
		driver.Loc != time.UTC || driver.TLSConfig != "true" ||
		driver.Timeout != 2*time.Second || driver.ReadTimeout != 3*time.Second || driver.WriteTimeout != 4*time.Second {
		t.Fatalf("驱动配置映射错误: addr=%q db=%q user=%q loc=%v", driver.Addr, driver.DBName, driver.User, driver.Loc)
	}
	if !strings.Contains(driver.FormatDSN(), "charset=utf8mb4") {
		t.Fatal("未设置字符集")
	}

	defaults, err := prepare(Endpoint{Host: "localhost", Database: "orders", Username: "root"}, "slave-1", "")
	if err != nil || defaults.name != "slave-1" || defaults.driver.Addr != "localhost:3306" || defaults.driver.Loc.String() != "Asia/Shanghai" ||
		!strings.Contains(defaults.driver.FormatDSN(), "charset=utf8mb4") || len(defaults.driver.Params) != 0 {
		t.Fatalf("默认连接配置错误: err=%v", err)
	}
}

func TestPrepareSessionTimeZone(t *testing.T) {
	endpoint := Endpoint{Host: "localhost", Database: "orders", Username: "root"}
	for _, zone := range []string{"+00:00", "Asia/Shanghai", "SYSTEM"} {
		for _, name := range []string{"master", "slave-1"} {
			prepared, err := prepare(endpoint, name, zone)
			if err != nil {
				t.Fatalf("%s %s: %v", name, zone, err)
			}
			if got, want := prepared.driver.Params["time_zone"], "'"+zone+"'"; got != want {
				t.Fatalf("%s %s: 会话时区参数=%q, want %q", name, zone, got, want)
			}
		}
	}
	for _, zone := range []string{"+00:00'; DROP TABLE orders; --", "Asia/Shanghai\n", "UTC\\x"} {
		_, err := New(context.Background(), Config{Name: "orders", SessionTimeZone: zone, Master: endpoint})
		if err == nil || !strings.Contains(err.Error(), "invalid session time zone") {
			t.Fatalf("无效会话时区 %q 未在建连前拒绝: %v", zone, err)
		}
	}
}

func TestPoolDefaultsRespectOpenLimit(t *testing.T) {
	defaults, err := normalizePool(Pool{})
	if err != nil || defaults.MaxOpenConns != 100 || defaults.MaxIdleConns != 10 {
		t.Fatalf("默认连接数不正确: pool=%+v err=%v", defaults, err)
	}
	pool, err := normalizePool(Pool{MaxOpenConns: 3})
	if err != nil || pool.MaxOpenConns != 3 || pool.MaxIdleConns != 3 || pool.ConnMaxLifetime != 3*time.Minute || pool.ConnMaxIdleTime != time.Minute {
		t.Fatalf("连接池默认值不正确: pool=%+v err=%v", pool, err)
	}
}

type testRow struct{ ID int }

func newDryRunDB(t *testing.T, l logger.Interface) *gorm.DB {
	t.Helper()
	connector, err := mysqldriver.NewConnector(mysqldriver.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	sqlDB := sql.OpenDB(connector)
	t.Cleanup(func() { _ = sqlDB.Close() })
	// DryRun 测试不连接真实 MySQL，因此跳过需要查询服务端的版本探测。
	db, err := gorm.Open(gormmysql.New(gormmysql.Config{
		Conn: sqlDB, SkipInitializeWithVersion: true,
	}), &gorm.Config{DisableAutomaticPing: true, Logger: l})
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func TestGORMTraceAndSlaveRouting(t *testing.T) {
	buf, ctx := captureRecords(t)
	master := newDryRunDB(t, newTraceLogger(Config{Name: "orders", SlowThreshold: time.Second, LogSQL: true}, "master", "master"))
	slaveA := newDryRunDB(t, newTraceLogger(Config{Name: "orders", SlowThreshold: time.Second, LogSQL: true}, "slave", "slave-1"))
	slaveB := newDryRunDB(t, newTraceLogger(Config{Name: "orders", SlowThreshold: time.Second, LogSQL: true}, "slave", "slave-2"))
	client := &Client{master: master, slaves: []*gorm.DB{slaveA, slaveB}}
	for _, db := range []*gorm.DB{client.MasterDB(ctx), client.SlaveDB(ctx), client.SlaveDB(ctx), client.SlaveDB(ctx)} {
		if err := db.Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	records := parseRecords(t, buf)
	if len(records) != 4 {
		t.Fatalf("查询日志数=%d, want 4: %q", len(records), buf.String())
	}
	for i, want := range []string{"master", "slave-1", "slave-2", "slave-1"} {
		record := records[i]
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		wantType := "slave"
		if want == "master" {
			wantType = "master"
		}
		if details["endpoint_type"] != wantType || details["endpoint"] != want || record["request_id"] != "request-1" || record["level"] != "INFO" {
			t.Fatalf("第 %d 次查询路由或 context 错误: %v", i, record)
		}
		if sql, ok := details["sql"].(string); !ok || !strings.Contains(sql, "token = ?") {
			t.Fatalf("GORM 未输出占位符 SQL: %v", details)
		}
	}
	if strings.Contains(buf.String(), "hidden-secret") {
		t.Fatalf("GORM 日志泄露参数值: %q", buf.String())
	}
}

func TestGORMInterpolatedSQL(t *testing.T) {
	buf, ctx := captureRecords(t)
	db := newDryRunDB(t, newTraceLogger(Config{Name: "orders", SlowThreshold: time.Second, LogSQL: true, InterpolateSQL: true}, "master", "master"))
	if err := db.WithContext(ctx).Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
		t.Fatal(err)
	}
	slowDB := newDryRunDB(t, newTraceLogger(Config{Name: "orders", SlowThreshold: time.Nanosecond, InterpolateSQL: true}, "slave", "slave-1"))
	if err := slowDB.WithContext(ctx).Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
		t.Fatal(err)
	}
	records := parseRecords(t, buf)
	if len(records) != 2 || records[0]["level"] != "INFO" || records[1]["level"] != "WARN" {
		t.Fatalf("插值查询分级不正确: %v", records)
	}
	for _, record := range records {
		details := record[logit.DownstreamDetailsKey].(map[string]any)
		if sql, ok := details["sql"].(string); !ok || !strings.Contains(sql, "token = 'hidden-secret'") {
			t.Fatalf("GORM 未把参数填入日志 SQL: %v", details)
		}
	}
}

func TestSlaveFallbackAndConcurrentSelection(t *testing.T) {
	master := newDryRunDB(t, newTraceLogger(Config{Name: "orders"}, "master", "master"))
	slaveA := newDryRunDB(t, newTraceLogger(Config{Name: "orders"}, "slave", "slave-1"))
	slaveB := newDryRunDB(t, newTraceLogger(Config{Name: "orders"}, "slave", "slave-2"))
	ctx := context.WithValue(context.Background(), testContextKey{}, "request")
	client := &Client{master: master}
	if got := client.SlaveDB(ctx); got.Config.Logger != master.Config.Logger || got.Statement.Context != ctx {
		t.Fatalf("无从库时没有使用主库或传递 context: %v", got)
	}

	client.slaves = []*gorm.DB{slaveA, slaveB}
	const count = 100
	loggers := make(chan logger.Interface, count)
	var workers sync.WaitGroup
	for range count {
		workers.Go(func() {
			db := client.SlaveDB(ctx)
			loggers <- db.Config.Logger
		})
	}
	workers.Wait()
	close(loggers)
	seen := map[logger.Interface]int{}
	for selected := range loggers {
		seen[selected]++
	}
	if seen[slaveA.Config.Logger] != count/2 || seen[slaveB.Config.Logger] != count/2 {
		t.Fatalf("并发轮询分布错误: %v", seen)
	}
}

type testContextKey struct{}

func TestCloseIsIdempotent(t *testing.T) {
	connector, err := mysqldriver.NewConnector(mysqldriver.NewConfig())
	if err != nil {
		t.Fatal(err)
	}
	pool := sql.OpenDB(connector)
	client := &Client{pools: []*sql.DB{pool}}
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
