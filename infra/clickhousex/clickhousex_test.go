package clickhousex

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	gormcore "github.com/bpcoder16/pixiu/infra/internal/gorm"
	"github.com/bpcoder16/pixiu/logit"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

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

func TestPrepareConnectionAndPool(t *testing.T) {
	tlsConfig := &tls.Config{ServerName: "clickhouse.example.com"}
	prepared, err := prepare(Endpoint{
		Host:        "::1",
		Port:        8443,
		Database:    "analytics",
		Username:    "reader",
		Password:    "secret",
		Protocol:    clickhouse.HTTP,
		TLS:         tlsConfig,
		DialTimeout: 2 * time.Second,
		ReadTimeout: 3 * time.Second,
		Settings: clickhouse.Settings{
			"max_execution_time": 5,
		},
		Compression: &clickhouse.Compression{Method: clickhouse.CompressionLZ4},
		Pool:        Pool{MaxOpenConns: 3},
	}, "master")
	if err != nil {
		t.Fatal(err)
	}
	opts := prepared.options
	if prepared.name != "master" || len(opts.Addr) != 1 || opts.Addr[0] != "[::1]:8443" ||
		opts.Auth.Database != "analytics" || opts.Auth.Username != "reader" || opts.Auth.Password != "secret" ||
		opts.Protocol != clickhouse.HTTP || opts.TLS != tlsConfig ||
		opts.DialTimeout != 2*time.Second || opts.ReadTimeout != 3*time.Second ||
		opts.Settings["max_execution_time"] != 5 || opts.Compression.Method != clickhouse.CompressionLZ4 ||
		prepared.pool.MaxOpenConns != 3 || prepared.pool.MaxIdleConns != 3 {
		t.Fatal("ClickHouse 驱动配置映射错误")
	}
	defaults, err := prepare(Endpoint{
		Host:     "localhost",
		Database: "analytics",
		Username: "reader",
	}, "slave-1")
	if err != nil || defaults.options.Addr[0] != "localhost:9000" || defaults.pool.MaxOpenConns != 100 ||
		defaults.pool.MaxIdleConns != 10 || defaults.pool.ConnMaxLifetime != 3*time.Minute ||
		defaults.pool.ConnMaxIdleTime != time.Minute {
		t.Fatalf("原生协议与连接池默认值错误: %+v err=%v", defaults, err)
	}
	httpDefault, err := prepare(Endpoint{
		Host:     "localhost",
		Database: "analytics",
		Username: "reader",
		Protocol: clickhouse.HTTP,
	}, "slave-2")
	if err != nil || httpDefault.options.Addr[0] != "localhost:8123" {
		t.Fatalf("HTTP 默认端口错误: %+v err=%v", httpDefault, err)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	if _, err := New(Config{Name: " \t "}); err == nil || err.Error() != "clickhousex: empty database name" {
		t.Fatalf("仅含空白的逻辑库名未被拒绝: %v", err)
	}
	valid := Endpoint{
		Host:     "127.0.0.1",
		Database: "analytics",
		Username: "reader",
		Password: "secret",
	}
	cases := []Config{
		{},
		{
			Name: "analytics",
			Master: Endpoint{
				Database: "analytics",
				Username: "reader",
			},
		},
		{
			Name: "analytics",
			Master: Endpoint{
				Host:     "127.0.0.1",
				Username: "reader",
			},
		},
		{
			Name: "analytics",
			Master: Endpoint{
				Host:     "127.0.0.1",
				Database: "analytics",
			},
		},
		{
			Name: "analytics",
			Master: Endpoint{
				Host:     "127.0.0.1",
				Database: "analytics",
				Username: "reader",
				Port:     65536,
			},
		},
		{
			Name: "analytics",
			Master: Endpoint{
				Host:        "127.0.0.1",
				Database:    "analytics",
				Username:    "reader",
				ReadTimeout: -time.Second,
			},
		},
		{
			Name: "analytics",
			Master: Endpoint{
				Host:     "127.0.0.1",
				Database: "analytics",
				Username: "reader",
				Pool:     Pool{MaxOpenConns: -1},
			},
		},
		{
			Name:   "analytics",
			Master: valid,
			Slaves: []Endpoint{{
				Host:     "127.0.0.1",
				Username: "reader",
			}},
		},
		{
			Name:          "analytics",
			Master:        valid,
			SlowThreshold: -time.Second,
		},
	}
	for _, cfg := range cases {
		client, err := New(cfg)
		if err == nil || client != nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("无效配置未正确拒绝或泄露密码: client=%v err=%v", client, err)
		}
	}
	_, err := New(Config{
		Name:   "analytics",
		Master: valid,
		Slaves: []Endpoint{
			valid,
			{
				Host:     "127.0.0.1",
				Username: "reader",
			},
		},
	})
	if err == nil || !strings.Contains(err.Error(), `endpoint "slave-2"`) {
		t.Fatalf("从库错误缺少端点序号: %v", err)
	}
}

func TestGORMCaller(t *testing.T) {
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

	diagnostic := newTraceLogger(Config{
		Name:          "analytics",
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
		if !ok || !strings.HasPrefix(caller, "infra/clickhousex/log.go:") || seen[caller] {
			t.Fatalf("GORM 日志 caller 未指向各级别及 Trace 入口: %v", records)
		}
		seen[caller] = true
	}
}

func TestTracePolicyDiagnosticsAndDuration(t *testing.T) {
	buf, base := captureRecords(t)
	ctx := logit.WithStart(base)
	l := newTraceLogger(Config{
		Name:          "analytics",
		SlowThreshold: 200 * time.Millisecond,
	}, "master", "master")
	called := 0
	query := func() (string, int64) {
		called++
		return "SELECT * FROM events WHERE secret = ?", 2
	}
	l.Trace(ctx, time.Now(), query, nil)
	if called != 0 || len(parseRecords(t, buf)) != 0 {
		t.Fatalf("正常查询不应输出 SQL: %q", buf.String())
	}
	l.Trace(ctx, time.Now().Add(-time.Second), query, nil)
	l.Trace(ctx, time.Now(), query, errors.New("failed secret"))
	l.Trace(ctx, time.Now(), query, gorm.ErrRecordNotFound)
	l.Info(ctx, "callback %s", "ready")
	l.LogMode(logger.Warn).Info(ctx, "hidden")
	records := parseRecords(t, buf)
	if len(records) != 4 || records[0]["level"] != "WARN" || records[1]["level"] != "ERROR" ||
		records[2]["level"] != "ERROR" || records[3]["level"] != "INFO" || called != 3 {
		t.Fatalf("查询日志分级错误: %v", records)
	}
	for _, record := range records {
		if record["msg"] != "ClickHouse" || record["request_id"] != "request-1" ||
			record[logit.DownstreamTypeKey] != "ClickHouse" || record[logit.DownstreamIDKey] != "analytics" {
			t.Fatalf("日志标识或 context 错误: %v", record)
		}
	}
	queryDetails := records[1][logit.DownstreamDetailsKey].(map[string]any)
	if queryDetails["endpoint_type"] != "master" || queryDetails["endpoint"] != "master" ||
		queryDetails["rows"] != float64(2) || queryDetails["sql"] != "SELECT * FROM events WHERE secret = ?" ||
		queryDetails["err"] != "failed secret" {
		t.Fatalf("查询详情错误: %v", queryDetails)
	}
	diagnostic := records[3][logit.DownstreamDetailsKey].(map[string]any)
	if len(diagnostic) != 3 || diagnostic["msg"] != "callback ready" {
		t.Fatalf("诊断详情错误: %v", diagnostic)
	}
	newTraceLogger(Config{
		Name:          "reports",
		SlowThreshold: time.Hour,
	}, "master", "master").Trace(ctx, time.Now(), func() (string, int64) {
		t.Fatal("禁用 SQL 日志时不应生成 SQL")
		return "", 0
	}, nil)
	logit.InfoDuration(ctx, "request done")
	records = parseRecords(t, buf)
	if len(records) != 5 {
		t.Fatalf("耗时汇总缺失: %v", records)
	}
	for _, key := range []string{
		"ClickHouse_analytics_1_duration_ms",
		"ClickHouse_analytics_2_duration_ms",
		"ClickHouse_analytics_3_duration_ms",
		"ClickHouse_analytics_4_duration_ms",
		"ClickHouse_reports_5_duration_ms",
	} {
		if _, ok := records[4][key].(float64); !ok {
			t.Fatalf("缺少 %s: %v", key, records[4])
		}
	}
}

func TestTraceClickHouseExceptionDetails(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{
		Name:          "analytics",
		SlowThreshold: time.Hour,
	}, "master", "master")
	query := func() (string, int64) {
		return "SELECT * FROM missing_table", -1
	}
	l.Trace(ctx, time.Now(), query, errors.Join(
		errors.New("query failed"),
		&clickhouse.Exception{
			Code:       60,
			Name:       "UNKNOWN_TABLE",
			Message:    "table not found",
			StackTrace: "private stack",
		},
	))
	l.Trace(ctx, time.Now(), query, errors.New("HTTP response body: Code: 60"))
	l.Trace(ctx, time.Now(), query, &clickhouse.Exception{
		Code:    999,
		Message: "unnamed error",
	})

	records := parseRecords(t, buf)
	if len(records) != 3 {
		t.Fatalf("查询错误日志数=%d, want 3", len(records))
	}
	first := records[0][logit.DownstreamDetailsKey].(map[string]any)
	if first["clickhouse_code"] != float64(60) || first["clickhouse_name"] != "UNKNOWN_TABLE" {
		t.Fatalf("未提取 ClickHouse 异常字段: %v", first)
	}
	if strings.Contains(buf.String(), "private stack") {
		t.Fatalf("查询错误日志泄露服务端堆栈: %s", buf.String())
	}
	second := records[1][logit.DownstreamDetailsKey].(map[string]any)
	if _, exists := second["clickhouse_code"]; exists {
		t.Fatalf("从普通错误文本提取了错误码: %v", second)
	}
	if _, exists := second["clickhouse_name"]; exists {
		t.Fatalf("普通错误出现异常名称: %v", second)
	}
	third := records[2][logit.DownstreamDetailsKey].(map[string]any)
	if third["clickhouse_code"] != float64(999) {
		t.Fatalf("无名称异常未输出错误码: %v", third)
	}
	if _, exists := third["clickhouse_name"]; exists {
		t.Fatalf("输出了空异常名称: %v", third)
	}
}

func TestTraceUsesNamedLogger(t *testing.T) {
	defaultBuf, base := captureRecords(t)
	namedBuf := &bytes.Buffer{}
	namedLogger := logit.MustNew(
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptWriter(logit.NewWriter(namedBuf)),
	)
	const name = "clickhousex-query-test"
	previous := logit.Named(name)
	logit.SetNamed(name, namedLogger)
	t.Cleanup(func() {
		logit.SetNamed(name, previous)
		_ = logit.Close(namedLogger)
	})

	ctx := logit.WithLoggerName(base, name)
	l := newTraceLogger(Config{
		Name:   "analytics",
		LogSQL: true,
	}, "slave", "slave-1")
	l.Trace(ctx, time.Now(), func() (string, int64) {
		return "SELECT 1", 1
	}, nil)
	if defaultBuf.Len() != 0 {
		t.Fatalf("查询日志误写到默认 Logger: %q", defaultBuf.String())
	}
	records := parseRecords(t, namedBuf)
	if len(records) != 1 || records[0]["request_id"] != "request-1" {
		t.Fatalf("命名 Logger 未收到请求日志: %v", records)
	}
}

type testRow struct{ ID int }

func newDryRunDB(t *testing.T, l logger.Interface) *gorm.DB {
	t.Helper()
	sqlDB := clickhouse.OpenDB(&clickhouse.Options{Addr: []string{"127.0.0.1:9000"}})
	t.Cleanup(func() { _ = sqlDB.Close() })
	db, err := gormcore.Open(sqlDB, gormclickhouse.New(gormclickhouse.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: true,
	}), logger.Default.LogMode(logger.Silent))
	if err != nil {
		t.Fatal(err)
	}
	if db.Config.SkipDefaultTransaction || !db.Config.DisableAutomaticPing {
		t.Fatalf("ClickHouse GORM 初始化配置错误: %+v", db.Config)
	}
	db.Config.Logger = l
	return db
}

func newTestClient(t *testing.T, master *gorm.DB, slaves ...*gorm.DB) *Client {
	t.Helper()
	client := &Client{}
	if err := gormcore.BuildCluster(context.Background(), &client.cluster, master, slaves, func(_ context.Context, db *gorm.DB, _ string) (*gorm.DB, *sql.DB, error) {
		return db, nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	return client
}

func newTestClientWithPool(t *testing.T, pool *sql.DB) *Client {
	t.Helper()
	client := &Client{}
	if err := gormcore.BuildCluster(context.Background(), &client.cluster, pool, []*sql.DB(nil), func(_ context.Context, pool *sql.DB, _ string) (*gorm.DB, *sql.DB, error) {
		return nil, pool, nil
	}); err != nil {
		t.Fatal(err)
	}
	return client
}

func TestGORMRoutingAndParameterFilter(t *testing.T) {
	buf, ctx := captureRecords(t)
	config := Config{
		Name:          "analytics",
		SlowThreshold: time.Second,
		LogSQL:        true,
	}
	master := newDryRunDB(t, newTraceLogger(config, "master", "master"))
	slaveA := newDryRunDB(t, newTraceLogger(config, "slave", "slave-1"))
	slaveB := newDryRunDB(t, newTraceLogger(config, "slave", "slave-2"))
	client := newTestClient(t, master, slaveA, slaveB)
	for _, db := range []*gorm.DB{
		client.MasterDB(ctx),
		client.SlaveDB(ctx),
		client.SlaveDB(ctx),
		client.SlaveDB(ctx),
	} {
		if err := db.Session(&gorm.Session{DryRun: true}).Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
			t.Fatal(err)
		}
	}
	records := parseRecords(t, buf)
	if len(records) != 4 {
		t.Fatalf("GORM 查询日志数=%d: %s", len(records), buf.String())
	}
	for i, want := range []string{"master", "slave-1", "slave-2", "slave-1"} {
		details := records[i][logit.DownstreamDetailsKey].(map[string]any)
		if details["endpoint"] != want || records[i]["level"] != "INFO" ||
			details["sql"] == "" || !strings.Contains(details["sql"].(string), "token = ?") {
			t.Fatalf("查询 %d 路由或 SQL 不正确: %v", i, records[i])
		}
	}
	if strings.Contains(buf.String(), "hidden-secret") {
		t.Fatalf("占位符模式泄露参数: %s", buf.String())
	}
	interpolated := newTraceLogger(Config{
		Name:           "analytics",
		InterpolateSQL: true,
	}, "master", "master")
	_, vars := interpolated.ParamsFilter(ctx, "SELECT ?", "hidden-secret")
	if len(vars) != 1 || vars[0] != "hidden-secret" {
		t.Fatalf("插值开关没有保留参数: %v", vars)
	}
	interpolatedDB := newDryRunDB(t, newTraceLogger(Config{
		Name:           "analytics",
		SlowThreshold:  time.Second,
		LogSQL:         true,
		InterpolateSQL: true,
	}, "master", "master"))
	if err := interpolatedDB.WithContext(ctx).Session(&gorm.Session{DryRun: true}).
		Where("token = ?", "hidden-secret").Find(&[]testRow{}).Error; err != nil {
		t.Fatal(err)
	}
	records = parseRecords(t, buf)
	sql := records[len(records)-1][logit.DownstreamDetailsKey].(map[string]any)["sql"].(string)
	if !strings.Contains(sql, "token = 'hidden-secret'") {
		t.Fatalf("插值 SQL 未包含参数: %q", sql)
	}
}

func TestSlaveFallbackConcurrentSelectionAndClose(t *testing.T) {
	master := newDryRunDB(t, newTraceLogger(Config{Name: "analytics"}, "master", "master"))
	slaveA := newDryRunDB(t, newTraceLogger(Config{Name: "analytics"}, "slave", "slave-1"))
	slaveB := newDryRunDB(t, newTraceLogger(Config{Name: "analytics"}, "slave", "slave-2"))
	ctx := context.Background()
	client := newTestClient(t, master)
	if got := client.SlaveDB(ctx); got.Config.Logger != master.Config.Logger || got.Statement.Context != ctx {
		t.Fatalf("没有从库时未使用主库: %v", got)
	}
	client = newTestClient(t, master, slaveA, slaveB)
	const count = 100
	selected := make(chan *traceLogger, count)
	var workers sync.WaitGroup
	for range count {
		workers.Go(func() {
			selected <- client.SlaveDB(ctx).Config.Logger.(*traceLogger)
		})
	}
	workers.Wait()
	close(selected)
	seen := map[*traceLogger]int{}
	for selectedLogger := range selected {
		seen[selectedLogger]++
	}
	if seen[slaveA.Config.Logger.(*traceLogger)] != count/2 ||
		seen[slaveB.Config.Logger.(*traceLogger)] != count/2 {
		t.Fatalf("并发轮询分布错误: %v", seen)
	}
	pool := clickhouse.OpenDB(&clickhouse.Options{Addr: []string{"127.0.0.1:9000"}})
	client = newTestClientWithPool(t, pool)
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("重复关闭: %v", err)
	}
	if err := pool.PingContext(ctx); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("连接池未关闭: %v", err)
	}
}
