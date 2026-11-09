package sqlitex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/mattn/go-sqlite3"
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
		client, err := New(cfg)
		if client != nil || err == nil {
			t.Fatalf("无效配置被接受: cfg=%+v client=%v err=%v", cfg, client, err)
		}
	}
	if _, err := New(Config{
		Name: "local",
		DSN:  ":memory:",
		Pool: Pool{MaxOpenConns: -1},
	}); err == nil || err.Error() != "sqlitex: negative pool setting" {
		t.Fatalf("连接池错误缺少模块前缀: %v", err)
	}
}

func TestNewRejectsDuplicateMemoryDSNParameters(t *testing.T) {
	for _, dsn := range []string{
		"file:sqlitex-duplicate-mode?mode=rwc&mode=memory&cache=shared",
		"file:sqlitex-duplicate-cache?mode=memory&cache=shared&cache=private",
		"file:sqlitex-duplicate-same?mode=memory&mode=memory&cache=shared",
		"file:sqlitex-duplicate-vfs?vfs=unix&vfs=memdb",
	} {
		client, err := New(Config{
			Name: "local",
			DSN:  dsn,
			Pool: Pool{
				MaxOpenConns:    2,
				ConnMaxLifetime: time.Millisecond,
			},
		})
		if client != nil {
			_ = client.Close()
		}
		if err == nil || !strings.Contains(err.Error(), "duplicate DSN parameter") {
			t.Fatalf("重复内存库参数未被拒绝: dsn=%q err=%v", dsn, err)
		}
	}
}

func TestNewProtectsMemoryDSN(t *testing.T) {
	for _, dsn := range []string{
		"file:%3Amemory%3A",
		"file:%3amemory%3a",
		"file:sample?mode=memory#anchor",
		"sqlitex-test-private?vfs=memdb",
		"%2Fsqlitex-test-private?vfs=memdb",
		"/?vfs=memdb",
		"file:/?vfs=memdb",
		"file://localhost/?vfs=memdb",
		"file:/sqlitex-test-native-memory?mode=memory&vfs=memdb",
		"file:/sqlitex-test-native-private?mode=memory&cache=private&vfs=memdb",
	} {
		for _, tc := range []struct {
			name    string
			pool    Pool
			journal JournalMode
			want    string
		}{
			{
				name: "multiple connections",
				pool: Pool{MaxOpenConns: 2},
				want: "private in-memory database requires one open connection",
			},
			{
				name: "connection lifetime",
				pool: Pool{ConnMaxLifetime: time.Second},
				want: "in-memory database cannot recycle connections",
			},
			{
				name: "connection idle time",
				pool: Pool{ConnMaxIdleTime: time.Second},
				want: "in-memory database cannot recycle connections",
			},
			{
				name:    "WAL",
				journal: JournalModeWAL,
				want:    "WAL requires a file database",
			},
			{
				name:    "lowercase WAL",
				journal: "wal",
				want:    "WAL requires a file database",
			},
			{
				name:    "mixed-case WAL",
				journal: "wAl",
				want:    "WAL requires a file database",
			},
		} {
			t.Run(dsn+"/"+tc.name, func(t *testing.T) {
				client, err := New(Config{
					Name:        "escaped-memory",
					DSN:         dsn,
					Pool:        tc.pool,
					JournalMode: tc.journal,
				})
				if client != nil {
					_ = client.Close()
				}
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("内存库配置未被拒绝: err=%v, want %q", err, tc.want)
				}
			})
		}
	}
}

func TestSharedMemoryUsesSameDatabase(t *testing.T) {
	for _, dsn := range []string{
		"file:%3Amemory%3A?cache=shared",
		"file:%2Fsqlitex-escaped-memdb?vfs=memdb",
		"file:sample?mode=memory&cache=shared#anchor",
		"/sqlitex-test-plain-shared?vfs=memdb",
		"/sqlitex-test-plain-mode-ignored?vfs=memdb&mode=memory",
		"file:/sqlitex-test-native-shared?mode=memory&cache=shared&vfs=memdb",
		"file://localhost/sqlitex-test-authority-shared?mode=memory&cache=shared&vfs=memdb",
		"\\sqlitex-test-backslash-shared?vfs=memdb",
		"file:%5Csqlitex-test-backslash-shared?vfs=memdb",
	} {
		t.Run(dsn, func(t *testing.T) {
			ctx := context.Background()
			client, err := New(Config{
				Name: "escaped-shared",
				DSN:  dsn,
				Pool: Pool{
					MaxOpenConns: 2,
					MaxIdleConns: 2,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			pool, err := client.DB(ctx).DB()
			if err != nil {
				t.Fatal(err)
			}
			first, err := pool.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			second, err := pool.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer second.Close()
			if _, err := first.ExecContext(ctx, "CREATE TABLE escaped_t(v INTEGER)"); err != nil {
				t.Fatal(err)
			}
			var count int
			if err := second.QueryRowContext(ctx, "SELECT COUNT(*) FROM escaped_t").Scan(&count); err != nil {
				t.Fatalf("内存库的两条连接未共享数据: %v", err)
			}
		})
	}
}

func TestEscapedFileDSNPreservesPath(t *testing.T) {
	for _, useURI := range []bool{false, true} {
		t.Run(fmt.Sprintf("URI=%t", useURI), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "%00%3Amemory%3A#data.db")
			dsn := path
			if useURI {
				uri := url.URL{Scheme: "file", Path: path}
				dsn = uri.String()
			}
			client, err := New(Config{
				Name: "escaped-file",
				DSN:  dsn,
				Pool: Pool{
					MaxOpenConns:    2,
					ConnMaxLifetime: time.Minute,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			if err := client.DB(context.Background()).Exec("CREATE TABLE file_t(v INTEGER)").Error; err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("文件路径被额外解码: %v", err)
			}
		})
	}
}

// 默认单连接能打开私有内存库，fragment 内的驱动参数不能影响连接。
func TestMemoryURIFragmentIgnoredByDriver(t *testing.T) {
	for _, foreignKeys := range []*bool{nil, new(true)} {
		client, err := New(Config{
			Name:        "fragment",
			DSN:         "file:sample?mode=memory#anchor&_fk=invalid",
			ForeignKeys: foreignKeys,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = client.Close() })
		if foreignKeys != nil {
			var enabled int
			if err := client.DB(t.Context()).Raw("PRAGMA foreign_keys").Scan(&enabled).Error; err != nil || enabled != 1 {
				t.Fatalf("显式外键设置未生效: enabled=%d err=%v", enabled, err)
			}
		}
	}
}

func TestNewRejectsInvalidURIPath(t *testing.T) {
	client, err := New(Config{
		Name: "invalid-uri",
		DSN:  "file:%zz",
	})
	if client != nil {
		_ = client.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "invalid DSN path") {
		t.Fatalf("无效 URI 路径未被拒绝: %v", err)
	}
}

func TestNewProtectsMemDB(t *testing.T) {
	for _, tc := range []struct {
		cfg  Config
		want string
	}{
		{
			cfg: Config{
				Name: "shared",
				DSN:  "file:/sqlitex-test-memdb-shared?vfs=memdb",
				Pool: Pool{ConnMaxLifetime: time.Millisecond},
			},
			want: "in-memory database cannot recycle connections",
		},
		{
			cfg: Config{
				Name: "plain-shared-lifetime",
				DSN:  "/sqlitex-test-plain-memdb-lifetime?vfs=memdb",
				Pool: Pool{ConnMaxLifetime: time.Millisecond},
			},
			want: "in-memory database cannot recycle connections",
		},
		{
			cfg: Config{
				Name: "plain-shared-idle",
				DSN:  "/sqlitex-test-plain-memdb-idle?vfs=memdb",
				Pool: Pool{ConnMaxIdleTime: time.Millisecond},
			},
			want: "in-memory database cannot recycle connections",
		},
		{
			cfg: Config{
				Name: "private",
				DSN:  "file:sqlitex-test-memdb-private?vfs=memdb",
				Pool: Pool{MaxOpenConns: 2},
			},
			want: "private in-memory database requires one open connection",
		},
	} {
		client, err := New(tc.cfg)
		if client != nil {
			_ = client.Close()
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("memdb 危险池配置未被拒绝: dsn=%q err=%v", tc.cfg.DSN, err)
		}
	}
}

func TestSharedMemDBUsesSameDatabase(t *testing.T) {
	ctx := context.Background()
	client, err := New(Config{
		Name: "shared",
		DSN:  "file:/sqlitex-test-memdb-allow?vfs=memdb",
		Pool: Pool{MaxOpenConns: 2, MaxIdleConns: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	pool, err := client.DB(ctx).DB()
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := pool.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if _, err := first.ExecContext(ctx, "CREATE TABLE shared_t(v INTEGER)"); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := second.QueryRowContext(ctx, "SELECT COUNT(*) FROM shared_t").Scan(&count); err != nil {
		t.Fatalf("具名 memdb 连接未共享数据: %v", err)
	}
}

func TestFilePoolLifetime(t *testing.T) {
	client, err := New(Config{
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

func TestOperationContextDoesNotCloseClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := New(Config{
		Name: "local",
		DSN:  testDSN(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.DB(ctx).Exec("SELECT 1").Error; !errors.Is(err, context.Canceled) {
		t.Fatalf("操作未遵循 context 取消: %v", err)
	}
	if err := client.DB(context.Background()).Exec("SELECT 1").Error; err != nil {
		t.Fatalf("操作 context 取消影响客户端生命周期: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if err := client.DB(context.Background()).Exec("SELECT 1").Error; err == nil {
		t.Fatal("显式关闭后仍可执行查询")
	}
}

// 事务取消触发 database/sql 自动回滚并丢弃连接；内存库可能随最后一条连接关闭而消失。
func TestCanceledTransactionDatabaseLifetime(t *testing.T) {
	for _, tc := range []struct {
		name   string
		dsn    string
		memory bool
	}{
		{
			name:   "private memory",
			dsn:    ":memory:",
			memory: true,
		},
		{
			name:   "shared memory",
			dsn:    "file:sqlitex-test-canceled-tx?mode=memory&cache=shared",
			memory: true,
		},
		{
			name: "file",
			dsn:  testDSN(t),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := New(Config{
				Name: tc.name,
				DSN:  tc.dsn,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			db := client.DB(t.Context())
			if err := db.Exec("CREATE TABLE records (id INTEGER)").Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Exec("INSERT INTO records VALUES (1)").Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			tx := client.DB(ctx).Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			cancel()
			// 等待异步回滚完成，不主动 Rollback，以免改变连接是否被丢弃的行为。
			deadline := time.Now().Add(5 * time.Second)
			for client.pool.Stats().OpenConnections != 0 {
				if time.Now().After(deadline) {
					t.Fatal("事务取消后连接未被回收")
				}
				time.Sleep(time.Millisecond)
			}
			var count int
			err = client.pool.QueryRowContext(t.Context(), "SELECT count(*) FROM records").Scan(&count)
			if tc.memory {
				if err == nil || !strings.Contains(err.Error(), "no such table") {
					t.Fatalf("最后一条连接关闭后内存库应消失: count=%d err=%v", count, err)
				}
			} else if err != nil || count != 1 {
				t.Fatalf("文件库已提交的数据应保留: count=%d err=%v", count, err)
			}
		})
	}
}

func TestFileDatabaseLifecycleAndTransaction(t *testing.T) {
	dsn := testDSN(t)
	ctx := context.Background()
	client, err := New(Config{Name: "local", DSN: dsn})
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
	other, err := New(Config{Name: "local", DSN: dsn})
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
	client, err := New(Config{
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
	client, err := New(Config{
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

func TestTypedOptionsAcceptDriverAliases(t *testing.T) {
	for _, tc := range []struct {
		name        string
		journal     JournalMode
		synchronous SynchronousMode
	}{
		{
			name:        "lowercase",
			journal:     "wal",
			synchronous: "full",
		},
		{
			name:        "mixed case",
			journal:     "wAl",
			synchronous: "FuLl",
		},
		{
			name:        "numeric synchronous",
			journal:     JournalModeWAL,
			synchronous: "2",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, err := New(Config{
				Name:        "driver-aliases",
				DSN:         testDSN(t),
				JournalMode: tc.journal,
				Synchronous: tc.synchronous,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			db := client.DB(context.Background())
			var journal string
			var synchronous int
			if err := db.Raw("PRAGMA journal_mode").Scan(&journal).Error; err != nil {
				t.Fatal(err)
			}
			if err := db.Raw("PRAGMA synchronous").Scan(&synchronous).Error; err != nil {
				t.Fatal(err)
			}
			if journal != "wal" || synchronous != 2 {
				t.Fatalf("驱动支持的配置别名未生效: journal=%s synchronous=%d", journal, synchronous)
			}
		})
	}
}

func TestTypedOptionsCanDisableForeignKeys(t *testing.T) {
	foreignKeys := false
	client, err := New(Config{
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
			client, err := New(tc.cfg)
			if client != nil || err == nil {
				t.Fatalf("无效配置被接受: client=%v err=%v", client, err)
			}
		})
	}
}

func TestNamedSharedMemoryUsesOneDatabase(t *testing.T) {
	ctx := context.Background()
	client, err := New(Config{
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
	client, err := New(Config{Name: "local", DSN: ":memory:"})
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
	newTraceLogger(Config{
		Name:          "archive",
		SlowThreshold: time.Hour,
	}).Trace(ctx, time.Now(), query, nil)
	if buf.Len() != 0 {
		t.Fatalf("普通查询产生了日志: %s", buf.String())
	}
	logit.InfoDuration(ctx, "request done")
	records := parseRecords(t, buf)
	if len(records) != 1 {
		t.Fatalf("耗时汇总日志数=%d, want 1", len(records))
	}
	for _, key := range []string{"SQLite_local_1_duration_ms", "SQLite_archive_2_duration_ms"} {
		if _, ok := records[0][key].(float64); !ok {
			t.Fatalf("缺少 SQLite 请求耗时 %q: %v", key, records[0])
		}
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

func TestTraceSQLiteErrorDetails(t *testing.T) {
	buf, ctx := captureRecords(t)
	l := newTraceLogger(Config{Name: "local"})
	query := func() (string, int64) {
		return "INSERT INTO records VALUES (?)", 0
	}
	l.Trace(ctx, time.Now(), query, fmt.Errorf("insert: %w", sqlite3.Error{
		Code:         sqlite3.ErrConstraint,
		ExtendedCode: sqlite3.ErrConstraintUnique,
	}))
	l.Trace(ctx, time.Now(), query, gorm.ErrRecordNotFound)
	records := parseRecords(t, buf)
	if len(records) != 2 {
		t.Fatalf("错误日志数=%d, want 2: %v", len(records), records)
	}
	first := records[0][logit.DownstreamDetailsKey].(map[string]any)
	if first["sqlite_code"] != float64(sqlite3.ErrConstraint) ||
		first["sqlite_extended_code"] != float64(sqlite3.ErrConstraintUnique) {
		t.Fatalf("SQLite 错误码日志错误: %v", first)
	}
	second := records[1][logit.DownstreamDetailsKey].(map[string]any)
	if _, ok := second["sqlite_code"]; ok {
		t.Fatalf("非驱动错误不应包含 SQLite 错误码: %v", second)
	}
	if _, ok := second["sqlite_extended_code"]; ok {
		t.Fatalf("非驱动错误不应包含 SQLite 扩展错误码: %v", second)
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
	client, err := New(Config{
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
	if err := client.DB(ctx).Create(&testRecord{ID: 1, Token: "first"}).Error; err != nil {
		t.Fatal(err)
	}
	buf.Reset()
	if err := client.DB(ctx).Create(&testRecord{ID: 1, Token: "duplicate"}).Error; err == nil {
		t.Fatal("重复主键写入未返回错误")
	}
	records = parseRecords(t, buf)
	if len(records) != 1 || records[0]["level"] != "ERROR" {
		t.Fatalf("重复主键错误日志不正确: %v", records)
	}
	details = records[0][logit.DownstreamDetailsKey].(map[string]any)
	if details["sqlite_code"] != float64(sqlite3.ErrConstraint) ||
		details["sqlite_extended_code"] != float64(sqlite3.ErrConstraintPrimaryKey) {
		t.Fatalf("真实 SQLite 查询缺少错误码: %v", details)
	}
	buf.Reset()
	interpolated, err := New(Config{
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

func TestScanLoggingRespectsInterpolation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		query     string
		logSQL    bool
		threshold time.Duration
		level     string
	}{
		{
			name:      "普通查询",
			query:     "SELECT ?",
			logSQL:    true,
			threshold: time.Hour,
			level:     "INFO",
		},
		{
			name:      "关闭普通日志的慢查询",
			query:     "SELECT ?",
			threshold: time.Nanosecond,
			level:     "WARN",
		},
		{
			name:      "关闭普通日志的错误查询",
			query:     "SELECT ? FROM missing_records",
			threshold: time.Hour,
			level:     "ERROR",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf, ctx := captureRecords(t)
			// 两种配置同时存活，验证后创建的客户端不会改变前一个客户端的过滤规则。
			var clients []*Client
			for _, interpolate := range []bool{false, true} {
				client, err := New(Config{
					Name:           fmt.Sprintf("scan-%t", interpolate),
					DSN:            ":memory:",
					LogSQL:         tc.logSQL,
					SlowThreshold:  tc.threshold,
					InterpolateSQL: interpolate,
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = client.Close() })
				clients = append(clients, client)
			}
			for i, client := range clients {
				buf.Reset()
				var value string
				err := client.DB(ctx).Raw(tc.query, "scan-secret").Scan(&value).Error
				if (err != nil) != (tc.level == "ERROR") {
					t.Fatalf("查询错误与预期不符: %v", err)
				}
				if err == nil && value != "scan-secret" {
					t.Fatalf("日志过滤改变了实际绑定参数: %q", value)
				}
				records := parseRecords(t, buf)
				if len(records) != 1 || records[0]["level"] != tc.level {
					t.Fatalf("Scan 日志数量或级别错误: %s", buf.String())
				}
				sql := records[0][logit.DownstreamDetailsKey].(map[string]any)["sql"].(string)
				if i == 0 && sql != tc.query {
					t.Errorf("关闭插值后 SQL 应保留占位符: %s", sql)
				}
				if i == 1 && !strings.Contains(sql, "scan-secret") {
					t.Errorf("开启插值后 SQL 应包含绑定参数: %s", sql)
				}
			}
		})
	}
}

func TestNewRejectsAmbiguousURIPath(t *testing.T) {
	for _, tc := range []struct {
		dsn    string
		reason string
	}{
		{dsn: "file:", reason: "empty"},
		{dsn: "file:?cache=private", reason: "empty"},
		{dsn: "file://", reason: "empty"},
		{dsn: "file://localhost", reason: "empty"},
		{dsn: "file::memory:%00tail", reason: "NUL"},
	} {
		t.Run(tc.dsn, func(t *testing.T) {
			client, err := New(Config{
				Name: "local",
				DSN:  tc.dsn,
				Pool: Pool{MaxOpenConns: 2},
			})
			if client != nil {
				_ = client.Close()
				t.Fatal("不明确的文件路径通过了连接池校验")
			}
			if err == nil || !strings.Contains(err.Error(), "DSN") || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("应指出 DSN 路径原因 %q: %v", tc.reason, err)
			}
		})
	}
}

func TestNewRejectsNULDSN(t *testing.T) {
	dsn := testDSN(t)
	for _, invalid := range []string{
		":memory:\x00tail",
		dsn + "&mode=memory\x00tail",
		dsn + "&mode=memory%00tail",
		dsn + "&mode%00tail=memory",
		dsn + "&cache=shared%00tail",
		dsn + "&cache%00tail=shared",
		dsn + "&vfs=memdb%00tail",
		dsn + "&vfs%00tail=memdb",
	} {
		t.Run(invalid, func(t *testing.T) {
			client, err := New(Config{
				Name: "nul-dsn",
				DSN:  invalid,
				Pool: Pool{MaxOpenConns: 2},
			})
			if client != nil {
				_ = client.Close()
				t.Fatal("含 NUL 的 DSN 被接受")
			}
			if err == nil || !strings.Contains(err.Error(), "NUL") || !strings.Contains(err.Error(), "DSN") {
				t.Fatalf("未指出 DSN 中的 NUL: %v", err)
			}
		})
	}
}

func TestNewPassesUnrelatedNULParametersToDriver(t *testing.T) {
	for _, query := range []string{
		"extra=%00",
		"extra=ok&extra=%00",
		"extra%00tail=memory",
		"mode_extra%00tail=memory",
	} {
		t.Run(query, func(t *testing.T) {
			client, err := New(Config{
				Name:        "driver-parameters",
				DSN:         testDSN(t) + "&" + query,
				BusyTimeout: time.Second,
				Pool: Pool{
					MaxOpenConns: 2,
					MaxIdleConns: 2,
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			first, err := client.pool.Conn(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			defer first.Close()
			if _, err := first.ExecContext(t.Context(), "CREATE TABLE records (id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			// 保持第一条连接占用，确认另一条连接仍访问同一个文件库。
			var count int
			if err := client.pool.QueryRowContext(t.Context(), "SELECT count(*) FROM records").Scan(&count); err != nil {
				t.Fatalf("无关参数改变了文件库的连接行为: %v", err)
			}
		})
	}
}
