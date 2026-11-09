//go:build !pixiu_bootstrap_no_pgsql

package bootstrap

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/pgsqlx"
)

func TestPostgreSQLFileConfiguration(t *testing.T) {
	file := filepath.Join(t.TempDir(), "pgsql.orders.yaml")
	const content = `initTimeout: 9s
database: orders
username: app
password: secret
charset: UTF8
location: Asia/Shanghai
sslMode: verify-full
sslRootCert: /certs/ca.pem
sslCert: /certs/client.pem
sslKey: /certs/client.key
dialTimeout: 2s
sessionTimeZone: UTC
slowThreshold: 1s
disableStatementCache: true
pool:
  maxOpenConns: 7
  maxIdleConns: 2
  connMaxLifetime: 4m
  connMaxIdleTime: 2m
master:
  host: master.example
  port: 5433
slaves:
  - host: slave.example
    port: 5434
`
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := configx.Parse[pgsqlFileConfig](file)
	if err != nil {
		t.Fatal(err)
	}
	got, err := cfg.clientConfig("orders")
	if err != nil {
		t.Fatal(err)
	}
	wantEndpoint := pgsqlx.Endpoint{
		Host:        "master.example",
		Port:        5433,
		Database:    "orders",
		Username:    "app",
		Password:    "secret",
		Charset:     "UTF8",
		Location:    "Asia/Shanghai",
		SSLMode:     "verify-full",
		SSLRootCert: "/certs/ca.pem",
		SSLCert:     "/certs/client.pem",
		SSLKey:      "/certs/client.key",
		DialTimeout: 2 * time.Second,
		Pool: pgsqlx.Pool{
			MaxOpenConns:    7,
			MaxIdleConns:    2,
			ConnMaxLifetime: 4 * time.Minute,
			ConnMaxIdleTime: 2 * time.Minute,
		},
	}
	if !reflect.DeepEqual(got.Master, wantEndpoint) || len(got.Slaves) != 1 {
		t.Fatal("主库配置或从库数量映射错误")
	}
	wantEndpoint.Host, wantEndpoint.Port = "slave.example", 5434
	if !reflect.DeepEqual(got.Slaves[0], wantEndpoint) {
		t.Fatal("从库未复用公共连接配置")
	}
	if got.Name != "orders" || got.InitTimeout != 9*time.Second || got.SessionTimeZone != "UTC" || got.SlowThreshold != time.Second || !got.DisableStatementCache || !got.LogSQL || !got.InterpolateSQL {
		t.Fatal("实例配置或省略日志开关的默认值映射错误")
	}
	if err := os.WriteFile(file, []byte(content+"logSQL: false\ninterpolateSQL: false\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err = configx.Parse[pgsqlFileConfig](file)
	if err != nil {
		t.Fatal(err)
	}
	got, err = cfg.clientConfig("orders")
	if err != nil || got.LogSQL || got.InterpolateSQL {
		t.Fatal("显式 false 必须关闭日志开关")
	}
	if _, err := configx.Parse[pgsqlFileConfig]("conf.example/pgsql.example.yaml"); err != nil {
		t.Fatalf("配置模板应能严格解析: %v", err)
	}
}
