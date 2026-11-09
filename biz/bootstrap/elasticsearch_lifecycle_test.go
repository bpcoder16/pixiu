//go:build !pixiu_bootstrap_no_es && !pixiu_bootstrap_no_mysql && !pixiu_bootstrap_no_redis

package bootstrap_test

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func TestElasticsearchVersionsTemplatesAndLifecycle(t *testing.T) {
	for _, defaultVersion := range []int{0, 7, 8, 9} {
		t.Run(fmt.Sprint(defaultVersion), func(t *testing.T) {
			isolatedLog(t, func() {
				template, err := os.ReadFile("conf.example/elasticsearch.example.yaml")
				if err != nil {
					t.Fatal(err)
				}
				cfg := loadLogConfig(t, "release", "log:\n  format: text\n")
				var servers []*bootstrapESServer
				for _, version := range []int{7, 8, 9} {
					s := newBootstrapESServer(t, version, true, "Basic dGVzdHVzZXI6c2VjcmV0", 0)
					servers = append(servers, s)
					cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.Certificate().Raw})
					certPath := filepath.Join(env.ConfigDirPath(), fmt.Sprintf("ca%d.pem", version))
					if err := os.WriteFile(certPath, cert, 0o600); err != nil {
						t.Fatal(err)
					}
					if version != 9 {
						certPath = filepath.Base(certPath)
					}
					content := strings.Replace(string(template), "version: 8", fmt.Sprintf("version: %d", version), 1)
					content = strings.Replace(content, "https://es.example.com:9200", s.URL, 1)
					content = strings.Replace(content, `username: ""`, `username: "testuser"`, 1)
					content = strings.Replace(content, `password: ""`, `password: "secret"`, 1)
					content = strings.Replace(content, `caCertFile: ""`, fmt.Sprintf("caCertFile: %q", certPath), 1)
					name := fmt.Sprintf("v%d", version)
					writeESConfig(t, name, content)
					bootstrap.MustRegisterElasticsearch(name, version == defaultVersion)
				}
				// 名称可与 MySQL、Redis 共存，文件前缀变更不能破坏旧组件。
				database := newBootstrapMySQLServer(t, "8.0.36", false)
				cache := newBootstrapRedisServer(t, "cache", "+PONG\r\n")
				writeMySQLConfig(t, "v8", mysqlConfigYAML(database.port))
				writeRedisConfig(t, "v8", redisConfigYAML(cache.port))
				bootstrap.MustRegisterMySQL("v8", true)
				bootstrap.MustRegisterRedis("v8", true)
				t.Chdir(t.TempDir())
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				ctx := context.Background()
				for _, version := range []int{7, 8, 9} {
					client := elasticsearchx.Named(fmt.Sprintf("v%d", version))
					if count, err := client.Count(ctx, "products", map[string]any{}); err != nil || count != int64(version) {
						t.Fatalf("版本路由错误: %d, %v", count, err)
					}
					if defaultVersion == version && elasticsearchx.Default() != client {
						t.Fatal("默认与命名实例必须共享")
					}
				}
				if defaultVersion == 0 && recoverValue(func() { elasticsearchx.Default() }) == nil {
					t.Fatal("不得自动设置默认实例")
				}
				client := elasticsearchx.Named("v8")
				requestCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
				if _, err := client.Count(requestCtx, "wait", map[string]any{}); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("应保留业务超时: %v", err)
				}
				if _, err := client.Count(ctx, "products", map[string]any{}); err != nil {
					t.Fatalf("请求取消不能关闭共享客户端: %v", err)
				}
				want := errors.New("worker close failed")
				if err := resources.Register(func() error {
					if _, err := client.Count(ctx, "products", map[string]any{}); err != nil {
						t.Errorf("项目资源关闭时 ES 应仍可用: %v", err)
					}
					if err := redisx.Default().Client().Ping(ctx).Err(); err != nil {
						t.Error(err)
					}
					pool, err := mysqlx.Default().MasterDB(ctx).DB()
					if err != nil || pool.Ping() != nil {
						t.Error("MySQL 提前关闭")
					}
					logit.Info(ctx, "project closing")
					return want
				}); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := resources.Close(); !errors.Is(err, want) {
						t.Fatalf("关闭错误丢失: %v", err)
					}
				}
				for _, server := range servers {
					server.requireClosed(t)
				}
				cache.requireClosed(t)
				database.requireClosed(t)
				if recoverValue(func() { elasticsearchx.Named("v8") }) == nil {
					t.Fatal("关闭后不得查询注册表")
				}
				if _, err := client.Count(ctx, "products", map[string]any{}); err == nil {
					t.Fatal("关闭后的客户端不得重新建连")
				}
				if stats := logit.Default().(logit.WriteErrorStats); stats.WriteErrors() != 0 {
					t.Fatalf("日志提前关闭: %v", stats.LastWriteError())
				}
			})
		})
	}
}
