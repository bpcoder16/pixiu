package bootstrap_test

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/elasticsearchx"
	esv8 "github.com/bpcoder16/pixiu/infra/elasticsearchx/v8"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/mysqlx"
	"github.com/bpcoder16/pixiu/infra/redisx"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

type bootstrapESServer struct {
	*httptest.Server
	opened  atomic.Int32
	closed  atomic.Int32
	waiting chan struct{}
}

// bootstrap 会设置 time.Local，模拟服务须等客户端开始请求后才能接收连接并读取时间。
// 使用 Proxy 回调建立显式同步；仅依赖 TCP 到达顺序不足以建立 Go 的 happens-before。
type bootstrapESListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (l *bootstrapESListener) start() {
	l.once.Do(func() { close(l.ready) })
}

func (l *bootstrapESListener) Accept() (net.Conn, error) {
	<-l.ready
	return l.Listener.Accept()
}

func (l *bootstrapESListener) Close() error {
	err := l.Listener.Close()
	l.start()
	return err
}

// 配置和关闭测试经过真实 SDK；只模拟产品信息、Count 与请求取消。
func newBootstrapESServer(t *testing.T, version int, tls bool, auth string, startupStatus int) *bootstrapESServer {
	t.Helper()
	s := &bootstrapESServer{waiting: make(chan struct{}, 1)}
	s.Server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 读完请求体后服务端才能持续检测客户端断开，验证取消时不残留阻塞处理器。
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("Authorization") != auth {
			t.Error("认证配置未传递给 SDK")
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/" {
			if startupStatus == -1 {
				<-r.Context().Done()
				return
			}
			if startupStatus != 0 {
				w.WriteHeader(startupStatus)
				return
			}
			fmt.Fprintf(w, `{"version":{"number":"%d.0.0","build_flavor":"default"},"tagline":"You Know, for Search"}`, version)
			return
		}
		if r.URL.Path == "/wait/_count" {
			select {
			case s.waiting <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		if r.URL.Path == "/error/_count" {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"error":{"type":"unavailable"}}`)
			return
		}
		fmt.Fprintf(w, `{"count":%d}`, version)
	}))
	listener := &bootstrapESListener{Listener: s.Listener, ready: make(chan struct{})}
	s.Listener = listener
	original := http.DefaultTransport
	transport := original.(*http.Transport).Clone()
	proxy := transport.Proxy
	transport.Proxy = func(r *http.Request) (*url.URL, error) {
		listener.start()
		if proxy != nil {
			return proxy(r)
		}
		return nil, nil
	}
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = original })
	s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			s.opened.Add(1)
		case http.StateClosed:
			s.closed.Add(1)
		}
	}
	if tls {
		s.StartTLS()
	} else {
		s.Start()
	}
	t.Cleanup(s.Close)
	return s
}

func (s *bootstrapESServer) requireClosed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for s.closed.Load() != s.opened.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.opened.Load() == 0 || s.closed.Load() != s.opened.Load() {
		t.Fatalf("ES 连接未释放: opened=%d closed=%d", s.opened.Load(), s.closed.Load())
	}
}

func writeESConfig(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "elasticsearch."+name+".yaml"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func esConfigYAML(s *bootstrapESServer, version int) string {
	return fmt.Sprintf("version: %d\naddresses: [%q]\nstartupTimeout: 1s\n", version, s.URL)
}

func requireESPanic(t *testing.T, want string, run func()) error {
	t.Helper()
	value := recoverValue(run)
	err, ok := value.(error)
	if !ok || !strings.Contains(err.Error(), want) {
		t.Fatalf("应 panic 并包含 %q: %v", want, value)
	}
	return err
}

func TestElasticsearchRegistration(t *testing.T) {
	isolatedLog(t, func() {
		for _, name := range []string{"", " ", "../search", "a.b", "搜索"} {
			requireESPanic(t, "invalid Elasticsearch name", func() { bootstrap.MustRegisterElasticsearch(name, true) })
		}
		bootstrap.MustRegisterElasticsearch("search_1-main", true)
		requireESPanic(t, "already registered", func() { bootstrap.MustRegisterElasticsearch("search_1-main", false) })
		requireESPanic(t, "default Elasticsearch", func() { bootstrap.MustRegisterElasticsearch("other", true) })
		if recoverValue(func() { elasticsearchx.Default() }) == nil {
			t.Fatal("声明阶段不得创建客户端")
		}
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
		var resources lifecycle.Stack
		defer resources.Close()
		err := requireESPanic(t, "elasticsearch.search_1-main.yaml", func() { bootstrap.MustBaseInit(cfg, &resources) })
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatal("缺失文件错误链丢失")
		}
		requireESPanic(t, "initialization has started", func() { bootstrap.MustRegisterElasticsearch("late", false) })
		requireESPanic(t, "initialization has started", func() { bootstrap.MustBaseInit(cfg, &resources) })
	})
}

func TestElasticsearchUndeclaredModuleIsUntouched(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
		writeESConfig(t, "unused", "invalid: [")
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		_, err := esv8.NewNamed(elasticsearchx.Config{Name: "probe"})
		if err == nil || strings.Contains(err.Error(), "clients closed") {
			t.Fatalf("未声明 ES 不应登记 CloseAll: %v", err)
		}
	})
}

func TestElasticsearchPreparesAllFilesBeforeConnecting(t *testing.T) {
	for _, tc := range []struct{ name, content, want string }{
		{"missing", "", "elasticsearch.other.yaml"},
		{"unknown", "version: 8\ntypo: true", "typo"},
		{"identity", "version: 8\nname: other", "name"},
		{"default", "version: 8\nisDefault: true", "isdefault"},
		{"version", "version: 6", "version"},
		{"missing-version", "{}", "version"},
		{"version-type", "version: '8'", "version"},
		{"duration", "version: 8\nstartupTimeout: later", "startupTimeout"},
		{"pool", "version: 8\npool:\n  typo: 3", "typo"},
		{"ca-missing", "version: 8\ncaCertFile: missing.pem", "missing.pem"},
		{"ca-empty", "version: 8\ncaCertFile: empty.pem", "empty CA"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				server := newBootstrapESServer(t, 8, false, "", 0)
				writeESConfig(t, "first", esConfigYAML(server, 8))
				if tc.content != "" {
					writeESConfig(t, "other", tc.content)
				}
				if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "empty.pem"), nil, 0o600); err != nil {
					t.Fatal(err)
				}
				bootstrap.MustRegisterElasticsearch("first", true)
				bootstrap.MustRegisterElasticsearch("other", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireESPanic(t, tc.want, func() { bootstrap.MustBaseInit(cfg, &resources) })
				if !strings.Contains(err.Error(), filepath.Join(env.ConfigDirPath(), "elasticsearch.other.yaml")) || server.opened.Load() != 0 {
					t.Fatalf("应在建连前完成全部配置准备并保留路径: %v", err)
				}
			})
		})
	}
}

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
					s := newBootstrapESServer(t, version, true, "Basic Yml1Y2FyZHM6c2VjcmV0", 0)
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

func TestElasticsearchStartupFailureCleanup(t *testing.T) {
	for _, tc := range []struct {
		name            string
		version, status int
	}{
		{"version", 9, 0},
		{"unauthorized", 8, http.StatusUnauthorized},
		{"timeout", 8, -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
				first := newBootstrapESServer(t, 8, false, "", 0)
				failed := newBootstrapESServer(t, tc.version, false, "", tc.status)
				writeESConfig(t, "first", esConfigYAML(first, 8))
				writeESConfig(t, "failed", strings.Replace(esConfigYAML(failed, 8), "1s", "100ms", 1))
				bootstrap.MustRegisterElasticsearch("first", true)
				bootstrap.MustRegisterElasticsearch("failed", false)
				var resources lifecycle.Stack
				defer resources.Close()
				err := requireESPanic(t, "elasticsearch.failed.yaml", func() { bootstrap.MustBaseInit(cfg, &resources) })
				if tc.name == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("启动超时错误链丢失: %v", err)
				}
				failed.requireClosed(t)
				if first.closed.Load() != 0 || recoverValue(func() { elasticsearchx.Default() }) != nil {
					t.Fatal("先前成功实例应交给应用栈关闭")
				}
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				first.requireClosed(t)
			})
		})
	}
}

func TestElasticsearchLoggingOptions(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options string
		logs    bool
		details bool
	}{
		{name: "omitted", logs: true},
		{name: "enabled", options: "logRequests: true\nlogDetails: true\n", logs: true, details: true},
		{name: "disabled", options: "logRequests: false\nlogDetails: true\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "debug", "log:\n  format: json\n  names: [search]\n")
				server := newBootstrapESServer(t, 8, false, "APIKey test-key", 0)
				writeESConfig(t, "search", esConfigYAML(server, 8)+"apiKey: test-key\nslowThreshold: 1h\n"+tc.options)
				bootstrap.MustRegisterElasticsearch("search", true)
				var resources lifecycle.Stack
				defer resources.Close()
				bootstrap.MustBaseInit(cfg, &resources)
				ctx := logit.WithStart(logit.WithLoggerName(context.Background(), "search"))
				client := elasticsearchx.Default()
				if _, err := client.Count(ctx, "products", map[string]any{"size": 1}); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Count(ctx, "error", map[string]any{}); err == nil {
					t.Fatal("应保留 ES HTTP 错误")
				}
				logit.InfoDuration(ctx, "completed")
				if err := resources.Close(); err != nil {
					t.Fatal(err)
				}
				for _, suffix := range []string{"info", "wf"} {
					data, err := os.ReadFile(filepath.Join(env.RootDirPath(), "log", "bootstrap-test.search."+suffix+".log"))
					if err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(data), `"msg":"elasticsearch"`) != tc.logs || strings.Contains(string(data), "request_body") != tc.details {
						t.Fatalf("日志开关未生效: %s", data)
					}
					if strings.Contains(string(data), "test-key") {
						t.Fatal("日志不得包含认证凭据")
					}
					if suffix == "info" && !strings.Contains(string(data), "elasticsearch_search_1_duration_ms") {
						t.Fatalf("请求耗时不应受日志开关影响: %s", data)
					}
				}
			})
		})
	}
}

func TestElasticsearchPoolLimitRespectsRequestContext(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "debug", "log:\n  format: text\n")
		server := newBootstrapESServer(t, 8, false, "", 0)
		writeESConfig(t, "search", esConfigYAML(server, 8)+"pool:\n  maxConnsPerHost: 1\n")
		bootstrap.MustRegisterElasticsearch("search", true)
		var resources lifecycle.Stack
		defer resources.Close()
		bootstrap.MustBaseInit(cfg, &resources)
		client := elasticsearchx.Default()
		firstCtx, cancelFirst := context.WithCancel(context.Background())
		defer cancelFirst()
		done := make(chan error, 1)
		go func() {
			_, err := client.Count(firstCtx, "wait", map[string]any{})
			done <- err
		}()
		select {
		case <-server.waiting:
		case <-time.After(time.Second):
			t.Fatal("首个请求未到达服务端")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := client.Count(ctx, "products", map[string]any{}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("总连接上限未生效，或排队未遵循 context: %v", err)
		}
		cancelFirst()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("首个请求取消错误丢失: %v", err)
		}
		if _, err := client.Count(context.Background(), "products", map[string]any{}); err != nil {
			t.Fatalf("释放连接后应恢复使用: %v", err)
		}
	})
}
