package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/natsx"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

var natsFixtureDir string

// 不使用首个用例的 TempDir，避免该用例清理后其他用例引用失效。
var natsFixture = sync.OnceValues(func() (string, error) {
	var err error
	natsFixtureDir, err = os.MkdirTemp("", "pixiu-nats-fixture-")
	if err != nil {
		return "", err
	}
	_, source, _, _ := runtime.Caller(0)
	binary := filepath.Join(natsFixtureDir, "nats-server")
	build := exec.Command("go", "build", "-mod=readonly", "-o", binary, "github.com/nats-io/nats-server/v2")
	build.Dir = filepath.Dir(source)
	if output, err := build.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build NATS fixture: %w\n%s", err, output)
	}
	return binary, nil
})

func TestMain(m *testing.M) {
	code := m.Run()
	if natsFixtureDir != "" {
		_ = os.RemoveAll(natsFixtureDir)
	}
	os.Exit(code)
}

// 独立进程隔离 macOS pse 采样与 MustBaseInit 设置 time.Local 的竞争。
func runtimeClient(t *testing.T, permission string) *natsx.Client {
	t.Helper()
	binary, err := natsFixture()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	cmd := exec.Command(binary,
		"-a", "127.0.0.1", "-p", "-1", "-js",
		"-sd", filepath.Join(dir, "store"), "--ports_file_dir", dir,
	)
	if permission != "" {
		rule := `publish: { deny: ["$JS.ACK.>"] }`
		if permission == "core" {
			rule = `subscribe: { deny: ["denied"] }`
		}
		config := filepath.Join(dir, "server.conf")
		content := fmt.Sprintf(`authorization { users: [{ user: app, password: secret, permissions: { %s } }] }`, rule)
		if err := os.WriteFile(config, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		cmd.Args = append(cmd.Args, "-c", config)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() {
		waitErr = cmd.Wait()
		close(done)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
			if waitErr != nil {
				t.Error(waitErr)
			}
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
			t.Error("NATS 夹具未结束")
		}
	})
	// 官方端口文件包含实际监听地址，避免抢占固定端口或解析日志文本。
	portsFile := filepath.Join(dir, fmt.Sprintf("nats-server_%d.ports", cmd.Process.Pid))
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	var url string
	for url == "" {
		var ports struct {
			Nats []string `json:"nats"`
		}
		data, err := os.ReadFile(portsFile)
		if err == nil && json.Unmarshal(data, &ports) == nil && len(ports.Nats) > 0 {
			url = ports.Nats[0]
			break
		}
		select {
		case <-done:
			t.Fatalf("NATS 夹具提前退出: %v", waitErr)
		case <-deadline.C:
			t.Fatal("NATS 夹具未就绪")
		case <-ticker.C:
		}
	}
	cfg := natsx.Config{
		Name: "runtime-test",
		URLs: []string{url},
	}
	if permission != "" {
		cfg.Username = "app"
		cfg.Password = "secret"
	}
	c, err := natsx.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func bootstrapRegistry(handler natsx.Handler) *natsx.Registry {
	return natsx.MustNewRegistry(natsx.StreamSpec{
		Name: "BOOTSTRAP",
		Config: &natsx.StreamConfig{
			Subjects: []string{"bootstrap.work"},
			Storage:  jetstream.MemoryStorage,
		},
		Consumers: []natsx.ConsumerSpec{{
			Durable:        "work",
			FilterSubjects: []string{"bootstrap.work"},
			Handler:        handler,
		}},
	})
}

func waitRuntime(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Runtime 未完成")
	}
}

func TestNATSServiceFactoryRollbackAndNames(t *testing.T) {
	c := runtimeClient(t, "")
	var calls atomic.Int32
	js, err := natsx.NewJetStreamRuntime(context.Background(), c, bootstrapRegistry(func(context.Context, natsx.Message) error { calls.Add(1); return nil }), natsx.JetStreamRuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	core, err := natsx.NewCoreRuntime(c, natsx.MustNewCoreRegistry(), natsx.CoreRuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	a, b := bootstrap.NATSJetStreamService(js), bootstrap.NATSCoreService(core)
	if a.Name != "nats:runtime-test" || b.Name != "nats-core:runtime-test" {
		t.Fatal(a.Name, b.Name)
	}
	cause := errors.New("next factory failed")
	err = bootstrap.StartWithConfig(bootstrap.StartConfig{ShutdownTimeout: 5 * time.Second},
		func() (apprun.Service, error) { return a, nil }, func() (apprun.Service, error) { return b, nil }, func() (apprun.Service, error) { return apprun.Service{}, cause })
	if !errors.Is(err, cause) {
		t.Fatal(err)
	}
	waitRuntime(t, js.Done())
	waitRuntime(t, core.Done())
	if calls.Load() != 0 {
		t.Fatal("工厂成功前执行业务")
	}
	if err := a.Run(); err != nil {
		t.Fatal("适配器自身主动关闭未归一", err)
	}
	if err := b.Run(); err != nil {
		t.Fatal(err)
	}
	if c.Conn().IsClosed() {
		t.Fatal("Runtime 关闭借用 Client")
	}
}

func TestNATSServiceExternalCloseNotHidden(t *testing.T) {
	c := runtimeClient(t, "")
	r, err := natsx.NewCoreRuntime(c, natsx.MustNewCoreRegistry(), natsx.CoreRuntimeConfig{})
	if err != nil {
		t.Fatal(err)
	}
	s := bootstrap.NATSCoreService(r)
	if err := r.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if err := s.Run(); !errors.Is(err, natsx.ErrClosed) {
		t.Fatal("吞掉了外部关闭", err)
	}
}

func TestStartWithConfigValidation(t *testing.T) {
	for _, timeout := range []time.Duration{
		-time.Second,
		0,
		time.Second,
		5*time.Second - time.Nanosecond,
	} {
		called := false
		err := bootstrap.StartWithConfig(bootstrap.StartConfig{ShutdownTimeout: timeout}, func() (apprun.Service, error) { called = true; return apprun.Service{}, nil })
		if err == nil || called {
			t.Fatalf("非法期限 %s: %v", timeout, err)
		}
	}
	if recoverValue(func() { bootstrap.NATSJetStreamService(nil) }) == nil || recoverValue(func() { bootstrap.NATSCoreService(nil) }) == nil {
		t.Fatal("nil Runtime 未拒绝")
	}
}

func TestNATSStartRealFaultStopsOtherServices(t *testing.T) {
	for _, kind := range []string{
		"core",
		"jetstream",
	} {
		t.Run(kind, func(t *testing.T) {
			c := runtimeClient(t, kind)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := natsx.NewJetStreamRuntime(ctx, c, bootstrapRegistry(func(context.Context, natsx.Message) error { return nil }), natsx.JetStreamRuntimeConfig{ShutdownTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			subject := "allowed"
			if kind == "core" {
				subject = "denied"
			}
			core, err := natsx.NewCoreRuntime(c, natsx.MustNewCoreRegistry(natsx.CoreSubscriptionSpec{
				Name:    "one",
				Subject: subject,
				Handler: func(context.Context, *nats.Msg) error { return nil },
			}), natsx.CoreRuntimeConfig{ShutdownTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.JetStream().Publish(ctx, "bootstrap.work", nil); err != nil {
				t.Fatal(err)
			}
			stopped := make(chan struct{})
			err = bootstrap.StartWithConfig(bootstrap.StartConfig{ShutdownTimeout: 5 * time.Second},
				func() (apprun.Service, error) { return bootstrap.NATSJetStreamService(js), nil },
				func() (apprun.Service, error) { return bootstrap.NATSCoreService(core), nil },
				func() (apprun.Service, error) {
					return apprun.Service{
						Name:     "peer",
						Run:      func() error { <-stopped; return nil },
						Shutdown: func() error { close(stopped); return nil },
					}, nil
				})
			if !errors.Is(err, nats.ErrPermissionViolation) {
				t.Fatal(err)
			}
			waitRuntime(t, stopped)
			waitRuntime(t, js.Done())
			waitRuntime(t, core.Done())
		})
	}
}
