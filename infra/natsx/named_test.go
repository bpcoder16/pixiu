package natsx

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/infra/internal/named"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/nats-io/nats.go"
)

func isolatedNamedClients(t *testing.T) {
	t.Helper()
	previous := namedClients
	registry := named.New[*Client]("natsx")
	namedClients = registry
	t.Cleanup(func() {
		_ = registry.CloseAll()
		namedClients = previous
	})
}

func requireNamedPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("未初始化或关闭后的查询应 panic")
		}
	}()
	fn()
}

func TestNamedClientsPreserveOwnershipAndRetry(t *testing.T) {
	isolatedNamedClients(t)
	address := startServer(t)
	cfg := Config{Name: "events", URLs: []string{address}}
	for _, create := range []func(Config, ...nats.Option) (*Client, error){NewNamed, NewDefault} {
		for _, name := range []string{"", " \t"} {
			if client, err := create(Config{Name: name, URLs: cfg.URLs}); client != nil || err == nil {
				t.Fatalf("空名称被接受: client=%v err=%v", client, err)
			}
		}
	}
	independent, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer independent.Close()
	requireNamedPanic(t, func() { Default() })
	requireNamedPanic(t, func() { Named(cfg.Name) })
	if _, err := NewDefault(Config{Name: cfg.Name}); err == nil {
		t.Fatal("无效配置应失败且不占用默认名称")
	}
	shared, err := NewDefault(cfg, nats.PingInterval(7*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if Default() != shared || Named(cfg.Name) != shared || shared.Conn().Opts.PingInterval != 7*time.Second {
		t.Fatal("默认与命名引用或官方选项传递错误")
	}
	if _, err := NewDefault(Config{Name: "other"}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("重复默认初始化应在建连前拒绝: %v", err)
	}
	if _, err := NewNamed(Config{Name: cfg.Name}); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("重复名称应在建连前拒绝: %v", err)
	}
	optionErr := errors.New("invalid option")
	otherConfig := Config{Name: "other", URLs: cfg.URLs}
	if _, err := NewNamed(otherConfig, func(*nats.Options) error { return optionErr }); !errors.Is(err, optionErr) {
		t.Fatalf("构造错误链丢失: %v", err)
	}
	other, err := NewNamed(otherConfig)
	if err != nil {
		t.Fatal(err)
	}
	if other == shared || Default() != shared {
		t.Fatal("命名创建不应覆盖默认引用")
	}
	for range 2 {
		if err := CloseAll(); err != nil {
			t.Fatal(err)
		}
	}
	if !shared.Conn().IsClosed() || !other.Conn().IsClosed() || independent.Conn().IsClosed() {
		t.Fatal("CloseAll 应仅关闭注册表拥有的客户端")
	}
	requireNamedPanic(t, func() { Default() })
	requireNamedPanic(t, func() { Named(cfg.Name) })
	if _, err := NewNamed(cfg); err == nil || !strings.Contains(err.Error(), "clients closed") {
		t.Fatalf("关闭后不应重新创建: %v", err)
	}
}

func TestNamedClientRegistryChecksBeforeClientConfig(t *testing.T) {
	isolatedNamedClients(t)
	if _, err := NewDefault(Config{
		Name: "events",
		URLs: []string{startServer(t)},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewDefault(Config{}); err == nil || !strings.Contains(err.Error(), "default client is already registered") {
		t.Fatalf("应先拒绝重复默认实例，再委托 New 校验连接配置: %v", err)
	}
	if err := CloseAll(); err != nil {
		t.Fatal(err)
	}
	for _, create := range []func(Config, ...nats.Option) (*Client, error){NewNamed, NewDefault} {
		if client, err := create(Config{}); client != nil || err == nil || !strings.Contains(err.Error(), "named clients closed") {
			t.Fatalf("关闭后的注册表不应进入客户端构造: client=%v err=%v", client, err)
		}
	}
}

func TestCloseAllDrainsWithHeldClientBeforeDependencies(t *testing.T) {
	isolatedNamedClients(t)
	client, err := NewDefault(Config{Name: "events", URLs: []string{startServer(t)}})
	if err != nil {
		t.Fatal(err)
	}
	var dependencyClosed atomic.Bool
	var resources lifecycle.Stack
	if err := resources.Register(func() error {
		dependencyClosed.Store(true)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := resources.Register(CloseAll); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{}, 1)
	finished := make(chan error, 1)
	defer close(release)
	if err := client.RegisterCoreConsumers(context.Background(), CoreConsumer{
		Name:    "worker",
		Subject: "work",
		Handler: func(ctx context.Context, _ *nats.Msg) error {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				finished <- ctx.Err()
				return ctx.Err()
			}
			if dependencyClosed.Load() {
				err := errors.New("消息处理依赖提前关闭")
				finished <- err
				return err
			}
			err := client.Publish(ctx, "completed", nil)
			finished <- err
			return err
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(context.Background(), "work", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("消费回调未启动")
	}
	closed := make(chan error, 1)
	go func() { closed <- resources.Close() }()
	deadline := time.After(3 * time.Second)
	for !client.closing.Load() {
		select {
		case <-deadline:
			t.Fatal("未开始关闭")
		case <-time.After(time.Millisecond):
		}
	}
	requireNamedPanic(t, func() { Default() })
	release <- struct{}{}
	if err := <-finished; err != nil {
		t.Fatalf("排空中的回调不能完成: %v", err)
	}
	select {
	case err := <-closed:
		if err != nil || !dependencyClosed.Load() || !client.Conn().IsClosed() {
			t.Fatalf("资源关闭失败: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("关闭未完成")
	}
}

func TestCloseAllPreservesTimeoutAndContinuesClosing(t *testing.T) {
	isolatedNamedClients(t)
	address := startServer(t)
	client, err := NewDefault(Config{Name: "blocked", URLs: []string{address}, CloseTimeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	other, err := NewNamed(Config{Name: "other", URLs: []string{address}})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	finished := make(chan struct{})
	if err := client.RegisterCoreConsumers(context.Background(), CoreConsumer{
		Name:    "blocked",
		Subject: "blocked",
		Handler: func(ctx context.Context, _ *nats.Msg) error {
			close(entered)
			<-ctx.Done()
			close(finished)
			return nil
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := client.Publish(context.Background(), "blocked", nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("消费回调未启动")
	}
	first := CloseAll()
	if !errors.Is(first, ErrCloseTimeout) || !strings.Contains(first.Error(), "blocked") || CloseAll() != first {
		t.Fatalf("关闭错误应保留实例名、错误链和重复关闭结果: %v", first)
	}
	if !client.Conn().IsClosed() || !other.Conn().IsClosed() {
		t.Fatal("单个实例关闭超时不应阻止其他实例回收")
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("关闭超时未取消消息 context")
	}
	select {
	case <-client.closedEvent:
	case <-time.After(3 * time.Second):
		t.Fatal("连接关闭回调未完成")
	}
}
