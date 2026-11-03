package bootstrap_test

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func TestNamedWebSocketServiceOwnership(t *testing.T) {
	isolatedLog(t, func() {
		cfg := loadLogConfig(t, "release", "")
		var resources lifecycle.Stack
		defer resources.Close()
		var builds atomic.Int32
		bootstrap.MustRegisterWebSocket("app", func() (*realtime.Server, error) {
			builds.Add(1)
			c := exampleRealtimeConfig()
			c.RegisterHandlers = func(r *realtime.Router) error {
				return r.Handle("test", "echo", func(ctx context.Context, s *realtime.Session, m *realtime.Message) error {
					var request wrapperspb.StringValue
					if err := m.Decode(&request); err != nil {
						return err
					}
					return s.Reply(ctx, m, "echo", &request)
				})
			}
			return realtime.NewServer(c)
		})
		bootstrap.MustRegisterWebSocket("admin", func() (*realtime.Server, error) {
			builds.Add(1)
			return realtime.NewServer(exampleRealtimeConfig())
		})
		if _, err := bootstrap.NewWebSocketService("app"); err == nil {
			t.Fatal("初始化前应拒绝创建服务")
		}
		bootstrap.MustBaseInitWithoutLogit(cfg, &resources)
		app, admin := realtime.Named("app"), realtime.Named("admin")
		if app == admin || builds.Load() != 2 || app.Ready() {
			t.Fatal("初始化应构造独立实例且不运行")
		}
		if recoverValue(func() { bootstrap.MustRegisterWebSocket("late", func() (*realtime.Server, error) { return nil, nil }) }) == nil {
			t.Fatal("初始化后仍可登记")
		}
		addr := wsAddress(t)
		writeWSConfig(t, "app", addr)
		service, err := bootstrap.NewWebSocketService("app")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := bootstrap.NewWebSocketService("app"); err == nil {
			t.Fatal("同一实例被多个 Service 接管")
		}
		runWSService(t, service, addr)
		wsExchange(t, wsDial(t, addr, realtime.SubprotocolJSON), "hello")
		// 资源栈只回收共享客户端，不能提前关闭 Service 管理的连接。
		if err := resources.Close(); err != nil {
			t.Fatal(err)
		}
		if !app.Ready() {
			t.Fatal("资源栈关闭了命名 WebSocket")
		}
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Fatal(err)
		}
		if Named := realtime.Named("app"); Named != app {
			t.Fatal("关闭替换了已登记实例")
		}
		select {
		case <-app.Done():
		default:
			t.Fatal("Service 关闭未等待 Server")
		}
		writeWSConfig(t, "admin", wsAddress(t))
		adminService, err := bootstrap.NewWebSocketService("admin")
		if err != nil {
			t.Fatal(err)
		}
		if err := stopAndWait(adminService.Stop, adminService.Wait, adminService.Cancel); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNamedWebSocketInitializationFailure(t *testing.T) {
	for _, mode := range []string{"error", "panic", "nil", "alias"} {
		t.Run(mode, func(t *testing.T) {
			isolatedLog(t, func() {
				cfg := loadLogConfig(t, "release", "")
				var resources lifecycle.Stack
				defer resources.Close()
				var first, partial *realtime.Server
				sentinel := errors.New("build failed")
				bootstrap.MustRegisterWebSocket("first", func() (*realtime.Server, error) {
					var err error
					first, err = realtime.NewServer(exampleRealtimeConfig())
					return first, err
				})
				bootstrap.MustRegisterWebSocket("second", func() (*realtime.Server, error) {
					switch mode {
					case "panic":
						panic("build panic")
					case "nil":
						return nil, nil
					case "alias":
						return first, nil
					default:
						partial, _ = realtime.NewServer(exampleRealtimeConfig())
						return partial, sentinel
					}
				})
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				value := recoverValue(func() { bootstrap.MustBaseInitWithoutLogit(cfg, &resources) })
				if value == nil {
					t.Fatal("初始化失败应 panic")
				}
				if mode == "error" {
					err, ok := value.(error)
					if !ok || !errors.Is(err, sentinel) {
						t.Fatalf("原始错误丢失: %v", value)
					}
					if err := partial.Wait(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if err := first.Wait(ctx); err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestWebSocketRegistrationRejectsInvalidDeclaration(t *testing.T) {
	isolatedLog(t, func() {
		loadLogConfig(t, "release", "")
		factory := func() (*realtime.Server, error) { return realtime.NewServer(exampleRealtimeConfig()) }
		for _, name := range []string{"", "a.b", " space"} {
			if recoverValue(func() { bootstrap.MustRegisterWebSocket(name, factory) }) == nil {
				t.Errorf("非法名称 %q 未被拒绝", name)
			}
		}
		if recoverValue(func() { bootstrap.MustRegisterWebSocket("nil", nil) }) == nil {
			t.Fatal("nil 工厂未被拒绝")
		}
		bootstrap.MustRegisterWebSocket("app", factory)
		if recoverValue(func() { bootstrap.MustRegisterWebSocket("app", factory) }) == nil {
			t.Fatal("重复声明未被拒绝")
		}
		if _, err := bootstrap.NewWebSocketService("unknown"); err == nil {
			t.Fatal(fmt.Sprint("未登记的服务应失败"))
		}
	})
}
