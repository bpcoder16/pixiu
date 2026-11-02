package bootstrap_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
)

func TestWebSocketServiceStartupRollback(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeWSConfig(t, "app", "127.0.0.1:0")
		var servers []*realtime.Server
		factory := func() (*realtime.Server, error) {
			server, err := realtime.NewServer(realtimeConfig())
			servers = append(servers, server)
			return server, err
		}
		serviceFactory := func() (apprun.Service, error) {
			return bootstrap.NewWebSocketService("app", factory)
		}
		// 第二次登记名称冲突时，两个尚未 Run 的服务都须回收。
		if err := bootstrap.Start(serviceFactory, serviceFactory); !errors.Is(err, apprun.ErrDuplicateService) {
			t.Fatalf("重复服务错误丢失: %v", err)
		}
		for _, server := range servers {
			select {
			case <-server.Done():
			default:
				t.Fatal("登记失败未回收服务")
			}
			if server.Ready() {
				t.Fatal("构造期间不应启动服务")
			}
		}
		writeWSContent(t, "app", "addr: ':0'\nreadHeaderTimeout: -1s\npath: /ws\n")
		if _, err := bootstrap.NewWebSocketService("app", factory); err == nil {
			t.Fatal("错误 HTTP 配置被接受")
		}
		if len(servers) != 3 {
			t.Fatalf("监听构造失败未覆盖 realtime 回收: servers=%d", len(servers))
		}
		select {
		case <-servers[len(servers)-1].Done():
		case <-time.After(time.Second):
			t.Fatal("监听构造失败未清理 realtime")
		}
	})
}

func TestWebSocketServiceRunStopRace(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		for i := range 12 {
			addr := wsAddress(t)
			writeWSConfig(t, "app", addr)
			service, err := bootstrap.NewWebSocketService("app", func() (*realtime.Server, error) {
				return realtime.NewServer(realtimeConfig())
			})
			if err != nil {
				t.Fatal(err)
			}
			var calls sync.WaitGroup
			calls.Add(2)
			go func() {
				defer calls.Done()
				if err := service.Run(); err != nil {
					t.Error(err)
				}
			}()
			go func() {
				defer calls.Done()
				if i%2 == 0 {
					service.Cancel()
				} else if err := service.Stop(); err != nil {
					t.Error(err)
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := service.Wait(ctx); err != nil {
				service.Cancel()
				t.Fatal(err)
			}
			cancel()
			calls.Wait()
			listener, err := net.Listen("tcp", addr)
			if err != nil {
				t.Fatalf("停收后仍占用监听: %v", err)
			}
			_ = listener.Close()
		}
	})
}
