package bootstrap_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
)

func TestWebSocketServiceStartupRollback(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		writeWSConfig(t, "app", "127.0.0.1:0")
		server := registerWSServer(t, "app", realtimeConfig())
		serviceFactory := func() (apprun.Service, error) { return bootstrap.NewWebSocketService("app") }
		// 第二次接管被拒绝时，启动器仍须回收第一个尚未 Run 的服务。
		if err := bootstrap.Start(serviceFactory, serviceFactory); err == nil || !strings.Contains(err.Error(), "already belongs") {
			t.Fatalf("重复接管错误丢失: %v", err)
		}
		select {
		case <-server.Done():
		case <-time.After(time.Second):
			t.Fatal("构造失败未回收已接管的实例")
		}
		if server.Ready() {
			t.Fatal("构造期间不应启动服务")
		}
		invalid := registerWSServer(t, "invalid", realtimeConfig())
		writeWSContent(t, "invalid", "addr: ':0'\nreadHeaderTimeout: -1s\npath: /ws\n")
		if _, err := bootstrap.NewWebSocketService("invalid"); err == nil {
			t.Fatal("错误 HTTP 配置被接受")
		}
		select {
		case <-invalid.Done():
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
			name := fmt.Sprintf("app-%d", i)
			writeWSConfig(t, name, addr)
			registerWSServer(t, name, realtimeConfig())
			service, err := bootstrap.NewWebSocketService(name)
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
