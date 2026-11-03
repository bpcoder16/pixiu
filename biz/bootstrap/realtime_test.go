package bootstrap_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/ginx"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/types/known/emptypb"
)

func realtimeConfig() realtime.Config {
	return realtime.Config{
		Authenticate: func(context.Context, *http.Request) (realtime.ClientInfo, error) {
			// 测试显式允许匿名，不依赖业务认证服务。
			return realtime.ClientInfo{}, nil
		},
		RegisterHandlers: func(r *realtime.Router) error {
			return r.Handle("home", "refresh", func(ctx context.Context, s *realtime.Session, m *realtime.Message) error {
				var request emptypb.Empty
				if err := m.Decode(&request); err != nil {
					return err
				}
				return s.Reply(ctx, m, "home.refreshed", &request)
			})
		},
	}
}

func TestRealtimeGinService(t *testing.T) {
	isolatedLog(t, func() {
		if err := env.Init(env.Config{
			AppName:       "realtime-test",
			RunMode:       env.RunModeRelease,
			TimeLocation:  "UTC",
			ConfigDirPath: t.TempDir(),
			LocalIP:       "127.0.0.1",
		}); err != nil {
			t.Fatal(err)
		}
		writeGinConfig(t, "disableAccessLog: true\n")
		writeHTTPConfig(t, "addr: 127.0.0.1:0\n")
		server, err := realtime.NewServer(realtimeConfig())
		if err != nil {
			t.Fatal(err)
		}
		defer server.Cancel()
		service := apprun.Service{
			Name:   "realtime:app",
			Run:    server.Run,
			Stop:   server.Stop,
			Wait:   server.Wait,
			Cancel: server.Cancel,
		}
		var engine *gin.Engine
		httpService, err := bootstrap.NewGinHTTPService(func(r *gin.Engine) {
			engine = r
			r.GET("/health", func(c *gin.Context) {
				c.Status(http.StatusNoContent)
			})
			ginx.RegisterIgnoredRoute(
				&r.RouterGroup,
				http.MethodGet,
				"/ws",
				gin.WrapH(server),
			)
		})
		if err != nil {
			t.Fatal(err)
		}
		defer stopAndWait(httpService.Stop, httpService.Wait, httpService.Cancel)
		run := make(chan error, 1)
		go func() { run <- service.Run() }()
		deadline := time.Now().Add(3 * time.Second)
		for !server.Ready() {
			if time.Now().After(deadline) {
				t.Fatal("realtime 未就绪")
			}
			time.Sleep(time.Millisecond)
		}
		h := httptest.NewServer(engine)
		defer h.Close()
		d := websocket.Dialer{Subprotocols: []string{realtime.SubprotocolJSON}}
		client, _, err := d.Dial("ws"+strings.TrimPrefix(h.URL, "http")+"/ws", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if err := client.WriteMessage(websocket.TextMessage, []byte(
			"{\"kind\":1,\"requestId\":\"r1\",\"scene\":\"home\",\"action\":\"refresh\",\"data\":{}}",
		)); err != nil {
			t.Fatal(err)
		}
		_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
		typ, reply, err := client.ReadMessage()
		if err != nil || typ != websocket.TextMessage || !strings.Contains(string(reply), "\"kind\":2") {
			t.Fatalf("Gin WebSocket 往返失败: %s, %v", reply, err)
		}
		response, err := http.Get(h.URL + "/health")
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusNoContent {
			t.Fatal("共用端口的普通 HTTP 路由失败")
		}
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-run:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Service.Run 没有退出")
		}
	})
}

func TestRealtimeFactoryFailureCleanup(t *testing.T) {
	server, err := realtime.NewServer(realtimeConfig())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Cancel()
	failure := errors.New("后续工厂构造失败")
	err = bootstrap.Start(
		func() (apprun.Service, error) {
			return apprun.Service{
				Name:   "realtime:app",
				Run:    server.Run,
				Stop:   server.Stop,
				Wait:   server.Wait,
				Cancel: server.Cancel,
			}, nil
		},
		func() (apprun.Service, error) {
			return apprun.Service{}, failure
		},
	)
	if !errors.Is(err, failure) {
		t.Fatalf("启动错误丢失: %v", err)
	}
	select {
	case <-server.Done():
	default:
		t.Fatal("后续工厂失败未回收 realtime")
	}
	if server.Ready() {
		t.Fatal("回收后仍然接纳握手")
	}
}
