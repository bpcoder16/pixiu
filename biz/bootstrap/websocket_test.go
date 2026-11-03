package bootstrap_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func wsAddress(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func writeWSConfig(t *testing.T, name, addr string) {
	t.Helper()
	writeWSContent(t, name, fmt.Sprintf("addr: %q\npath: /ws\n", addr))
}

func writeWSContent(t *testing.T, name, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(env.ConfigDirPath(), "websocket."+name+".yaml"), []byte(content), 0600); err != nil {
		t.Fatal(err)
	}
}

func registerWSServer(t *testing.T, name string, cfg realtime.Config) *realtime.Server {
	t.Helper()
	server, err := realtime.NewServer(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := realtime.Register(name, server); err != nil {
		t.Fatal(err)
	}
	return server
}

func runWSService(t *testing.T, service apprun.Service, addr string) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- service.Run() }()
	t.Cleanup(func() {
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Error(err)
		}
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("组合服务 Run 未退出")
		}
	})
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + addr + "/ready/ws")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusNoContent {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("组合服务未就绪")
}

func wsDial(t *testing.T, addr, subprotocol string) *websocket.Conn {
	t.Helper()
	d := websocket.Dialer{
		Subprotocols:     []string{subprotocol},
		HandshakeTimeout: time.Second,
	}
	c, _, err := d.Dial("ws://"+addr+"/ws", http.Header{"Origin": {"http://" + addr}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func wsExchange(t *testing.T, c *websocket.Conn, want string) {
	t.Helper()
	frameType := websocket.TextMessage
	frame := []byte(`{"kind":1,"requestId":"r1","scene":"test","action":"echo","data":"hello"}`)
	if c.Subprotocol() == realtime.SubprotocolProtobuf {
		frameType = websocket.BinaryMessage
		body, _ := proto.Marshal(wrapperspb.String("hello"))
		frame, _ = proto.Marshal(&protocol.Envelope{
			Kind:      protocol.Kind_REQUEST,
			RequestId: "r1",
			Scene:     "test",
			Action:    "echo",
			Body:      &protocol.Envelope_Data{Data: body},
		})
	}
	_ = c.SetWriteDeadline(time.Now().Add(time.Second))
	if err := c.WriteMessage(frameType, frame); err != nil {
		t.Fatal(err)
	}
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	kind, data, err := c.ReadMessage()
	if err != nil || kind != frameType {
		t.Fatalf("收帧失败: %d %v", kind, err)
	}
	var got wrapperspb.StringValue
	if frameType == websocket.TextMessage {
		var envelope struct{ Data json.RawMessage }
		if err := json.Unmarshal(data, &envelope); err != nil {
			t.Fatal(err)
		}
		err = protojson.Unmarshal(envelope.Data, &got)
	} else {
		var envelope protocol.Envelope
		if err := proto.Unmarshal(data, &envelope); err != nil {
			t.Fatal(err)
		}
		err = proto.Unmarshal(envelope.GetData(), &got)
	}
	if err != nil || got.Value != want {
		t.Fatalf("身份/状态错误: got=%q want=%q err=%v", got.Value, want, err)
	}
}

// 三端共用协议和业务函数，但配置、身份、状态、监听与关闭各自独立。
func TestWebSocketServicesIndependent(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		var services []apprun.Service
		var clients []*websocket.Conn
		// 路由检查结束即关闭普通 HTTP 连接，排空测试只保留显式建立的 WebSocket。
		routeClient := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
		for i, name := range []string{"app", "web", "admin"} {
			addr := wsAddress(t)
			switch name {
			case "app":
				template, err := os.ReadFile("conf.example/websocket.example.yaml")
				if err != nil {
					t.Fatal(err)
				}
				writeWSContent(t, name, strings.NewReplacer(
					`":8889"`, fmt.Sprintf("%q", addr),
					`"/ws/app"`, `"/ws"`,
				).Replace(string(template)))
			case "web", "admin":
				writeWSConfig(t, name, addr)
			}
			cfg := realtimeConfig()
			cfg.Authenticate = func(context.Context, *http.Request) (realtime.ClientInfo, error) {
				return realtime.ClientInfo{UserID: name}, nil
			}
			cfg.NewState = func(realtime.ClientInfo) any { return new(int) }
			cfg.RegisterHandlers = func(r *realtime.Router) error {
				return r.Handle("test", "echo", func(ctx context.Context, s *realtime.Session, m *realtime.Message) error {
					var body wrapperspb.StringValue
					if err := m.Decode(&body); err != nil {
						return err
					}
					count := s.State().(*int)
					*count++
					return s.Reply(ctx, m, "test.echo", wrapperspb.String(fmt.Sprintf("%s:%d:%s", s.Info().UserID, *count, body.Value)))
				})
			}
			registerWSServer(t, name, cfg)
			service, err := bootstrap.NewWebSocketService(name)
			if err != nil || service.Name != "websocket:"+name {
				t.Fatalf("构造服务失败: %s %v", service.Name, err)
			}
			// 构造后不重新读取配置。
			writeWSContent(t, name, "invalid: true\n")
			runWSService(t, service, addr)
			// 独立端口只提供升级与就绪探针，其他路径不接受普通 HTTP 请求。
			for _, path := range []string{"/", "/page", "/ws-demo", "/ws/", "/ready/ws/"} {
				for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodHead} {
					req, err := http.NewRequest(method, "http://"+addr+path, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := routeClient.Do(req)
					if err != nil {
						t.Fatal(err)
					}
					_ = response.Body.Close()
					if response.StatusCode != http.StatusNotFound || response.Header.Get("Allow") != "" {
						t.Fatalf("非服务路径应返回 404: method=%s path=%s status=%d Allow=%q", method, path, response.StatusCode, response.Header.Get("Allow"))
					}
				}
			}
			for _, path := range []string{"/ws", "/ready/ws"} {
				req, _ := http.NewRequest(http.MethodHead, "http://"+addr+path, nil)
				response, err := routeClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				_ = response.Body.Close()
				if response.StatusCode != http.StatusMethodNotAllowed || response.Header.Get("Allow") != "GET" {
					t.Fatalf("保留入口方法边界错误: %s %d", path, response.StatusCode)
				}
			}
			subprotocol := realtime.SubprotocolJSON
			if i == 1 {
				subprotocol = realtime.SubprotocolProtobuf
			}
			client := wsDial(t, addr, subprotocol)
			wsExchange(t, client, name+":1:hello")
			services = append(services, service)
			clients = append(clients, client)
		}
		if err := stopAndWait(services[0].Stop, services[0].Wait, services[0].Cancel); err != nil {
			t.Fatal(err)
		}
		if _, _, err := clients[0].ReadMessage(); !websocket.IsCloseError(err, websocket.CloseGoingAway) {
			t.Fatalf("app 连接未关闭: %v", err)
		}
		wsExchange(t, clients[1], "web:2:hello")
		wsExchange(t, clients[2], "admin:2:hello")
	})
}

func TestWebSocketServiceFailureCleanup(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		registerWSServer(t, "app", realtimeConfig())
		for _, name := range []string{"", "../app", " app", "app/web"} {
			if _, err := bootstrap.NewWebSocketService(name); err == nil {
				t.Fatalf("接纳了非法名称 %q", name)
			}
		}
		if _, err := bootstrap.NewWebSocketService("app"); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("缺失配置错误链丢失: %v", err)
		}
		for i, content := range []string{
			"addr: ':0'\nunknown: true\npath: /ws\n",
			// 旧 http 块不再接纳，即使其内部字段本身合法。
			"http:\n  addr: ':0'\npath: /ws\n",
			"http:\n  addr: ':0'\n  unknown: true\npath: /ws\n",
			"path: /ws\n",
			"addr: ':0'\npath: /ready/ws\n",
			"addr: ':0'\npath: 'GET /ws'\n",
			"addr: ':0'\npath: /ws/{id}\n",
			"addr: ':0'\nreadHeaderTimeout: -1s\npath: /ws\n",
			"addr: ':8889'\nhttp:\n  addr: ':8890'\npath: /ws\n",
			"readTimeout: 2s\nhttp:\n  addr: ':8889'\npath: /ws\n",
			"readTimeout: 0s\nhttp:\n  addr: ':8889'\n  readTimeout: 2s\npath: /ws\n",
		} {
			name := fmt.Sprintf("invalid-%d", i)
			registerWSServer(t, name, realtimeConfig())
			writeWSContent(t, name, content)
			if _, err := bootstrap.NewWebSocketService(name); err == nil {
				t.Fatalf("非法配置未拒绝: %s", content)
			}
		}
		writeWSConfig(t, "app", "127.0.0.1:0")
		writeWSConfig(t, "unregistered", "127.0.0.1:0")
		_, lookupErr := bootstrap.NewWebSocketService("unregistered")
		var panicErr *apprun.PanicError
		if lookupErr == nil || !strings.Contains(lookupErr.Error(), `"unregistered" is not registered`) || errors.As(lookupErr, &panicErr) {
			t.Fatalf("未知实例应返回带名称的普通错误: %v", lookupErr)
		}
		service, err := bootstrap.NewWebSocketService("app")
		if err != nil {
			t.Fatal(err)
		}
		if err := stopAndWait(service.Stop, service.Wait, service.Cancel); err != nil {
			t.Fatal(err)
		}
		if err := service.Run(); err != nil {
			t.Fatalf("停收后 Run 不应重启: %v", err)
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		writeWSConfig(t, "occupied", ln.Addr().String())
		registerWSServer(t, "occupied", realtimeConfig())
		service, err = bootstrap.NewWebSocketService("occupied")
		if err != nil {
			t.Fatal(err)
		}
		var networkError *net.OpError
		if err := service.Run(); !errors.As(err, &networkError) || networkError.Op != "listen" {
			t.Fatalf("监听失败丢失: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Wait(ctx); err != nil {
			t.Fatalf("监听失败已通过 Run 上报，两侧排空应成功: %v", err)
		}
	})
}

func TestWebSocketServiceWaitIncludesOnClose(t *testing.T) {
	isolatedLog(t, func() {
		initHTTPConfigDir(t)
		addr := wsAddress(t)
		writeWSConfig(t, "app", addr)
		entered := make(chan context.Context, 1)
		opened := make(chan struct{}, 1)
		release := make(chan struct{})
		releaseCleanup := sync.OnceFunc(func() { close(release) })
		defer releaseCleanup()
		cfg := realtimeConfig()
		cfg.Transport.OnCloseTimeout = 5 * time.Second
		cfg.OnOpen = func(context.Context, *realtime.Session) error {
			opened <- struct{}{}
			return nil
		}
		cfg.OnClose = func(ctx context.Context, _ *realtime.Session) {
			entered <- ctx
			<-release
		}
		server := registerWSServer(t, "app", cfg)
		service, err := bootstrap.NewWebSocketService("app")
		if err != nil {
			t.Fatal(err)
		}
		// 此用例预期取消清理，单独验证错误而不使用成功路径的清理助手。
		defer service.Cancel()
		done := make(chan error, 1)
		go func() { done <- service.Run() }()
		deadline := time.Now().Add(3 * time.Second)
		var conn *websocket.Conn
		for time.Now().Before(deadline) {
			dialer := websocket.Dialer{Subprotocols: []string{realtime.SubprotocolJSON}}
			conn, _, err = dialer.Dial("ws://"+addr+"/ws", nil)
			if err == nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		select {
		case <-opened:
		case <-time.After(time.Second):
			t.Fatal("会话未发布")
		}
		if err := service.Stop(); err != nil {
			t.Fatal(err)
		}
		var cleanup context.Context
		select {
		case cleanup = <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("OnClose 未启动")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if err := service.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Wait 未覆盖 OnClose: %v", err)
		}
		if cleanup.Err() != nil {
			t.Fatal("Wait 超时提前取消了清理 Context")
		}
		service.Cancel()
		select {
		case <-cleanup.Done():
		case <-time.After(time.Second):
			t.Fatal("组合服务 Cancel 未通知 OnClose")
		}
		select {
		case <-server.Done():
			t.Fatal("OnClose 未实际返回便宣告完成")
		default:
		}
		releaseCleanup()
		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.Wait(ctx); !errors.Is(err, websocketx.ErrDrainFailed) {
			t.Fatalf("取消清理未报告排空失败: %v", err)
		}
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("内部 Run 未回收")
		}
	})
}
