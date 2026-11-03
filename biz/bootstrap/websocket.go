package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/bpcoder16/pixiu/biz/realtime"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

type webSocketConfig struct {
	HTTP httpserver.Config `mapstructure:",squash"`
	Path string            `mapstructure:"path"`
}

// NewWebSocketService 构造独占端口的 websocket:<name> 服务，不启动监听或后台任务。
// 严格读取 env.ConfigDirPath() 下的 websocket.<name>.yaml，顶层 addr 与 path 必填。
// name 为非空 ASCII 字母、数字、下划线、连字符组合。
// 接管已由 MustBaseInit 或 realtime.Register 登记的命名实例，每个实例只可接管一次。
// 成功后本服务独占生命周期，调用方不得再单独运行或登记内部 Server；失败时回收本轮已接管的对象。
// 只处理配置的升级路径与固定 /ready/ws，两者仅允许 GET；其他路径返回 404，不提供普通 HTTP 路由扩展。
// HTTP 与 realtime 并发启动，以 /ready/ws 的 204 表示可接纳连接；所有实例共享外层管理器的停机预算。
func NewWebSocketService(name string) (apprun.Service, error) {
	if err := validateInstanceName("WebSocket", name); err != nil {
		return apprun.Service{}, err
	}
	configFile := filepath.Join(env.ConfigDirPath(), "websocket."+name+".yaml")
	cfg, err := configx.Parse[webSocketConfig](configFile)
	if err != nil {
		return apprun.Service{}, fmt.Errorf("bootstrap: load WebSocket config: %w", err)
	}
	if strings.TrimSpace(cfg.HTTP.Addr) == "" || cfg.HTTP.Addr != strings.TrimSpace(cfg.HTTP.Addr) {
		return apprun.Service{}, fmt.Errorf("bootstrap: %s requires addr without surrounding whitespace", configFile)
	}
	if !strings.HasPrefix(cfg.Path, "/") || path.Clean(cfg.Path) != cfg.Path ||
		strings.ContainsAny(cfg.Path, "{}?#% \t\r\n") || cfg.Path == "/ready/ws" {
		return apprun.Service{}, fmt.Errorf("bootstrap: %s requires a literal absolute path other than /ready/ws", configFile)
	}
	serviceName := "websocket:" + name
	var server *realtime.Server
	if lookupErr := callServiceCleanup(serviceName, "lookup realtime", func() error {
		var resolveErr error
		server, resolveErr = registeredWebSocket(name)
		return resolveErr
	}); lookupErr != nil {
		return apprun.Service{}, lookupErr
	}
	if claimErr := claimWebSocket(name); claimErr != nil {
		return apprun.Service{}, claimErr
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != cfg.Path && r.URL.Path != "/ready/ws" {
			http.NotFound(w, r)
			return
		}
		// 显式处理 HEAD 等方法，保持升级入口与探针的 GET 契约。
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/ready/ws" {
			if !server.Ready() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
			return
		}
		server.ServeHTTP(w, r)
	})
	httpService, err := newHTTPService(serviceName+":http", cfg.HTTP, handler)
	if err != nil {
		server.Cancel()
		return apprun.Service{}, fmt.Errorf("bootstrap: create WebSocket listener from %q: %w", configFile, err)
	}
	s := &webSocketService{
		name:     serviceName,
		http:     httpService,
		realtime: server,
		runsDone: make(chan struct{}),
	}
	return apprun.Service{
		Name:   serviceName,
		Run:    s.run,
		Stop:   s.stop,
		Wait:   s.wait,
		Cancel: s.cancel,
	}, nil
}

// 仅组合本服务拥有的两个生命周期，不引入第二套信号或排空期限。
type webSocketService struct {
	name     string
	http     apprun.Service
	realtime *realtime.Server

	mu         sync.Mutex // 协调 Run 与 Stop；先停收时由 Stop 直接关闭 runsDone。
	started    bool
	stopping   bool
	runsDone   chan struct{}
	lateRunErr error // 第二个 Run 的结果，在 runsDone 关闭后读取。
	stopOnce   sync.Once
	stopErr    error
}

func (s *webSocketService) run() error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return fmt.Errorf("bootstrap: %s already started", s.name)
	}
	s.started = true
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	s.mu.Unlock()
	results := make(chan error, 2)
	for i, run := range []func() error{s.http.Run, s.realtime.Run} {
		go func() {
			component := s.http.Name
			if i == 1 {
				component = s.name + ":realtime"
			}
			results <- callServiceCleanup(component, "run", run)
		}()
	}
	// 任一侧退出即停收另一侧；不能在此等待排空，否则会遮蔽监听失败并阻止管理器启动关闭。
	err := <-results
	go func() {
		s.lateRunErr = <-results
		close(s.runsDone)
	}()
	return errors.Join(err, s.stop())
}

func (s *webSocketService) stop() error {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopping = true
		if !s.started {
			close(s.runsDone)
		}
		s.mu.Unlock()
		// 先封闭升级门，再停监听；两侧 Stop 都不等待在途业务。
		s.stopErr = errors.Join(s.realtime.Stop(), s.http.Stop())
	})
	return s.stopErr
}

func (s *webSocketService) wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("bootstrap: nil WebSocket wait context")
	}
	// Stop 已并行发起实际排空；依次等待使用同一个 ctx，不累计两份期限。
	err := errors.Join(s.http.Wait(ctx), s.realtime.Wait(ctx))
	select {
	case <-s.runsDone:
	default:
		select {
		case <-s.runsDone:
		case <-ctx.Done():
			return errors.Join(err, ctx.Err())
		}
	}
	// realtime.Run 本身也等待排空；Wait 已返回同一错误时只保留一份。
	if s.lateRunErr != nil && !errors.Is(err, s.lateRunErr) {
		return errors.Join(err, s.lateRunErr)
	}
	return err
}

func (s *webSocketService) cancel() {
	_ = s.stop()
	s.realtime.Cancel()
	s.http.Cancel()
}
