package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

var (
	ErrClosed  = errors.New("httpserver: closed")
	ErrStarted = errors.New("httpserver: already started")
)

// Config 配置监听地址和网络期限,零值使用普通 API 默认配置。
// 仅 ReadTimeout、WriteTimeout 允许负值,表示不设上限。
type Config struct {
	// Addr 是 Run 使用的监听地址,空值默认使用 :8888。
	Addr string `mapstructure:"addr"`
	// ReadHeaderTimeout 是读取请求头的最长时间,零值默认使用 5 秒。
	ReadHeaderTimeout time.Duration `mapstructure:"readHeaderTimeout"`
	// ReadTimeout 是读取完整请求(含请求体)的最长时间,零值默认使用 10 秒,负值不设上限。
	ReadTimeout time.Duration `mapstructure:"readTimeout"`
	// WriteTimeout 是写入响应的超时时间,零值默认使用 15 秒,负值不设上限。
	WriteTimeout time.Duration `mapstructure:"writeTimeout"`
	// IdleTimeout 是长连接等待下一次请求的最长时间,零值默认使用 60 秒。
	IdleTimeout time.Duration `mapstructure:"idleTimeout"`
	// MaxHeaderBytes 限制请求行与请求头的总字节数,零值默认使用 64 KiB,不限制请求体。
	MaxHeaderBytes int `mapstructure:"maxHeaderBytes"`
}

// Server 独立管理普通 HTTP 连接;不管理 hijacked 连接。
type Server struct {
	// config 保存已填入默认值的配置,构造完成后不再修改。
	config Config
	// http 承载请求处理、连接管理和排空,由本实例控制其生命周期。
	http *http.Server
	// ctx 是业务请求的父 context，仅在显式 Cancel 或实际排空结束时取消。
	ctx context.Context
	// cancel 通知在途请求停止；不能强杀忽略取消的 Handler。
	cancel context.CancelFunc

	// mu 保护 started、closing 和启动/请求工作登记,协调并发启动与关闭。
	mu sync.Mutex
	// started 在监听成功并通过状态校验后置为 true,不再重置,保证实例只启动一次。
	started bool
	// closing 在 Stop 时封闭新请求及后续启动。
	closing bool
	// closeErr 在 done 关闭前发布，仅由 Wait 读取。
	closeErr error
	done     chan struct{}
	work     sync.WaitGroup
}

// New 校验配置并构造实例,不启动监听。
func New(cfg Config, handler http.Handler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("httpserver: nil handler")
	}
	if cfg.ReadHeaderTimeout < 0 || cfg.IdleTimeout < 0 || cfg.MaxHeaderBytes < 0 {
		return nil, errors.New("httpserver: negative limit or timeout")
	}
	if cfg.Addr == "" {
		cfg.Addr = ":8888"
	}
	if cfg.ReadHeaderTimeout == 0 {
		cfg.ReadHeaderTimeout = 5 * time.Second
	}
	if cfg.ReadTimeout == 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout == 0 {
		cfg.WriteTimeout = 15 * time.Second
	}
	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = time.Minute
	}
	if cfg.MaxHeaderBytes == 0 {
		cfg.MaxHeaderBytes = 64 << 10
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		config: cfg,
		http: &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadHeaderTimeout: cfg.ReadHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
			MaxHeaderBytes:    cfg.MaxHeaderBytes,
		},
		done:   make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
	}
	// 接纳与停收使用同一把门，Wait 开始后不再增加工作计数。
	s.http.BaseContext = func(net.Listener) context.Context { return ctx }
	s.http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			http.Error(w, "server stopping", http.StatusServiceUnavailable)
			return
		}
		s.work.Add(1)
		s.mu.Unlock()
		defer s.work.Done()
		handler.ServeHTTP(w, r)
	})
	return s, nil
}

func (s *Server) start() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrStarted
	}
	s.started = true
	s.mu.Unlock()
	return nil
}

func (s *Server) serve(listener net.Listener) error {
	err := s.http.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Run 监听配置地址并阻塞运行,由 Stop 发起停收，Wait 等待业务排空。
func (s *Server) Run() error {
	// 先返回已确定的生命周期错误,避免被监听错误掩盖;监听后仍需检查并发状态变更。
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.started {
		s.mu.Unlock()
		return ErrStarted
	}
	// 监听前登记，覆盖启动日志和 Serve 交接；后登记的 listener 清理先于 Done 执行。
	s.work.Add(1)
	s.mu.Unlock()
	defer s.work.Done()
	ln, err := net.Listen("tcp", s.config.Addr)
	if err != nil {
		return fmt.Errorf("httpserver: listen: %w", err)
	}
	defer func() {
		// Serve 会关闭 listener,这里兜底提前退出;忽略清理错误以保留主要操作的返回结果。
		_ = ln.Close()
	}()
	if startErr := s.start(); startErr != nil {
		return startErr
	}
	if logit.InfoEnabled(s.ctx) {
		logit.Info(s.ctx, "HTTPServerStart", logit.Str("addr", ln.Addr().String()))
	}
	return s.serve(ln)
}

// Stop 封闭普通请求入口并启动排空，不等待已有 Handler；可先于 Run 调用。
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	s.mu.Unlock()
	go func() {
		// 只由外部 Wait 的 context 限制等待，不在这里另起组件计时器。
		err := s.http.Shutdown(context.Background())
		s.work.Wait()
		s.closeErr = err
		s.cancel()
		close(s.done)
	}()
	return nil
}

// Wait 等待启动过程、连接排空以及实际 Handler 返回，ctx 到期只结束本次等待。
func (s *Server) Wait(ctx context.Context) error {
	if ctx == nil {
		return errors.New("httpserver: nil wait context")
	}
	select {
	case <-s.done:
		return s.closeErr
	default:
	}
	select {
	case <-s.done:
		return s.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel 先复用 Stop，再通知剩余请求取消并关闭普通连接；不管理 hijacked 连接。
func (s *Server) Cancel() {
	_ = s.Stop()
	s.cancel()
	_ = s.http.Close()
}
