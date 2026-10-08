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

// Config 配置监听地址、网络期限和关闭期限,零值使用普通 API 默认配置。
// 仅 ReadTimeout、WriteTimeout 允许负值,表示不设上限。
type Config struct {
	// Addr 是 Run 使用的监听地址,空值默认使用 :8888。
	Addr string
	// ReadHeaderTimeout 是读取请求头的最长时间,零值默认使用 5 秒。
	ReadHeaderTimeout time.Duration
	// ReadTimeout 是读取完整请求(含请求体)的最长时间,零值默认使用 10 秒,负值不设上限。
	ReadTimeout time.Duration
	// WriteTimeout 是写入响应的超时时间,零值默认使用 15 秒,负值不设上限。
	WriteTimeout time.Duration
	// IdleTimeout 是长连接等待下一次请求的最长时间,零值默认使用 60 秒。
	IdleTimeout time.Duration
	// MaxHeaderBytes 限制请求行与请求头的总字节数,零值默认使用 64 KiB,不限制请求体。
	MaxHeaderBytes int
	// ShutdownTimeout 限制首次 Shutdown 触发后的排空期限,零值默认使用 20 秒。
	ShutdownTimeout time.Duration
}

// Server 独立管理普通 HTTP 连接;不管理 hijacked 连接。
type Server struct {
	// config 保存已填入默认值的配置,构造完成后不再修改。
	config Config
	// http 承载请求处理、连接管理和限时排空,由本实例控制其生命周期。
	http *http.Server
	// ctx 只控制内部生命周期和关闭通知,不作为请求的父 context。
	ctx context.Context
	// cancel 在正常排空或超时强制关闭连接后调用。
	cancel context.CancelFunc

	// mu 保护 started、closing 和 closeErr,协调并发启动与关闭。
	mu sync.Mutex
	// started 在监听成功并通过状态校验后置为 true,不再重置,保证实例只启动一次。
	started bool
	// closing 在首次 Shutdown 时置为 true,阻止后续启动,并让其他关闭调用等待同一次结果。
	closing bool
	// closeErr 保存最终关闭结果,在取消内部 ctx 前写入,供 Run 和重复 Shutdown 返回。
	closeErr error
}

// New 校验配置并构造实例,不启动监听。
func New(cfg Config, handler http.Handler) (*Server, error) {
	if handler == nil {
		return nil, errors.New("httpserver: nil handler")
	}
	if cfg.ReadHeaderTimeout < 0 || cfg.IdleTimeout < 0 || cfg.ShutdownTimeout < 0 || cfg.MaxHeaderBytes < 0 {
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
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 20 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
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
		ctx:    ctx,
		cancel: cancel,
	}, nil
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
		// 标准库 Serve 在排空开始时返回,这里继续等待最终关闭结果。
		<-s.ctx.Done()
		return s.shutdownResult()
	}
	return err
}

// Run 监听配置地址并阻塞运行,由另一个 goroutine 调用 Shutdown 发起关闭。
func (s *Server) Run() error {
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

// Shutdown 是唯一关闭入口,停止接入并按 ShutdownTimeout 限时排空。
// 超时后强制关闭普通连接;重复或并发调用共享同一次期限和关闭结果。
func (s *Server) Shutdown() error {
	if !s.beginClose() {
		<-s.ctx.Done()
		return s.shutdownResult()
	}
	ctx, cancel := context.WithTimeout(s.ctx, s.config.ShutdownTimeout)
	defer cancel()
	err := s.http.Shutdown(ctx)
	if err != nil {
		err = errors.Join(err, s.http.Close())
	}
	s.finishClose(err)
	return err
}

func (s *Server) beginClose() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.closing = true
	return true
}

func (s *Server) finishClose(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeErr = err
	// 先保存结果再唤醒等待方,正常关闭不会把 context.Canceled 当成错误返回。
	s.cancel()
}

func (s *Server) shutdownResult() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeErr
}
