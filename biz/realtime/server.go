package realtime

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/bpcoder16/pixiu/infra/websocketx"
)

const (
	serverCreated uint32 = iota // 新建服务的零值，Run 前拒绝握手。
	serverRunning               // Run 已启动，允许新连接进入握手流程。
	serverStopped               // Stop 或 Cancel 后停止接纳，后续 Run 也无法重新开启接纳。
)

// Server 组合业务 Session 和原始传输；HTTP 与本服务的生命周期均需由调用方管理。
// bootstrap.NewWebSocketService 可将两者组合为独立端口服务；共用端口时分别登记。
// 构造后不得复制，Run 只执行一次，Stop/Wait/Cancel 可并发调用。
type Server struct {
	config   Config
	endpoint *websocketx.Endpoint
	state    atomic.Uint32
	started  atomic.Bool
	// 将传输连接关联到已初始化的业务会话，供消息和业务收尾回调使用。
	sessions sync.Map
}

// NewServer 只构造对象，不启动后台任务、不监听端口。
func NewServer(cfg Config) (*Server, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}
	if cfg.Transport.ReadLimit == 0 {
		cfg.Transport.ReadLimit = 64 << 10
	}
	if cfg.Transport.MaxSendBytes == 0 {
		cfg.Transport.MaxSendBytes = 64 << 10
	}
	s := &Server{config: cfg}
	transport := cfg.Transport
	transport.Subprotocols = []string{SubprotocolJSON, SubprotocolProtobuf}
	transport.RequireSubprotocol = true
	transport.CheckReady = s.Ready
	transport.Authenticate = func(ctx context.Context, r *http.Request) (websocketx.Identity, error) {
		info, err := cfg.Authenticate(ctx, r)
		return websocketx.Identity{
			Subject: info.UserID,
			Value:   info,
		}, err
	}
	transport.OnOpen = s.onOpen
	transport.OnMessage = s.onMessage
	if cfg.OnClose != nil {
		transport.OnClose = s.onClose
	} else {
		transport.OnDetach = func(raw *websocketx.Session) { s.sessions.Delete(raw) }
	}
	if cfg.Revalidate != nil {
		transport.Revalidate = func(ctx context.Context, raw *websocketx.Session) error {
			return cfg.Revalidate(ctx, raw.Identity().Value.(ClientInfo))
		}
	}
	endpoint, err := websocketx.New(transport)
	if err != nil {
		return nil, err
	}
	s.endpoint = endpoint
	return s, nil
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.endpoint.ServeHTTP(w, r)
}

// Run 进入就绪状态并等待真实收尾；Stop 先于 Run 时直接等待关闭，不重新接纳。
func (s *Server) Run() error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrStarted
	}
	s.state.CompareAndSwap(serverCreated, serverRunning)
	return s.Wait(context.Background())
}

func (s *Server) Ready() bool { return s.state.Load() == serverRunning }

// Stop 启动有序排空，不提前取消已接纳的业务。
func (s *Server) Stop() error {
	s.state.Store(serverStopped)
	return s.endpoint.Stop()
}

// Wait 只等待收尾，期限到达时由调用方显式 Cancel。
func (s *Server) Wait(ctx context.Context) error { return s.endpoint.Wait(ctx) }

// Cancel 通知强制取消；忽略取消的业务仍会延迟 Done。
func (s *Server) Cancel() {
	s.state.Store(serverStopped)
	s.endpoint.Cancel()
}

func (s *Server) Done() <-chan struct{} { return s.endpoint.Done() }

func (s *Server) onOpen(ctx context.Context, raw *websocketx.Session) error {
	session := &Session{
		raw:          raw,
		info:         raw.Identity().Value.(ClientInfo),
		maxSendBytes: s.config.Transport.MaxSendBytes,
	}
	if s.config.NewState != nil {
		session.state = s.config.NewState(session.info)
	}
	s.sessions.Store(raw, session)
	if s.config.OnClose == nil {
		// OnDetach 可能已在 NewState 期间执行；补删迟到的登记。
		select {
		case <-raw.Detached():
			s.sessions.Delete(raw)
		default:
		}
	}
	if s.config.OnOpen != nil {
		return s.config.OnOpen(ctx, session)
	}
	return nil
}

func (s *Server) onMessage(ctx context.Context, raw *websocketx.Session, frame websocketx.Message) error {
	value, ok := s.sessions.Load(raw)
	if !ok {
		return websocketx.ErrClosed
	}
	session := value.(*Session)
	want := websocketx.Text
	if session.Subprotocol() == SubprotocolProtobuf {
		want = websocketx.Binary
	}
	if frame.Type != want {
		_, _ = raw.Close(1003, "unexpected message type")
		return ErrInvalidMessage
	}
	message, err := decodeEnvelope(session.Subprotocol(), frame.Data)
	if err != nil {
		_, _ = raw.Close(1007, "invalid envelope")
		return err
	}
	message.session = session
	message.ctx = ctx
	return s.config.OnMessage(ctx, session, message)
}

func (s *Server) onClose(ctx context.Context, raw *websocketx.Session) {
	value, ok := s.sessions.LoadAndDelete(raw)
	if ok {
		s.config.OnClose(ctx, value.(*Session))
	}
}
