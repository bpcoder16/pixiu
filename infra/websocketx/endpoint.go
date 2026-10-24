package websocketx

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/base64"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// Endpoint 是 WebSocket 服务入口，负责握手、会话管理和关闭收尾。
// 实现 http.Handler，使用 New 构造；不监听端口，也不关闭应用借用的资源。
type Endpoint struct {
	config         Config                  // 初始化后固定的配置。
	upgrader       websocket.Upgrader      // 将 HTTP 请求升级为 WebSocket 连接。
	mu             sync.RWMutex            // 保护连接登记、计数和接纳状态。
	sessions       map[string]*Session     // 可查找和路由的活跃会话，关闭时立即摘除。
	all            map[string]*Session     // 尚未完成收尾的全部会话，包含已摘除的会话。
	handshakes     map[*handshake]struct{} // 正在认证或升级的握手。
	connections    int                     // 握手、活跃及收尾中的连接总数；交接 Session 不重复计数。
	work           sync.WaitGroup          // 等待握手、会话及已登记回调实际结束。
	inputStopped   atomic.Bool             // 是否已停止接纳新握手、入站消息和资格复核。
	canceled       atomic.Bool             // 是否已发起强制取消。
	inputCtx       context.Context         // 停收上下文，用于取消握手和停止新资格复核。
	stopInput      context.CancelFunc      // 取消 inputCtx，发出停收信号。
	sessionCtx     context.Context         // 所有会话的父上下文，与 HTTP 请求生命周期独立。
	cancelSessions context.CancelFunc      // 取消全部会话的父上下文。
	stopOnce       sync.Once               // 确保关闭收尾任务只启动一次。
	cancelOnce     sync.Once               // 确保强制取消只执行一次。
	sealOnce       sync.Once               // 确保发送接纳只封闭一次。
	outputSealed   chan struct{}           // 关闭后拒绝新发送，已接纳的发送继续收尾。
	cancelCh       chan struct{}           // 关闭后通知收尾任务强制断开网络连接。
	done           chan struct{}           // 关闭后表示 Endpoint 的全部工作已结束。
	closeErr       error                   // 收尾完成后由 Wait 返回的最终错误。
	failureMu      sync.Mutex              // 保护停收后的失败统计。
	failures       DrainError              // 停收后的失败计数，最终汇总为 closeErr。
}

type handshake struct {
	endpoint *Endpoint
	ctx      context.Context
	conn     net.Conn // 由 Endpoint.mu 保护，成功 Hijack 后立即登记。
}

func New(cfg Config) (*Endpoint, error) {
	cfg, err := normalize(cfg)
	if err != nil {
		return nil, err
	}
	inputCtx, stopInput := context.WithCancel(context.Background())
	sessionCtx, cancelSessions := context.WithCancel(context.Background())
	e := &Endpoint{
		config:         cfg,
		sessions:       make(map[string]*Session),
		all:            make(map[string]*Session),
		handshakes:     make(map[*handshake]struct{}),
		inputCtx:       inputCtx,
		stopInput:      stopInput,
		sessionCtx:     sessionCtx,
		cancelSessions: cancelSessions,
		outputSealed:   make(chan struct{}),
		cancelCh:       make(chan struct{}),
		done:           make(chan struct{}),
	}
	e.upgrader = websocket.Upgrader{
		ReadBufferSize:    4096,
		WriteBufferSize:   4096,
		WriteBufferPool:   &sync.Pool{},
		Subprotocols:      cfg.Subprotocols,
		EnableCompression: cfg.EnableCompression,
		CheckOrigin:       e.checkOrigin,
		Error: func(w http.ResponseWriter, _ *http.Request, status int, _ error) {
			http.Error(w, http.StatusText(status), status)
		},
	}
	return e, nil
}

func (e *Endpoint) checkOrigin(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if len(values) == 0 {
		return !e.config.RejectMissingOrigin
	}
	if len(values) != 1 {
		return false
	}
	origin, err := originKey(values[0])
	if err != nil {
		return false
	}
	if e.config.AllowAnyOrigin {
		return true
	}
	if len(e.config.AllowedOrigins) > 0 {
		return slices.Contains(e.config.AllowedOrigins, origin)
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return origin == scheme+"://"+strings.ToLower(r.Host)
}

// ServeHTTP 的握手同步执行，返回后不再保存或使用 HTTP/Gin 请求对象。
func (e *Endpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(scope(r.Context()), e.config.HandshakeTimeout)
	h := &handshake{
		endpoint: e,
		ctx:      ctx,
	}
	e.mu.Lock()
	if e.inputStopped.Load() {
		e.mu.Unlock()
		cancel()
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		return
	}
	if len(e.handshakes) >= e.config.MaxPendingHandshakes || (e.config.MaxConnections > 0 && e.connections >= e.config.MaxConnections) {
		// 活跃期间的额度拒绝也有元信息观察；停止后不再启动新的组件回调。
		e.work.Add(1)
		e.mu.Unlock()
		defer e.work.Done()
		defer cancel()
		http.Error(w, "Service Unavailable", http.StatusServiceUnavailable)
		e.observe(ctx, Event{
			Kind:     EventHandshake,
			Code:     http.StatusServiceUnavailable,
			Err:      ErrQueueFull,
			Duration: time.Since(started),
		})
		return
	}
	e.handshakes[h] = struct{}{}
	e.connections++
	e.work.Add(1)
	e.mu.Unlock()
	// AfterFunc 的实际结束也计入握手工作，不能在回收资源时留下迟到的关闭动作。
	// inputCtx.Done() 表示端点停收；inputWatchDone 仅表示本次握手的停收回调已执行完毕。
	inputWatchDone := make(chan struct{})
	stopInputWatch := context.AfterFunc(e.inputCtx, func() {
		cancel()
		close(inputWatchDone)
	})
	// ctx.Done() 表示握手超时或取消；deadlineWatchDone 仅表示对应清理回调已执行完毕，不保证连接关闭成功。
	deadlineWatchDone := make(chan struct{})
	stopDeadlineWatch := context.AfterFunc(ctx, func() {
		e.mu.RLock()
		conn := h.conn
		e.mu.RUnlock()
		if conn != nil {
			_ = conn.Close()
		}
		close(deadlineWatchDone)
	})
	deadlineWatchStopped := false
	tw := &upgradeWriter{
		ResponseWriter: w,
		handshake:      h,
	}
	status := http.StatusInternalServerError
	var eventErr error
	var session *Session
	// 清理独立注册，确保 panic 处理中的错误响应再次 panic 时仍能释放握手资源。
	defer func() {
		if !stopInputWatch() {
			// 停止监听不会等待已启动的回调，需等其完成后才能结束握手工作。
			<-inputWatchDone
		}
		if !deadlineWatchStopped && !stopDeadlineWatch() {
			// 等待已启动的清理回调完成，避免握手工作结束后仍有关闭动作。
			<-deadlineWatchDone
		}
		cancel()
		e.mu.Lock()
		conn := h.conn
		h.conn = nil
		e.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		e.observe(ctx, Event{
			Kind:     EventHandshake,
			Code:     status,
			Err:      eventErr,
			Duration: time.Since(started),
		})
		e.mu.Lock()
		delete(e.handshakes, h)
		if session == nil {
			e.connections--
		}
		e.mu.Unlock()
		e.work.Done()
	}()
	// 后注册、先执行：在清理和观察前记录 panic，并尝试关闭会话或返回错误响应。
	defer func() {
		if recover() != nil {
			eventErr = ErrPanic
			e.recordDrainFailure(drainFailureCallback)
			if session != nil {
				session.terminate(websocket.CloseInternalServerErr, "internal error")
			}
			if !tw.hijacked && !tw.written {
				http.Error(tw, "Internal Server Error", http.StatusInternalServerError)
			}
		}
	}()
	reject := func(code int, err error) {
		status, eventErr = code, err
		http.Error(tw, http.StatusText(code), code)
	}
	ready := func() bool {
		return !e.inputStopped.Load() && ctx.Err() == nil && (e.config.CheckReady == nil || e.config.CheckReady())
	}
	if !ready() {
		reject(http.StatusServiceUnavailable, ErrClosed)
		return
	}
	// Sec-WebSocket-Key 是客户端为本次握手生成并经 Base64 编码的随机值，用于服务端生成握手应答，不能代替身份认证。
	// 协议要求解码后恰好为 16 字节；这里只校验编码和长度，不判断随机性。
	// 在 Authenticate 前检查方法、升级头、版本和 Key，避免明显无效的握手占用认证资源。
	key, keyErr := base64.StdEncoding.DecodeString(r.Header.Get("Sec-WebSocket-Key"))
	if r.Method != http.MethodGet || !websocket.IsWebSocketUpgrade(r) || r.Header.Get("Sec-WebSocket-Version") != "13" || keyErr != nil || len(key) != 16 {
		reject(http.StatusBadRequest, ErrInvalidMessage)
		return
	}
	if !e.checkOrigin(r) {
		reject(http.StatusForbidden, ErrInvalidMessage)
		return
	}
	if e.config.RequireSubprotocol {
		matched := false
		for _, protocol := range websocket.Subprotocols(r) {
			matched = matched || slices.Contains(e.config.Subprotocols, protocol)
		}
		if !matched {
			reject(http.StatusBadRequest, ErrInvalidMessage)
			return
		}
	}
	var identity Identity
	if e.config.Authenticate != nil {
		var err error
		identity, err = e.config.Authenticate(ctx, r)
		if err != nil {
			reject(http.StatusUnauthorized, ErrCallback)
			return
		}
	}
	if !ready() {
		reject(http.StatusServiceUnavailable, ErrClosed)
		return
	}
	deadline, _ := ctx.Deadline()
	upgrader := e.upgrader
	upgrader.HandshakeTimeout = time.Until(deadline)
	if upgrader.HandshakeTimeout <= 0 {
		reject(http.StatusServiceUnavailable, context.DeadlineExceeded)
		return
	}
	conn, err := upgrader.Upgrade(tw, r, nil)
	if err != nil {
		eventErr = ErrInvalidMessage
		return
	}
	// 101 已写出；后续即使无法交接 Session，也保留实际 HTTP 升级状态。
	status = http.StatusSwitchingProtocols
	// 先停止清理监听或等待已启动的回调完成，才能交接连接；否则回调可能迟到关闭正式会话。
	if !stopDeadlineWatch() {
		<-deadlineWatchDone
	}
	deadlineWatchStopped = true
	_ = conn.SetCompressionLevel(e.config.CompressionLevel)
	var random [16]byte
	// 当前 Go 版本的 crypto/rand.Read 保证填满缓冲区，正常返回时不会产生错误。
	_, _ = rand.Read(random[:])
	id := base64.RawURLEncoding.EncodeToString(random[:])
	if !ready() {
		eventErr = ErrClosed
		return
	}
	e.mu.Lock()
	if e.inputStopped.Load() || ctx.Err() != nil {
		e.mu.Unlock()
		eventErr = ErrClosed
		return
	}
	session = newSession(e, conn, id, identity)
	e.sessions[id] = session
	e.all[id] = session
	h.conn = nil // 同一锁下交接，握手取消从此不能关闭正式 Session。
	e.work.Add(1)
	e.mu.Unlock()
	go session.run()
}

// upgradeWriter 跟踪握手响应状态，并在 Hijack 成功后立即登记底层连接。
// 即使 Upgrade 尚未返回就阻塞在 101 写入或发生 panic，超时和异常清理仍能关闭连接。
type upgradeWriter struct {
	http.ResponseWriter            // 底层 HTTP 响应，用于升级前返回普通 HTTP 错误。
	handshake           *handshake // 当前握手，交接给 Session 前负责连接的异常清理。
	written             bool       // 是否已尝试写出 HTTP 响应，供 panic 清理判断能否补写错误。
	hijacked            bool       // 是否已接管底层连接，接管后禁止普通 HTTP 写入。
}

// WriteHeader 在写出前记录响应状态，避免 panic 后重复输出 HTTP 响应；接管后直接忽略。
func (w *upgradeWriter) WriteHeader(code int) {
	if w.hijacked {
		return
	}
	w.written = true
	w.ResponseWriter.WriteHeader(code)
}

// Write 同样记录响应状态；接管后拒绝写入，避免向已接管的连接输出普通 HTTP 正文。
func (w *upgradeWriter) Write(data []byte) (int, error) {
	if w.hijacked {
		return 0, net.ErrClosed
	}
	w.written = true
	return w.ResponseWriter.Write(data)
}

// Hijack 接管并提前登记 TCP 连接，连接交给 Session 前仍由握手负责回收。
func (w *upgradeWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h := w.handshake
	// 接管前先检查，避免已取消或停收的请求继续接管连接。
	if h.ctx.Err() != nil || h.endpoint.inputStopped.Load() {
		return nil, nil, ErrClosed
	}
	conn, rw, err := http.NewResponseController(w.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	w.hijacked = true
	h.endpoint.mu.Lock()
	// 在返回 Gorilla 前登记，确保写入 101 响应期间也能被超时或异常清理回收。
	h.conn = conn
	// 接管期间可能已停收或超时；再次检查，补上取消回调先执行却未找到连接的窗口。
	closing := h.endpoint.inputStopped.Load() || h.ctx.Err() != nil
	h.endpoint.mu.Unlock()
	if closing {
		_ = conn.Close()
		return nil, nil, ErrClosed
	}
	return conn, rw, nil
}

func (e *Endpoint) Len() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.sessions)
}

func (e *Endpoint) Get(id string) (*Session, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	s, ok := e.sessions[id]
	return s, ok
}

// Range 锁内获取快照，锁外调用 fn；返回 false 时结束遍历。
func (e *Endpoint) Range(fn func(*Session) bool) {
	if fn == nil {
		return
	}
	e.mu.RLock()
	snapshot := make([]*Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		snapshot = append(snapshot, s)
	}
	e.mu.RUnlock()
	for _, s := range snapshot {
		if !fn(s) {
			return
		}
	}
}

func (e *Endpoint) Budget() *Budget       { return e.config.Budget }
func (e *Endpoint) Done() <-chan struct{} { return e.done }

// StopInput 封闭新握手、数据接纳和新复核；存量业务和输出仍可运行。
func (e *Endpoint) StopInput() error {
	e.mu.Lock()
	e.inputStopped.Store(true)
	e.mu.Unlock()
	e.stopInput()
	return nil
}

// Stop 启动唯一收尾任务；不等待业务、网络或观察器。
func (e *Endpoint) Stop() error {
	_ = e.StopInput()
	e.stopOnce.Do(func() { go e.drain() })
	return nil
}

func (e *Endpoint) drain() {
	finished := make(chan struct{})
	forceDone := make(chan struct{})
	// 一个端点级取消循环负责断网，不为每个连接新建关闭 goroutine。
	go func() {
		defer close(forceDone)
		select {
		case <-finished:
			return
		case <-e.cancelCh:
		}
		e.mu.RLock()
		connections := make([]net.Conn, 0, len(e.all)+len(e.handshakes))
		for _, s := range e.all {
			connections = append(connections, s.conn.NetConn())
		}
		for h := range e.handshakes {
			if h.conn != nil {
				connections = append(connections, h.conn)
			}
		}
		e.mu.RUnlock()
		for _, conn := range connections {
			_ = conn.Close()
		}
	}()
	e.mu.RLock()
	snapshot := make([]*Session, 0, len(e.all))
	for _, s := range e.all {
		snapshot = append(snapshot, s)
	}
	e.mu.RUnlock()
	for _, s := range snapshot {
		<-s.dispatchDone
		<-s.revalidateDone
	}
	e.mu.Lock()
	e.sealOnce.Do(func() { close(e.outputSealed) })
	e.mu.Unlock()
	e.work.Wait()
	close(finished)
	<-forceDone
	e.cancelSessions()
	e.failureMu.Lock()
	if e.failures != (DrainError{}) {
		result := e.failures
		e.closeErr = &result
	}
	e.failureMu.Unlock()
	close(e.done)
}

// Wait 仅等待，不启动关闭，也不把调用方期限传播到业务。
func (e *Endpoint) Wait(ctx context.Context) error {
	if ctx == nil {
		return ErrNilContext
	}
	select {
	case <-e.done:
		return e.closeErr
	default:
	}
	select {
	case <-e.done:
		return e.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancel 同步封闭输出、通知取消，网络和回调收尾由已有工作负责。
func (e *Endpoint) Cancel() {
	_ = e.Stop()
	e.cancelOnce.Do(func() {
		e.mu.Lock()
		e.canceled.Store(true)
		e.sealOnce.Do(func() { close(e.outputSealed) })
		close(e.cancelCh)
		e.mu.Unlock()
		e.cancelSessions()
	})
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// closeBadGateway 表示网关收到上游无效响应（1014）；Gorilla v1.5.3 未提供对应常量。
const closeBadGateway = 1014

// validClose 保留标准关闭码白名单与 3000–4999 的应用关闭码范围。
// 未收到状态、异常断开和 TLS 握手失败是本地状态，不能作为关闭帧中的状态码发送。
func validClose(code int, reason string) bool {
	return (code >= 3000 && code <= 4999 || slices.Contains([]int{
		websocket.CloseNormalClosure,           // 1000：正常关闭。
		websocket.CloseGoingAway,               // 1001：端点离开，例如服务停止。
		websocket.CloseProtocolError,           // 1002：协议错误。
		websocket.CloseUnsupportedData,         // 1003：不支持收到的数据类型。
		websocket.CloseInvalidFramePayloadData, // 1007：消息内容无效，例如文本不是合法 UTF-8。
		websocket.ClosePolicyViolation,         // 1008：违反接纳策略，例如单连接队列满或资格复核失败。
		websocket.CloseMessageTooBig,           // 1009：消息超过大小限制。
		websocket.CloseMandatoryExtension,      // 1010：缺少客户端要求的扩展。
		websocket.CloseInternalServerErr,       // 1011：服务端内部错误。
		websocket.CloseServiceRestart,          // 1012：服务重启。
		websocket.CloseTryAgainLater,           // 1013：暂时无法处理，请稍后重试，例如共享预算不足。
		closeBadGateway,                        // 1014：网关收到上游无效响应。
	}, code)) && len(reason) <= 123
}

var _ http.Handler = (*Endpoint)(nil)
