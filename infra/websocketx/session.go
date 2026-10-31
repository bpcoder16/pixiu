package websocketx

import (
	"context"
	"errors"
	"math/rand/v2"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

// Session 由 Endpoint 管理；发送并发安全，身份与连接属性创建后不变。
type Session struct {
	endpoint       *Endpoint          // 所属的 WebSocket 服务入口。
	conn           *websocket.Conn    // 底层 WebSocket 网络连接。
	id             string             // 当前连接的标识，创建后固定。
	identity       Identity           // 握手认证得到的身份快照，创建后不变。
	ctx            context.Context    // 会话生命周期上下文，发起关闭时取消。
	cancel         context.CancelFunc // 取消会话上下文及其派生业务上下文。
	mu             sync.Mutex         // 保护关闭状态、消息队列和额度统计。
	closed         bool               // 是否已发起关闭，不代表收尾已完成。
	closeCode      int                // 首次关闭时确定的 WebSocket 关闭码。
	closeReason    string             // 首次关闭时确定的关闭原因。
	closeCause     error              // 首次关闭时的脱敏故障分类，仅用于观察。
	closeStarted   time.Time          // 首次发起关闭的时间，用于关闭期限和耗时统计。
	usage          Usage              // 当前收发额度占用，包含读取、处理和写入中的消息。
	receive        fifo[incoming]     // 已接纳、等待业务处理的入站消息队列。
	send           fifo[outgoing]     // 已接纳、等待写入网络的出站消息队列。
	receiveWake    chan struct{}      // 通知业务派发循环有新入站消息。
	sendWake       chan struct{}      // 通知写入循环有新出站消息。
	detached       chan struct{}      // 关闭后表示已摘除可路由索引，OnDetach 可能尚未结束。
	detachDone     chan struct{}      // 关闭后表示 OnDetach 阶段已结束。
	detachErr      error              // OnDetach 的失败标记，detachDone 关闭后读取。
	done           chan struct{}      // 关闭后表示网络、业务及 OnClose 全部收尾完成。
	readDone       chan struct{}      // 关闭后表示网络读取循环已退出。
	writeDone      chan struct{}      // 关闭后表示网络写入及发送队列清理已结束。
	dispatchDone   chan struct{}      // 关闭后表示业务派发及接收队列清理已结束。
	revalidateDone chan struct{}      // 关闭后表示资格复核已结束，未配置时直接关闭。
}

type incoming struct {
	message  Message
	accepted time.Time
}

type outgoing struct {
	message  Message
	prepared *PreparedMessage
	size     int64
}

func newSession(e *Endpoint, conn *websocket.Conn, id string, identity Identity) *Session {
	ctx, cancel := context.WithCancel(scope(e.sessionCtx))
	return &Session{
		endpoint:       e,
		conn:           conn,
		id:             id,
		identity:       identity,
		ctx:            ctx,
		cancel:         cancel,
		receiveWake:    make(chan struct{}, 1),
		sendWake:       make(chan struct{}, 1),
		detached:       make(chan struct{}),
		detachDone:     make(chan struct{}),
		done:           make(chan struct{}),
		readDone:       make(chan struct{}),
		writeDone:      make(chan struct{}),
		dispatchDone:   make(chan struct{}),
		revalidateDone: make(chan struct{}),
	}
}

func (s *Session) ID() string                { return s.id }
func (s *Session) Identity() Identity        { return s.identity }
func (s *Session) RemoteAddr() string        { return s.conn.RemoteAddr().String() }
func (s *Session) Subprotocol() string       { return s.conn.Subprotocol() }
func (s *Session) Context() context.Context  { return s.ctx }
func (s *Session) Detached() <-chan struct{} { return s.detached }
func (s *Session) Done() <-chan struct{}     { return s.done }

func (s *Session) Usage() Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.usage
}

// Close 发起单连接关闭，不排空未写消息；不会等待网络和业务收尾。
// OnDetach 在内部锁外同步执行，必须遵守快速内存操作约定。
func (s *Session) Close(code int, reason string) (bool, error) {
	if !validClose(code, reason) || !utf8.ValidString(reason) {
		return false, errors.New("websocketx: invalid close code or reason")
	}
	return s.terminate(code, reason), nil
}

// terminate 只发起单连接关闭：摘除活跃索引并取消会话上下文，不等待网络和业务收尾。
// OnDetach 在锁外同步执行；返回 true 表示本次首次发起关闭，false 表示此前已发起关闭，均不代表收尾完成。
// code 是 WebSocket 关闭状态码，reason 是关闭原因；仅首次调用保存，供 run 尽力发送关闭帧及记录关闭事件。
// Endpoint 强制取消时，run 跳过关闭帧发送，直接关闭底层连接。
// 三个通知分别表示不同阶段：
//   - detached：已摘除活跃索引并取消上下文，唤醒 run 开始网络收尾；OnDetach 可能尚未结束。
//   - detachDone：OnDetach 阶段已结束（包括未配置或捕获到 panic），由首次调用在返回前关闭。
//   - done：网络、业务及关闭回调等全部收尾完成，由 run 关闭；调用方通过 Done() 等待。
func (s *Session) terminate(code int, reason string) bool {
	return s.terminateWithError(code, reason, nil)
}

// terminateWithError 与正常关闭共用首次终止边界，后续断网错误不能覆盖原始原因。
// cause 必须是脱敏分类，不能传入业务或对端错误原文。
func (s *Session) terminateWithError(code int, reason string, cause error) bool {
	e := s.endpoint
	// 所有需要两把锁的路径统一 Endpoint → Session；回调永远在锁外。
	e.mu.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		e.mu.Unlock()
		return false
	}
	s.closed = true
	s.closeCode = code
	s.closeReason = reason
	s.closeCause = cause
	s.closeStarted = time.Now()
	if e.sessions[s.id] == s {
		delete(e.sessions, s.id)
	}
	s.cancel()
	close(s.detached)
	s.mu.Unlock()
	e.mu.Unlock()
	defer close(s.detachDone)
	if e.config.OnDetach != nil {
		func() {
			defer func() {
				if recover() != nil {
					s.detachErr = ErrPanic
					e.recordDrainFailure(drainFailureCallback)
				}
			}()
			e.config.OnDetach(s)
		}()
	}
	return true
}

func (s *Session) run() {
	go s.readLoop()
	go s.writeLoop()
	go s.dispatchLoop()
	if s.endpoint.config.Revalidate != nil {
		go s.revalidateLoop()
	} else {
		close(s.revalidateDone)
	}
	<-s.detached
	s.mu.Lock()
	code, reason, started := s.closeCode, s.closeReason, s.closeStarted
	closeErr := s.closeCause
	s.mu.Unlock()
	// 网络回收独立于用户钩子，慢 OnDetach/OnClose 不能阻止断网。
	// 1006 等本地状态只能用于观察，不能写入 Close 控制帧。
	if !s.endpoint.canceled.Load() && validClose(code, reason) {
		_ = s.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), started.Add(s.endpoint.config.CloseTimeout))
	}
	_ = s.conn.Close()
	<-s.readDone
	<-s.writeDone
	<-s.dispatchDone
	<-s.revalidateDone
	<-s.detachDone
	ctx := scope(s.ctx)
	closeErr = errors.Join(closeErr, s.detachErr)
	if s.endpoint.config.OnClose != nil {
		func() {
			defer func() {
				if recover() != nil {
					closeErr = errors.Join(closeErr, ErrPanic)
					s.endpoint.recordDrainFailure(drainFailureCallback)
				}
			}()
			s.endpoint.config.OnClose(ctx, s)
		}()
	}
	s.endpoint.observe(ctx, Event{
		Kind:         EventClosed,
		ConnectionID: s.id,
		Code:         code,
		Duration:     time.Since(started),
		Err:          closeErr,
	})
	s.endpoint.mu.Lock()
	delete(s.endpoint.all, s.id)
	s.endpoint.connections--
	s.endpoint.mu.Unlock()
	close(s.done)
	s.endpoint.work.Done()
}

// invoke 不把业务移交临时 goroutine；期限只负责取消和断开连接，真实返回仍需等待。
func (s *Session) invoke(deadline time.Time, code int, fn func(context.Context) error) (context.Context, error) {
	ctx, cancel := context.WithDeadline(scope(s.ctx), deadline)
	timedOut := make(chan struct{})
	timer := time.AfterFunc(time.Until(deadline), func() {
		// 先由 context 确定期限或主动取消的原因，不能抢先 cancel 将超时改写为取消。
		<-ctx.Done()
		if ctx.Err() == context.DeadlineExceeded {
			s.terminateWithError(code, "callback timeout", context.DeadlineExceeded)
		}
		close(timedOut)
	})
	err := func() (err error) {
		defer func() {
			if recover() != nil {
				err = ErrPanic
			}
		}()
		if fn != nil {
			return fn(ctx)
		}
		return nil
	}()
	if !timer.Stop() {
		<-timedOut
	}
	ctxErr := ctx.Err()
	expired := !time.Now().Before(deadline)
	if err == nil && errors.Is(ctxErr, context.Canceled) && !expired && !s.endpoint.canceled.Load() {
		s.mu.Lock()
		// 正常关闭也会取消 context，不能把按时成功的回调误报为排空失败。
		if s.closeCode == websocket.CloseNormalClosure && s.closeCause == nil {
			ctxErr = nil
		}
		s.mu.Unlock()
	}
	if ctxErr != nil || expired {
		err = ctxErr
		if err == nil {
			err = context.DeadlineExceeded
		}
		s.terminateWithError(code, "callback canceled", err)
	}
	cancel()
	return ctx, err
}

func (s *Session) dispatchLoop() {
	defer close(s.dispatchDone)
	defer s.discardReceive()
	e := s.endpoint
	if isClosed(s.detached) {
		return
	}
	if e.config.OnOpen != nil {
		ctx, err := s.invoke(time.Now().Add(e.config.MessageTimeout), websocket.CloseInternalServerErr, func(ctx context.Context) error {
			return e.config.OnOpen(ctx, s)
		})
		if err != nil {
			e.recordDrainFailure(drainFailureCallback)
			s.terminate(websocket.CloseInternalServerErr, "initialization failed")
			e.observe(ctx, Event{
				Kind:         EventOpened,
				ConnectionID: s.id,
				Err:          callbackError(err),
			})
			return
		}
	}
	e.observeFresh(s.ctx, Event{
		Kind:         EventOpened,
		ConnectionID: s.id,
	})
	for {
		e.mu.RLock()
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			e.mu.RUnlock()
			return
		}
		item, ok := s.receive.pop()
		stopped := e.inputStopped.Load()
		s.mu.Unlock()
		e.mu.RUnlock()
		if !ok {
			if stopped {
				return
			}
			select {
			case <-s.receiveWake:
			case <-e.inputCtx.Done():
			case <-s.detached:
				return
			}
			continue
		}
		deadline := item.accepted.Add(e.config.MessageTimeout)
		if !time.Now().Before(deadline) {
			e.recordDrainFailure(drainFailureReceiveExpired)
			s.releaseReceive(int64(len(item.message.Data)))
			e.observeFresh(s.ctx, Event{
				Kind:         EventReceiveDropped,
				ConnectionID: s.id,
				Type:         item.message.Type,
				Bytes:        int64(len(item.message.Data)),
				Err:          ErrExpired,
			})
			s.terminate(websocket.CloseTryAgainLater, "message expired")
			return
		}
		ctx, err := s.invoke(deadline, websocket.CloseInternalServerErr, func(ctx context.Context) error {
			if e.config.OnMessage != nil {
				return e.config.OnMessage(ctx, s, item.message)
			}
			return nil
		})
		if err != nil {
			e.recordDrainFailure(drainFailureCallback)
			if errors.Is(err, ErrPanic) {
				s.terminate(websocket.CloseInternalServerErr, "handler panic")
			}
		}
		e.observe(ctx, Event{
			Kind:         EventHandled,
			ConnectionID: s.id,
			Type:         item.message.Type,
			Bytes:        int64(len(item.message.Data)),
			Duration:     time.Since(item.accepted),
			Err:          callbackError(err),
		})
		s.releaseReceive(int64(len(item.message.Data)))
	}
}

func callbackError(err error) error {
	if err == nil {
		return nil
	}
	// 保留可识别的分类，不返回可能携带凭据的包装错误文本。
	if errors.Is(err, ErrPanic) {
		return ErrPanic
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return context.DeadlineExceeded
	}
	if errors.Is(err, context.Canceled) {
		return context.Canceled
	}
	return ErrCallback
}

func (s *Session) revalidateLoop() {
	defer close(s.revalidateDone)
	e := s.endpoint
	for {
		// 0.9–1.1 倍间隔避免同时建连的会话集中访问认证依赖。
		delay := time.Duration(float64(e.config.RevalidateInterval) * (0.9 + rand.Float64()*0.2))
		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-e.inputCtx.Done():
			timer.Stop()
			return
		case <-s.detached:
			timer.Stop()
			return
		}
		e.mu.RLock()
		stopped := e.inputStopped.Load() || isClosed(s.detached)
		e.mu.RUnlock()
		if stopped {
			return
		}
		// 复核在接纳门检查时成为在途工作，StopInput 不取消这次已接纳调用。
		started := time.Now()
		ctx, err := s.invoke(started.Add(e.config.RevalidateTimeout), websocket.ClosePolicyViolation, func(ctx context.Context) error {
			return e.config.Revalidate(ctx, s)
		})
		if err != nil {
			e.recordDrainFailure(drainFailureCallback)
			s.terminate(websocket.ClosePolicyViolation, "identity no longer valid")
		}
		e.observe(ctx, Event{
			Kind:         EventRevalidated,
			ConnectionID: s.id,
			Duration:     time.Since(started),
			Err:          callbackError(err),
		})
	}
}

func (s *Session) releaseReceive(size int64) {
	s.mu.Lock()
	s.usage.ReceiveMessages--
	s.usage.ReceiveBytes -= size
	s.endpoint.config.Budget.release(true, 1, size)
	s.mu.Unlock()
}

func (s *Session) discardReceive() {
	for {
		s.mu.Lock()
		item, ok := s.receive.pop()
		s.mu.Unlock()
		if !ok {
			return
		}
		s.endpoint.recordDrainFailure(drainFailureReceiveDropped)
		s.releaseReceive(int64(len(item.message.Data)))
		s.endpoint.observeFresh(s.ctx, Event{
			Kind:         EventReceiveDropped,
			ConnectionID: s.id,
			Type:         item.message.Type,
			Bytes:        int64(len(item.message.Data)),
			Err:          ErrClosed,
		})
	}
}
