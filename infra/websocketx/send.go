package websocketx

import (
	"bytes"
	"context"
	"errors"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

// PreparedMessage 绑定 Endpoint 和不可变帧，业务不得复用到其他 Endpoint。
type PreparedMessage struct {
	endpoint  *Endpoint
	frame     *websocket.PreparedMessage
	typ       MessageType
	size      int64
	expiresAt time.Time
}

func validateMessage(message Message, limit int64) error {
	if message.Type != Text && message.Type != Binary {
		return ErrInvalidMessage
	}
	if int64(len(message.Data)) > limit {
		return ErrMessageTooLarge
	}
	if message.Type == Text && !utf8.Valid(message.Data) {
		return ErrInvalidMessage
	}
	if !message.ExpiresAt.IsZero() && !time.Now().Before(message.ExpiresAt) {
		return ErrExpired
	}
	return nil
}

// Prepare 复制一份正文并由 Gorilla 缓存固定编码选项对应的帧；不占发送队列预算。
func (e *Endpoint) Prepare(message Message) (*PreparedMessage, error) {
	if e.canceled.Load() || isClosed(e.outputSealed) {
		return nil, ErrClosed
	}
	if err := validateMessage(message, e.config.MaxSendBytes); err != nil {
		return nil, err
	}
	// Gorilla 构造时已复制到不可变 server frame，不再做一次重复正文复制。
	frame, err := websocket.NewPreparedMessage(int(message.Type), message.Data)
	if err != nil {
		return nil, err
	}
	return &PreparedMessage{
		endpoint:  e,
		frame:     frame,
		typ:       message.Type,
		size:      int64(len(message.Data)),
		expiresAt: message.ExpiresAt,
	}, nil
}

func (s *Session) Send(ctx context.Context, message Message) error {
	return s.enqueue(ctx, message, nil)
}

func (s *Session) SendPrepared(ctx context.Context, message *PreparedMessage) error {
	return s.enqueue(ctx, Message{}, message)
}

func (s *Session) enqueue(ctx context.Context, message Message, prepared *PreparedMessage) error {
	if ctx == nil {
		return ErrNilContext
	}
	e := s.endpoint
	e.mu.RLock()
	defer e.mu.RUnlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed || e.canceled.Load() || isClosed(e.outputSealed) {
		return ErrClosed
	}
	size := int64(len(message.Data))
	if prepared != nil {
		if prepared.endpoint != e || prepared.frame == nil {
			return ErrWrongEndpoint
		}
		size = prepared.size
		message = Message{
			Type:      prepared.typ,
			ExpiresAt: prepared.expiresAt,
		}
		if !message.ExpiresAt.IsZero() && !time.Now().Before(message.ExpiresAt) {
			return ErrExpired
		}
	} else if err := validateMessage(message, e.config.MaxSendBytes); err != nil {
		return err
	}
	if s.usage.SendMessages >= int64(e.config.SendQueueSize) || size > e.config.SendQueueBytes-s.usage.SendBytes {
		return ErrQueueFull
	}
	if !e.config.Budget.reserve(false, 1, size) {
		return ErrBudgetExceeded
	}
	s.usage.SendMessages++
	s.usage.SendBytes += size
	if prepared == nil {
		message.Data = bytes.Clone(message.Data)
	}
	s.send.push(outgoing{
		message:  message,
		prepared: prepared,
		size:     size,
	}, e.config.SendQueueSize)
	notify(s.sendWake)
	return nil
}

func (s *Session) releaseSend(item outgoing) {
	s.mu.Lock()
	s.usage.SendMessages--
	s.usage.SendBytes -= item.size
	s.endpoint.config.Budget.release(false, 1, item.size)
	s.mu.Unlock()
}

func (s *Session) discardSend() {
	for {
		s.mu.Lock()
		item, ok := s.send.pop()
		s.mu.Unlock()
		if !ok {
			return
		}
		s.endpoint.recordDrainFailure(drainFailureSendDropped)
		s.releaseSend(item)
		s.endpoint.observeFresh(s.ctx, Event{
			Kind:         EventSendDropped,
			ConnectionID: s.id,
			Type:         item.message.Type,
			Bytes:        item.size,
			Err:          ErrClosed,
		})
	}
}

func (s *Session) writeLoop() {
	defer close(s.writeDone)
	defer s.discardSend()
	e := s.endpoint
	var ticks <-chan time.Time
	if !e.config.DisableHeartbeat {
		ticker := time.NewTicker(e.config.PingInterval)
		defer ticker.Stop()
		ticks = ticker.C
	}
	for {
		e.mu.RLock()
		s.mu.Lock()
		if s.closed || e.canceled.Load() {
			s.mu.Unlock()
			e.mu.RUnlock()
			s.terminate(websocket.CloseGoingAway, "canceled")
			return
		}
		item, ok := s.send.pop()
		sealed := isClosed(e.outputSealed)
		s.mu.Unlock()
		e.mu.RUnlock()
		if ok {
			if !item.message.ExpiresAt.IsZero() && !time.Now().Before(item.message.ExpiresAt) {
				e.recordDrainFailure(drainFailureSendExpired)
				s.releaseSend(item)
				e.observeFresh(s.ctx, Event{
					Kind:         EventSendDropped,
					ConnectionID: s.id,
					Type:         item.message.Type,
					Bytes:        item.size,
					Err:          ErrExpired,
				})
				if !s.pingIfDue(ticks) {
					return
				}
				continue
			}
			started := time.Now()
			err := s.conn.SetWriteDeadline(started.Add(e.config.WriteTimeout))
			if err == nil {
				if item.prepared != nil {
					err = s.conn.WritePreparedMessage(item.prepared.frame)
				} else {
					err = s.conn.WriteMessage(int(item.message.Type), item.message.Data)
				}
			}
			s.releaseSend(item)
			event := Event{
				Kind:         EventSent,
				ConnectionID: s.id,
				Type:         item.message.Type,
				Bytes:        item.size,
				Duration:     time.Since(started),
			}
			if err != nil {
				e.recordDrainFailure(drainFailureWrite)
				event.Kind = EventSendDropped
				event.Err = errors.New("websocketx: write failed")
				s.terminate(websocket.CloseInternalServerErr, "write failed")
			}
			e.observeFresh(s.ctx, event)
			if err != nil {
				return
			}
			// 持续发送时也检查 Ping，不能让繁忙输出饿死心跳。
			if !s.pingIfDue(ticks) {
				return
			}
			continue
		}
		if sealed {
			s.terminate(websocket.CloseGoingAway, "server stopping")
			return
		}
		select {
		case <-s.sendWake:
		case <-s.detached:
			return
		case <-e.outputSealed:
		case <-ticks:
			if !s.ping() {
				return
			}
		}
	}
}

// 正常写入与过期丢弃都检查心跳，持续到来的过期消息不能饿死控制帧。
func (s *Session) pingIfDue(ticks <-chan time.Time) bool {
	select {
	case <-ticks:
		return s.ping()
	default:
		return true
	}
}

func (s *Session) ping() bool {
	if err := s.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.endpoint.config.WriteTimeout)); err != nil {
		s.terminate(websocket.CloseInternalServerErr, "heartbeat failed")
		return false
	}
	return true
}
