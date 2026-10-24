package websocketx

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gorilla/websocket"
)

func (s *Session) readLoop() {
	defer close(s.readDone)
	e := s.endpoint
	s.conn.SetReadLimit(e.config.ReadLimit) // 限制线上数据；下面另限制解压后的数据。
	if !e.config.DisableHeartbeat {
		_ = s.conn.SetReadDeadline(time.Now().Add(e.config.PongTimeout))
		s.conn.SetPongHandler(func(string) error {
			return s.conn.SetReadDeadline(time.Now().Add(e.config.PongTimeout))
		})
	}
	s.conn.SetCloseHandler(func(code int, _ string) error {
		if code == websocket.CloseNoStatusReceived {
			code = websocket.CloseNormalClosure
		}
		s.terminate(code, "peer closed")
		return nil
	})
	var scratch [4096]byte
	for {
		typ, reader, err := s.conn.NextReader()
		if err != nil {
			s.failRead(err)
			return
		}
		remaining := e.config.ReadLimit
		if e.inputStopped.Load() {
			if !s.discardInput(reader, scratch[:], remaining) {
				return
			}
			continue
		}
		data, err := s.readData(reader, scratch[:], &remaining)
		if err != nil {
			if err == io.EOF {
				// 停收期间已经读完本条消息，不重复读取已归还池的解压器。
				continue
			}
			if errors.Is(err, ErrClosed) && !isClosed(s.detached) && !e.canceled.Load() {
				if !s.discardInput(reader, scratch[:], remaining) {
					return
				}
				continue
			}
			s.failRead(err)
			return
		}
		if typ == websocket.TextMessage && !utf8.Valid(data) {
			s.releaseReceive(int64(len(data)))
			s.terminateWithError(websocket.CloseInvalidFramePayloadData, "invalid UTF-8", ErrInvalidMessage)
			return
		}
		// 完整消息接纳与 StopInput 使用同一把门，半条消息不进入存量队列。
		e.mu.RLock()
		s.mu.Lock()
		accepted := !e.inputStopped.Load() && !s.closed
		if accepted {
			s.receive.push(incoming{
				message: Message{
					Type: MessageType(typ),
					Data: data,
				},
				accepted: time.Now(),
			}, e.config.ReceiveQueueSize)
			notify(s.receiveWake)
		}
		s.mu.Unlock()
		e.mu.RUnlock()
		if !accepted {
			s.releaseReceive(int64(len(data)))
			continue
		}
		e.observeFresh(s.ctx, Event{
			Kind:         EventReceived,
			ConnectionID: s.id,
			Type:         MessageType(typ),
			Bytes:        int64(len(data)),
			Usage:        s.Usage(),
		})
	}
}

// failRead 统一帧头、正文和停收丢弃的错误归因，不把底层错误原文带入日志。
func (s *Session) failRead(err error) {
	if s.endpoint.canceled.Load() {
		s.terminate(websocket.CloseGoingAway, "canceled")
		return
	}
	code := websocket.CloseAbnormalClosure
	var cause error = net.ErrClosed
	var closeErr *websocket.CloseError
	var networkErr net.Error
	switch {
	case errors.Is(err, ErrMessageTooLarge), errors.Is(err, websocket.ErrReadLimit):
		code, cause = websocket.CloseMessageTooBig, ErrMessageTooLarge
	case errors.Is(err, ErrQueueFull):
		code, cause = websocket.ClosePolicyViolation, ErrQueueFull
	case errors.Is(err, ErrBudgetExceeded):
		code, cause = websocket.CloseTryAgainLater, ErrBudgetExceeded
	case errors.As(err, &closeErr):
		code = closeErr.Code
	case errors.As(err, &networkErr):
		if networkErr.Timeout() {
			cause = context.DeadlineExceeded
		}
	case strings.HasPrefix(err.Error(), "websocket: "):
		// Gorilla v1.5.3 的协议解析错误没有导出类型，仅识别固定前缀。
		code, cause = websocket.CloseProtocolError, ErrInvalidMessage
	}
	s.terminateWithError(code, "receive failed", cause)
}

func (s *Session) discardInput(reader io.Reader, scratch []byte, remaining int64) bool {
	// 沿用本条消息尚余的解压额度；停收不能让已读的部分重新获得额度。
	for {
		if remaining == 0 {
			var probe [1]byte
			n, err := reader.Read(probe[:])
			if n == 0 && err == io.EOF {
				return true
			}
			if err != nil && err != io.EOF {
				s.failRead(err)
				return false
			}
			s.failRead(ErrMessageTooLarge)
			return false
		}
		n, err := reader.Read(scratch[:min(int64(len(scratch)), remaining)])
		remaining -= int64(n)
		if err == io.EOF {
			return true
		}
		if err != nil {
			s.failRead(err)
			return false
		}
	}
}

// readData 在分配/保留数据前预留读取槽位和分块字节；出错时释放一次。
// io.EOF 表示停收后的探测已到消息末尾；ErrClosed 表示仍需有界丢弃余下输入。
func (s *Session) readData(reader io.Reader, scratch []byte, remaining *int64) (data []byte, result error) {
	e := s.endpoint
	e.mu.RLock()
	s.mu.Lock()
	if e.inputStopped.Load() || s.closed {
		s.mu.Unlock()
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	if s.usage.ReceiveMessages >= int64(e.config.ReceiveQueueSize) {
		s.mu.Unlock()
		e.mu.RUnlock()
		return nil, ErrQueueFull
	}
	if !e.config.Budget.reserve(true, 1, 0) {
		s.mu.Unlock()
		e.mu.RUnlock()
		return nil, ErrBudgetExceeded
	}
	s.usage.ReceiveMessages++
	s.mu.Unlock()
	e.mu.RUnlock()
	defer func() {
		if result != nil {
			s.releaseReceive(int64(len(data)))
			data = nil
		}
	}()
	for {
		s.mu.Lock()
		if s.closed || e.inputStopped.Load() {
			s.mu.Unlock()
			return data, ErrClosed
		}
		localAvailable := e.config.ReceiveQueueBytes - s.usage.ReceiveBytes
		maximum := min(int64(len(scratch)), *remaining, localAvailable)
		reserved := e.config.Budget.reserveReadBytes(maximum)
		s.usage.ReceiveBytes += reserved
		s.mu.Unlock()
		if reserved == 0 {
			// 固定一个探测字节只区分 EOF 与超额，不进入消息缓冲或队列。
			var probe [1]byte
			n, err := reader.Read(probe[:])
			*remaining -= int64(n)
			if n == 0 && err == io.EOF {
				return data, nil
			}
			if err != nil && err != io.EOF {
				return data, err
			}
			if *remaining < 0 {
				return data, ErrMessageTooLarge
			}
			// 停收若在探测读取期间生效，转为有界丢弃，不能再误报接收过载。
			if e.inputStopped.Load() {
				if err == io.EOF {
					return data, io.EOF
				}
				return data, ErrClosed
			}
			if localAvailable == 0 {
				return data, ErrQueueFull
			}
			return data, ErrBudgetExceeded
		}
		n, err := reader.Read(scratch[:reserved])
		*remaining -= int64(n)
		s.mu.Lock()
		unused := reserved - int64(n)
		s.usage.ReceiveBytes -= unused
		e.config.Budget.release(true, 0, unused)
		s.mu.Unlock()
		data = append(data, scratch[:n]...)
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return data, err
		}
	}
}
