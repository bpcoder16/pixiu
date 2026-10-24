package websocketx

import (
	"compress/flate"
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
		if e.inputStopped.Load() {
			// 下一次 NextReader 会跳过未消费的线上正文；不再调用解压读取器，控制帧仍正常处理。
			continue
		}
		data, err := s.readData(reader, scratch[:])
		if err != nil {
			if errors.Is(err, ErrClosed) && !isClosed(s.detached) && !e.canceled.Load() {
				continue
			}
			s.failRead(err)
			return
		}
		if e.inputStopped.Load() {
			s.releaseReceive(int64(len(data)))
			continue
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
	var corruptErr flate.CorruptInputError
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
	case errors.As(err, &corruptErr):
		// 解压器直接返回标准库错误，不能只按 Gorilla 错误前缀分类。
		code, cause = websocket.CloseProtocolError, ErrInvalidMessage
	case strings.HasPrefix(err.Error(), "websocket: "):
		// Gorilla v1.5.3 的协议解析错误没有导出类型，仅识别固定前缀。
		code, cause = websocket.CloseProtocolError, ErrInvalidMessage
	}
	s.terminateWithError(code, "receive failed", cause)
}

// readData 在分配/保留数据前预留读取槽位和分块字节；出错时释放一次。
// 发现停收时返回 ErrClosed，由读取循环通过 NextReader 跳过剩余正文。
func (s *Session) readData(reader io.Reader, scratch []byte) (data []byte, result error) {
	e := s.endpoint
	remaining := e.config.ReadLimit
	e.mu.RLock()
	s.mu.Lock()
	if e.inputStopped.Load() || s.closed {
		s.mu.Unlock()
		e.mu.RUnlock()
		return nil, ErrClosed
	}
	// 不在唯一读循环中等待空位，以免阻塞后续 Ping/Pong/Close 控制帧的处理。
	// 队列满按过载策略拒绝新消息，返回 ErrQueueFull，由 failRead 以 1008 关闭连接。
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
		maximum := min(int64(len(scratch)), remaining, localAvailable)
		reserved := e.config.Budget.reserveReadBytes(maximum)
		s.usage.ReceiveBytes += reserved
		s.mu.Unlock()
		if reserved == 0 {
			// 固定一个探测字节只区分 EOF 与超额，不进入消息缓冲或队列。
			var probe [1]byte
			n, err := reader.Read(probe[:])
			// 本次探测期间若已停收，忽略未接纳正文的内容与大小，不再继续解压。
			if e.inputStopped.Load() {
				return data, ErrClosed
			}
			remaining -= int64(n)
			if n == 0 && err == io.EOF {
				return data, nil
			}
			if err != nil && err != io.EOF {
				return data, err
			}
			if remaining < 0 {
				return data, ErrMessageTooLarge
			}
			if localAvailable == 0 {
				return data, ErrQueueFull
			}
			return data, ErrBudgetExceeded
		}
		n, err := reader.Read(scratch[:reserved])
		s.mu.Lock()
		unused := reserved - int64(n)
		stopped := s.closed || e.inputStopped.Load()
		if stopped {
			// 已开始的读取无法中断；返回后归还整块预留额度，不再复制或保留本次正文。
			unused = reserved
		}
		s.usage.ReceiveBytes -= unused
		e.config.Budget.release(true, 0, unused)
		s.mu.Unlock()
		if stopped {
			return data, ErrClosed
		}
		remaining -= int64(n)
		data = append(data, scratch[:n]...)
		if err == io.EOF {
			return data, nil
		}
		if err != nil {
			return data, err
		}
	}
}
