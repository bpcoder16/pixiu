package realtime

import (
	"context"
	"time"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"google.golang.org/protobuf/proto"
)

// Error 的内容由业务显式授权公开；不会自动将内部 error 转成此类型。
type Error struct {
	Code    string
	Message string
}

// Outgoing 是下发消息，只允许 protocol.Kind_RESPONSE、protocol.Kind_EVENT。
// Type 必须由业务显式指定，用于客户端分发和选择正文解码类型，不使用上行路由字段。
// Data/Error 必须且只能指定一个，Error 只用于响应。
// 响应必须携带 RequestID，事件不携带。调用期间不得修改消息及其正文。
type Outgoing struct {
	Kind      protocol.Kind
	RequestID string
	Type      string
	Data      proto.Message
	Error     *Error
	// ExpiresAt 为零时不过期，非零时在入队和实际写出前检查。
	ExpiresAt time.Time
}

// Session 代表单次连接；身份固定，发送并发安全，业务状态的并发安全由业务保证。
// 会话不是分布式寻址句柄，不跨重连复用。
type Session struct {
	raw          *websocketx.Session
	info         ClientInfo
	state        any
	maxSendBytes int64 // 含信封的单帧发送上限。
}

func (s *Session) ID() string               { return s.raw.ID() }
func (s *Session) Info() ClientInfo         { return s.info }
func (s *Session) State() any               { return s.state }
func (s *Session) Subprotocol() string      { return s.raw.Subprotocol() }
func (s *Session) RemoteAddr() string       { return s.raw.RemoteAddr() }
func (s *Session) Context() context.Context { return s.raw.Context() }
func (s *Session) Done() <-chan struct{}    { return s.raw.Done() }

// Close 发起关闭并丢弃未写消息，不等待回调收尾；返回值沿用 websocketx。
func (s *Session) Close(code int, reason string) (bool, error) {
	return s.raw.Close(code, reason)
}

// Send 同步编码并接纳到有界队列；成功不表示已写入网络或对端收到。
func (s *Session) Send(ctx context.Context, message Outgoing) error {
	return s.send(ctx, nil, message)
}

func (s *Session) send(ctx, requestCtx context.Context, message Outgoing) error {
	if ctx == nil {
		return websocketx.ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.Context().Err() != nil {
		return websocketx.ErrClosed
	}
	frame, err := encodeEnvelope(s.Subprotocol(), message, s.maxSendBytes)
	if err != nil {
		return err
	}
	return s.raw.SendWhileActive(ctx, requestCtx, frame)
}

// Reply 使用业务指定的消息类型关联当前连接的请求，只回显 requestId。
// 请求回调结束或超时后不能继续通过此方法回复。
func (s *Session) Reply(ctx context.Context, request *Message, messageType string, data proto.Message) error {
	return s.reply(ctx, request, messageType, data, nil)
}

// ReplyError 使用业务指定的消息类型下发公开错误，不使用内部错误文本。
func (s *Session) ReplyError(ctx context.Context, request *Message, messageType, code, message string) error {
	return s.reply(ctx, request, messageType, nil, &Error{
		Code:    code,
		Message: message,
	})
}

func (s *Session) reply(ctx context.Context, request *Message, messageType string, data proto.Message, publicError *Error) error {
	if request == nil || request.session != s {
		return ErrInvalidMessage
	}
	if err := request.ctx.Err(); err != nil {
		return err
	}
	return s.send(ctx, request.ctx, Outgoing{
		Kind:      protocol.Kind_RESPONSE,
		RequestID: request.requestID,
		Type:      messageType,
		Data:      data,
		Error:     publicError,
	})
}
