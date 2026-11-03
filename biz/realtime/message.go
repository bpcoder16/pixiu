package realtime

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Message 保存已验证信封和未解码业务正文，仅在业务 Handler 执行期间有效。
type Message struct {
	requestID string
	scene     string
	action    string
	data      []byte
	session   *Session
	ctx       context.Context
}

func (m *Message) RequestID() string { return m.requestID }
func (m *Message) Scene() string     { return m.scene }
func (m *Message) Action() string    { return m.action }

// Decode 根据协商协议解析业务 .proto 消息；dst 必须是独立的可写对象。
// 不复用全局原型，也不忽略未知业务 JSON 字段。失败时 dst 可能已部分写入。
func (m *Message) Decode(dst proto.Message) error {
	if m == nil || m.session == nil || nilMessage(dst) {
		return ErrInvalidMessage
	}
	if err := m.ctx.Err(); err != nil {
		return err
	}
	var err error
	switch m.session.Subprotocol() {
	case SubprotocolJSON:
		err = protojson.Unmarshal(m.data, dst)
	default:
		err = proto.Unmarshal(m.data, dst)
	}
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	return nil
}

func nilMessage(message proto.Message) bool {
	return message == nil || !message.ProtoReflect().IsValid()
}
