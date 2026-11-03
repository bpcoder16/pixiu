package realtime

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// PushKind 指定一个端内的推送范围。具体端由调用方选择的主题决定。
type PushKind string

const (
	PushBroadcast PushKind = "broadcast"
	PushUser      PushKind = "user"
	PushDevice    PushKind = "device"
)

// PushCommand 是跨节点事件。使用 MarshalPushCommand 和 UnmarshalPushCommand 读写总线 JSON。
type PushCommand struct {
	Kind    PushKind
	ID      string
	Message PushMessage
}

const maxPushCommandBytes = 1 << 20

func (c PushCommand) validTarget() bool {
	switch c.Kind {
	case PushBroadcast:
		return c.ID == ""
	case PushUser, PushDevice:
		return strings.TrimSpace(c.ID) != ""
	default:
		return false
	}
}

// 可选字段保留原始值，区分缺失、显式 null 以及合法的 ProtoJSON null 正文。
type pushCommandJSON struct {
	Kind    PushKind       `json:"kind"`
	ID      jsontext.Value `json:"id,omitzero"`
	Message struct {
		Type      string         `json:"type"`
		Data      jsontext.Value `json:"data,omitzero"`
		Error     jsontext.Value `json:"error,omitzero"`
		ExpiresAt jsontext.Value `json:"expiresAt,omitzero"`
	} `json:"message"`
}

// MarshalPushCommand 将命令编码为最多 1 MiB 的 JSON。data 直接嵌入业务 ProtoJSON 值。
func MarshalPushCommand(cmd PushCommand) ([]byte, error) {
	if !cmd.validTarget() {
		return nil, ErrInvalidMessage
	}
	msg := cmd.Message
	if err := validateOutgoing(msg.outgoing()); err != nil {
		return nil, err
	}
	wire := pushCommandJSON{Kind: cmd.Kind}
	wire.Message.Type = msg.Type
	var err error
	if cmd.ID != "" {
		wire.ID, err = json.Marshal(cmd.ID)
		if err != nil {
			return nil, fmt.Errorf("%w: id: %v", ErrInvalidMessage, err)
		}
	}
	if msg.Error != nil {
		wire.Message.Error, err = json.Marshal((*jsonError)(msg.Error))
	} else {
		wire.Message.Data, err = protojson.Marshal(msg.Data)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: body: %v", ErrInvalidMessage, err)
	}
	if !msg.ExpiresAt.IsZero() {
		wire.Message.ExpiresAt, err = msg.ExpiresAt.MarshalJSON()
		if err != nil {
			return nil, fmt.Errorf("%w: expiresAt: %v", ErrInvalidMessage, err)
		}
	}
	data, err := json.Marshal(wire)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	if len(data) > maxPushCommandBytes {
		return nil, ErrInvalidMessage
	}
	return data, nil
}

// UnmarshalPushCommand 解析总线 JSON。newData 根据业务 type 返回全新的可写 proto，
// 未知类型返回 nil。error 事件不调用 newData。截止时间保留给投递和写出时检查。
// 已知类型的 data 忽略本地 proto 未定义的字段，使滚动发布中的旧节点也能投递已知字段。
func UnmarshalPushCommand(data []byte, newData func(string) proto.Message) (PushCommand, error) {
	if len(data) > maxPushCommandBytes {
		return PushCommand{}, ErrInvalidMessage
	}
	var wire pushCommandJSON
	if err := json.Unmarshal(data, &wire, json.RejectUnknownMembers(true)); err != nil {
		return PushCommand{}, fmt.Errorf("%w: %v", ErrInvalidMessage, err)
	}
	cmd := PushCommand{Kind: wire.Kind}
	if wire.ID != nil {
		if cmd.Kind == PushBroadcast || json.Unmarshal(wire.ID, &cmd.ID) != nil {
			return PushCommand{}, ErrInvalidMessage
		}
	}
	body := wire.Message
	if !cmd.validTarget() || !validID(body.Type) || (body.Data == nil) == (body.Error == nil) {
		return PushCommand{}, ErrInvalidMessage
	}
	msg := &cmd.Message
	msg.Type = body.Type
	if body.ExpiresAt != nil {
		if err := msg.ExpiresAt.UnmarshalJSON(body.ExpiresAt); err != nil || msg.ExpiresAt.IsZero() {
			return PushCommand{}, ErrInvalidMessage
		}
	}
	if body.Error != nil {
		var problem jsonError
		if err := json.Unmarshal(body.Error, &problem, json.RejectUnknownMembers(true)); err != nil {
			return PushCommand{}, fmt.Errorf("%w: error: %v", ErrInvalidMessage, err)
		}
		msg.Error = (*Error)(&problem)
	} else {
		if newData == nil {
			return PushCommand{}, ErrInvalidMessage
		}
		msg.Data = newData(msg.Type)
		if nilMessage(msg.Data) {
			return PushCommand{}, fmt.Errorf("%w: unknown data type %q", ErrInvalidMessage, msg.Type)
		}
		options := protojson.UnmarshalOptions{DiscardUnknown: true}
		if err := options.Unmarshal(body.Data, msg.Data); err != nil {
			return PushCommand{}, fmt.Errorf("%w: data: %v", ErrInvalidMessage, err)
		}
	}
	if err := validateOutgoing(msg.outgoing()); err != nil {
		return PushCommand{}, err
	}
	return cmd, nil
}
