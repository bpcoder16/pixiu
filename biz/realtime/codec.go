package realtime

import (
	"encoding/json"
	"unicode/utf8"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func validID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range []byte(value) {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

func decodeEnvelope(protocolName string, data []byte) (*Message, error) {
	var message *Message
	var err error
	if protocolName == SubprotocolJSON {
		message, err = decodeJSON(data)
	} else {
		message, err = decodePB(data)
	}
	if err != nil {
		return nil, err
	}
	if !validID(message.requestID) || !validID(message.scene) || !validID(message.action) {
		return nil, ErrInvalidMessage
	}
	return message, nil
}

func decodePB(data []byte) (*Message, error) {
	if !validPBRequestFields(data) {
		return nil, ErrInvalidMessage
	}
	var envelope protocol.Envelope
	if err := proto.Unmarshal(data, &envelope); err != nil {
		return nil, ErrInvalidMessage
	}
	body, ok := envelope.Body.(*protocol.Envelope_Data)
	if envelope.Kind != protocol.Kind_REQUEST || !ok {
		return nil, ErrInvalidMessage
	}
	return &Message{
		requestID: envelope.RequestId,
		scene:     envelope.Scene,
		action:    envelope.Action,
		data:      body.Data,
	}, nil
}

// 只检查信封顶层。跳过长度前缀字段时保留 data 原样，业务正文由 Message.Decode 解析。
func validPBRequestFields(data []byte) bool {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		// 请求禁用 Envelope.error（字段 6），即使后面还有 data；group 在跳过字段值前拒绝。
		if n < 0 || num == 6 || typ == protowire.StartGroupType || typ == protowire.EndGroupType {
			return false
		}
		data = data[n:]
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return false
		}
		data = data[n:]
	}
	return true
}

func decodeJSON(data []byte) (*Message, error) {
	var envelope struct {
		Kind      protocol.Kind   `json:"kind"`
		RequestID string          `json:"requestId"`
		Scene     string          `json:"scene"`
		Action    string          `json:"action"`
		Data      json.RawMessage `json:"data"`
		Error     json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return nil, ErrInvalidMessage
	}
	// RawMessage 可区分 data 缺失与显式 null，同时保证 error:null 也会被拒绝。
	if envelope.Kind != protocol.Kind_REQUEST || len(envelope.Data) == 0 || len(envelope.Error) != 0 {
		return nil, ErrInvalidMessage
	}
	return &Message{
		requestID: envelope.RequestID,
		scene:     envelope.Scene,
		action:    envelope.Action,
		data:      envelope.Data,
	}, nil
}

func validateOutgoing(message Outgoing) error {
	if !validID(message.Type) {
		return ErrInvalidMessage
	}
	if message.Kind == protocol.Kind_RESPONSE {
		if !validID(message.RequestID) {
			return ErrInvalidMessage
		}
	} else if message.Kind != protocol.Kind_EVENT || message.RequestID != "" {
		return ErrInvalidMessage
	}
	hasData := !nilMessage(message.Data)
	if hasData == (message.Error != nil) {
		return ErrInvalidMessage
	}
	if message.Error != nil && (message.Kind != protocol.Kind_RESPONSE || !validID(message.Error.Code) ||
		len(message.Error.Message) > 1024 || !utf8.ValidString(message.Error.Message)) {
		return ErrInvalidMessage
	}
	return nil
}

func encodeEnvelope(protocolName string, message Outgoing, maxSendBytes int64) (websocketx.Message, error) {
	if err := validateOutgoing(message); err != nil {
		return websocketx.Message{}, err
	}
	frame := websocketx.Message{ExpiresAt: message.ExpiresAt}
	var body []byte
	var err error
	if message.Error == nil {
		if protocolName == SubprotocolJSON {
			body, err = protojson.Marshal(message.Data)
		} else {
			body, err = proto.Marshal(message.Data)
		}
		if err != nil {
			return frame, err
		}
		// PB 正文超限时提前省去信封编码；JSON 以编码后的完整帧为准。
		if protocolName != SubprotocolJSON && int64(len(body)) > maxSendBytes {
			return frame, websocketx.ErrMessageTooLarge
		}
	}
	if protocolName == SubprotocolJSON {
		// JSON 正文是 ProtoJSON 值；直接编码 PB Envelope 会把 bytes data 变成 base64。
		envelope := jsonEnvelope{
			Kind:      message.Kind,
			RequestID: message.RequestID,
			Type:      message.Type,
			Data:      body,
		}
		if message.Error != nil {
			envelope.Error = &jsonError{
				Code:    message.Error.Code,
				Message: message.Error.Message,
			}
		}
		frame.Type = websocketx.Text
		frame.Data, err = json.Marshal(envelope)
	} else {
		envelope := &protocol.Envelope{
			Kind:      message.Kind,
			RequestId: message.RequestID,
			Type:      message.Type,
		}
		if message.Error != nil {
			envelope.Body = &protocol.Envelope_Error{Error: &protocol.Error{
				Code:    message.Error.Code,
				Message: message.Error.Message,
			}}
		} else {
			envelope.Body = &protocol.Envelope_Data{Data: body}
		}
		frame.Type = websocketx.Binary
		frame.Data, err = proto.Marshal(envelope)
	}
	if err != nil {
		return frame, err
	}
	// 信封开销和 JSON 转义也占额度。
	if int64(len(frame.Data)) > maxSendBytes {
		return frame, websocketx.ErrMessageTooLarge
	}
	return frame, nil
}

type jsonEnvelope struct {
	Kind      protocol.Kind   `json:"kind"`
	RequestID string          `json:"requestId,omitempty"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     *jsonError      `json:"error,omitempty"`
}

type jsonError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
