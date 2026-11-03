package realtime

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/typepb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestPushCommandRoundTrip(t *testing.T) {
	expiry := time.Date(2030, 1, 1, 0, 0, 0, 123, time.UTC)
	cmd := PushCommand{
		Kind: PushUser,
		ID:   "用户",
		Message: PushMessage{
			Type:      "counter",
			Data:      wrapperspb.UInt64(1 << 63),
			ExpiresAt: expiry,
		},
	}
	wire, err := MarshalPushCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"kind":"user","id":"用户","message":{"type":"counter","data":"9223372036854775808","expiresAt":"2030-01-01T00:00:00.000000123Z"}}`
	if string(wire) != want {
		t.Fatalf("总线正文 = %s", wire)
	}
	got, err := UnmarshalPushCommand(wire, func(typ string) proto.Message {
		if typ != "counter" {
			t.Fatalf("type = %q", typ)
		}
		return new(wrapperspb.UInt64Value)
	})
	if err != nil || got.Kind != cmd.Kind || got.ID != cmd.ID || !got.Message.ExpiresAt.Equal(expiry) || !proto.Equal(got.Message.Data, cmd.Message.Data) {
		t.Fatalf("还原失败: %+v, %v", got, err)
	}
	cmd = PushCommand{Kind: PushBroadcast, Message: PushMessage{Type: "notice", Error: &Error{Code: "STALE", Message: "请刷新"}}}
	wire, err = MarshalPushCommand(cmd)
	if err != nil || string(wire) != `{"kind":"broadcast","message":{"type":"notice","error":{"code":"STALE","message":"请刷新"}}}` {
		t.Fatalf("错误事件 = %s, %v", wire, err)
	}
	got, err = UnmarshalPushCommand(wire, nil)
	if err != nil || got.Message.Error == nil || *got.Message.Error != *cmd.Message.Error {
		t.Fatalf("错误事件还原失败: %+v, %v", got, err)
	}
	// google.protobuf.Value 的 null 是合法的业务正文。
	cmd = PushCommand{Kind: PushDevice, ID: "d", Message: PushMessage{Type: "value", Data: structpb.NewNullValue()}}
	wire, err = MarshalPushCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err = UnmarshalPushCommand(wire, func(string) proto.Message { return new(structpb.Value) })
	if err != nil || !proto.Equal(cmd.Message.Data, got.Message.Data) {
		t.Fatalf("null 正文丢失: %s, %v", wire, err)
	}
}

func TestPushCommandIgnoresNewDataFields(t *testing.T) {
	wire := []byte(`{"kind":"broadcast","message":{"type":"status","data":{"name":"OPEN","displayColor":"red"}}}`)
	cmd, err := UnmarshalPushCommand(wire, func(string) proto.Message { return new(typepb.EnumValue) })
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(cmd.Message.Data, &typepb.EnumValue{Name: "OPEN"}) {
		t.Fatalf("应保留已知字段: %v", cmd.Message.Data)
	}
}

func TestPushCommandRejectsAmbiguousWire(t *testing.T) {
	for _, raw := range []string{
		`{"kind":"user","message":{"type":"x","data":"v"}}`,
		`{"kind":"device","id":" ","message":{"type":"x","data":"v"}}`,
		`{"kind":"broadcast","id":"","message":{"type":"x","data":"v"}}`,
		`{"kind":"broadcast","id":null,"message":{"type":"x","data":"v"}}`,
		`{"kind":"all","message":{"type":"x","data":"v"}}`,
		`{"kind":"user","kind":"broadcast","message":{"type":"x","data":"v"}}`,
		`{"kind":"broadcast","unexpected":true,"message":{"type":"x","data":"v"}}`,
		`{"kind":"broadcast","type":"x","data":"v"}`,
		`{"kind":"broadcast","message":{"type":"!","data":"v"}}`,
		`{"kind":"broadcast","message":{"type":"x"}}`,
		`{"kind":"broadcast","message":{"type":"x","data":"v","error":null}}`,
		`{"kind":"broadcast","message":{"type":"x","data":"v","data":"w"}}`,
		`{"kind":"broadcast","message":{"type":"x","error":{"code":"!"}}}`,
		`{"kind":"broadcast","message":{"type":"x","data":{}}}`,
		`{"kind":"broadcast","message":{"type":"x","data":"v","expiresAt":null}}`,
		`{"kind":"broadcast","message":{"type":"x","data":"v","expiresAt":"tomorrow"}}`,
		`{"kind":"broadcast","message":{"type":"x","data":"` + strings.Repeat("a", 1<<20) + `"}}`,
	} {
		if _, err := UnmarshalPushCommand([]byte(raw), func(string) proto.Message { return new(wrapperspb.StringValue) }); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("非法命令被接受: %.130q, %v", raw, err)
		}
	}
	cmd := PushCommand{Kind: PushUser, ID: "u", Message: PushMessage{Type: "example", Data: wrapperspb.String("ok")}}
	wire, err := MarshalPushCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	for _, resolve := range []func(string) proto.Message{nil, func(string) proto.Message { return (*wrapperspb.StringValue)(nil) }} {
		if _, err := UnmarshalPushCommand(wire, resolve); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("无正文定义仍成功: %v", err)
		}
	}
	for _, c := range []PushCommand{
		{Kind: PushBroadcast, ID: "u", Message: cmd.Message},
		{Kind: PushDevice, Message: cmd.Message},
		{Kind: PushUser, ID: "u", Message: PushMessage{Type: "x", Data: cmd.Message.Data, Error: &Error{Code: "X"}}},
		{Kind: PushUser, ID: "u", Message: PushMessage{Type: "x", Data: wrapperspb.Bytes(bytes.Repeat([]byte{1}, 1<<20))}},
	} {
		if _, err := MarshalPushCommand(c); !errors.Is(err, ErrInvalidMessage) {
			t.Fatalf("非法命令被编码: %v", err)
		}
	}
}
