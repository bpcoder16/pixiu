package realtime

import (
	"context"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type depthCase struct {
	name    string
	message proto.Message
}

func depthCases(t *testing.T) []depthCase {
	t.Helper()
	// 保留超过旧 32 层限制的消息，以及各自具有不同 ProtoJSON 映射的类型。
	repeated := &descriptorpb.DescriptorProto{Name: proto.String("leaf")}
	for range 63 {
		repeated = &descriptorpb.DescriptorProto{NestedType: []*descriptorpb.DescriptorProto{repeated}}
	}
	list := structpb.NewStringValue("leaf")
	object := structpb.NewStringValue("leaf")
	var packed proto.Message = wrapperspb.String("leaf")
	for range 33 {
		list = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{list}})
		object = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"child": object}})
		var err error
		packed, err = anypb.New(packed)
		if err != nil {
			t.Fatal(err)
		}
	}
	return []depthCase{
		{name: "repeated-64", message: repeated},
		{name: "list-33", message: list},
		{name: "map-33", message: object},
		{name: "any-33", message: packed},
		{
			name:    "bytes-are-opaque",
			message: wrapperspb.Bytes([]byte(strings.Repeat("[", 40) + `"opaque"` + strings.Repeat("]", 40))),
		},
	}
}

// Send 只编码和检查帧大小，正文递归限制由接收方决定，且不修改调用方对象。
func TestSendDepthBoundary(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, tc := range depthCases(t) {
			t.Run(protocolName+"/"+tc.name, func(t *testing.T) {
				opened := make(chan *Session, 1)
				cfg := baseConfig()
				cfg.OnOpen = func(_ context.Context, s *Session) error {
					opened <- s
					return nil
				}
				_, h := startTestServer(t, cfg)
				client := dial(t, h, protocolName)
				session := await(t, opened)
				before := proto.Clone(tc.message)
				message := Outgoing{
					Kind: protocol.Kind_EVENT,
					Type: "home.refreshed",
					Data: tc.message,
				}
				if err := session.Send(context.Background(), message); err != nil {
					t.Fatalf("发送不应附加正文深度限制: %v", err)
				}
				body := responseBody(t, client, protocol.Kind_EVENT)
				got := message.Data.ProtoReflect().New().Interface()
				var err error
				if protocolName == SubprotocolJSON {
					err = protojson.Unmarshal(body, got)
				} else {
					err = proto.Unmarshal(body, got)
				}
				if err != nil || !proto.Equal(got, message.Data) {
					t.Fatalf("线上正文不能按同编码解码: %v", err)
				}
				if !proto.Equal(tc.message, before) {
					t.Fatal("发送修改了调用方对象")
				}
			})
		}
	}
}

// 信封不提前扫描正文深度，Message.Decode 沿用 JSON/PB 各自的默认递归保护。
func TestReceiveDepthBoundary(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, tc := range depthCases(t) {
			t.Run(protocolName+"/"+tc.name, func(t *testing.T) {
				decoded := make(chan error, 1)
				got := tc.message.ProtoReflect().New().Interface()
				cfg := baseConfig()
				cfg.OnMessage = func(_ context.Context, _ *Session, message *Message) error {
					decoded <- message.Decode(got)
					return nil
				}
				_, h := startTestServer(t, cfg)
				client := dial(t, h, protocolName)
				frameType, wire := requestWire(t, protocolName, tc.message)
				if err := client.WriteMessage(frameType, wire); err != nil {
					t.Fatal(err)
				}
				if err := await(t, decoded); err != nil {
					t.Fatalf("合法深层正文不应被固定 32 层限制拒绝: %v", err)
				}
				if !proto.Equal(got, tc.message) {
					t.Fatal("深层正文解码后内容不一致")
				}
			})
		}
	}
}
