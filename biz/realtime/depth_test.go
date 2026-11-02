package realtime

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type depthCase struct {
	name       string
	message    proto.Message
	jsonOK     bool
	pbOK       bool
	envelopeOK bool // JSON 正文容器检查先于 Message.Decode，超限时直接关闭连接。
}

func depthCases(t *testing.T) []depthCase {
	t.Helper()
	var cases []depthCase
	// repeated message 的 JSON 数组计入容器深度；PB 按解码器自身的消息递归计数。
	for _, n := range []int{15, 16, 17, 31, 32, 33} {
		message := &descriptorpb.DescriptorProto{Name: proto.String("leaf")}
		for i := 1; i < n; i++ {
			message = &descriptorpb.DescriptorProto{NestedType: []*descriptorpb.DescriptorProto{message}}
		}
		cases = append(cases, depthCase{
			name:       fmt.Sprintf("repeated-%d", n),
			message:    message,
			jsonOK:     n <= 16,
			pbOK:       n <= 32,
			envelopeOK: n <= 16,
		})
	}
	// Value/ListValue 和 Value/Struct 的 ProtoJSON 定制映射与 PB 的嵌套层数不同。
	for _, shape := range []string{"list", "map"} {
		levels := []int{15, 16, 31, 32, 33}
		if shape == "map" {
			levels = []int{10, 11, 31, 32, 33}
		}
		for _, n := range levels {
			message := structpb.NewStringValue("leaf")
			for i := 0; i < n; i++ {
				if shape == "list" {
					message = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{message}})
				} else {
					message = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"child": message}})
				}
			}
			pbOK := n <= 15
			if shape == "map" {
				pbOK = n <= 10
			}
			cases = append(cases, depthCase{
				name:       fmt.Sprintf("%s-%d", shape, n),
				message:    message,
				jsonOK:     n <= 31,
				pbOK:       pbOK,
				envelopeOK: n <= 32,
			})
		}
	}
	for _, n := range []int{31, 32, 33} {
		var message proto.Message = wrapperspb.String("leaf")
		for i := 0; i < n; i++ {
			var err error
			message, err = anypb.New(message)
			if err != nil {
				t.Fatal(err)
			}
		}
		cases = append(cases, depthCase{
			name:       fmt.Sprintf("any-%d", n),
			message:    message,
			jsonOK:     n <= 32,
			pbOK:       true, // PB 的 Any.value 是 bytes，本轮不额外解包或增加消息树约束。
			envelopeOK: n <= 32,
		})
	}
	cases = append(cases, depthCase{
		name:       "bytes-are-opaque",
		message:    wrapperspb.Bytes([]byte(strings.Repeat("[", 40) + `"opaque"` + strings.Repeat("]", 40))),
		jsonOK:     true,
		pbOK:       true,
		envelopeOK: true,
	})
	return cases
}

// 从公开 Send 验证边界，并检查拒绝的正文没有入队、调用方对象没有被校验修改。
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
					Kind:   Event,
					Scene:  "home",
					Action: "refresh",
					Data:   tc.message,
				}
				err := session.Send(context.Background(), message)
				wantOK := protocolName != SubprotocolJSON || tc.envelopeOK
				if !wantOK {
					if !errors.Is(err, ErrInvalidMessage) {
						t.Fatalf("JSON 容器超深应拒绝发送: %v", err)
					}
					message.Data = wrapperspb.String("next")
					if err := session.Send(context.Background(), message); err != nil {
						t.Fatal(err)
					}
				} else if err != nil {
					t.Fatalf("边界内的消息应允许发送: %v", err)
				}
				body := responseBody(t, client, "event")
				got := message.Data.ProtoReflect().New().Interface()
				if protocolName == SubprotocolJSON {
					err = protojson.Unmarshal(body, got)
				} else {
					err = proto.Unmarshal(body, got)
				}
				if err != nil || !proto.Equal(got, message.Data) {
					t.Fatalf("线上正文不能按同编码解码，或被拒消息仍入队: %v", err)
				}
				if !proto.Equal(tc.message, before) {
					t.Fatal("发送校验修改了调用方对象")
				}
			})
		}
	}
}

// 保留原有入站保护：JSON 容器超限关闭 1007，业务 Decode 超限返回 ErrInvalidMessage。
func TestReceiveDepthBoundary(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, tc := range depthCases(t) {
			t.Run(protocolName+"/"+tc.name, func(t *testing.T) {
				decoded := make(chan error, 1)
				cfg := baseConfig()
				cfg.OnMessage = func(_ context.Context, _ *Session, message *Message) error {
					decoded <- message.Decode(tc.message.ProtoReflect().New().Interface())
					return nil
				}
				_, h := startTestServer(t, cfg)
				client := dial(t, h, protocolName)
				frameType, wire := requestWire(t, protocolName, tc.message)
				if err := client.WriteMessage(frameType, wire); err != nil {
					t.Fatal(err)
				}
				if protocolName == SubprotocolJSON && !tc.envelopeOK {
					_ = client.SetReadDeadline(time.Now().Add(3 * time.Second))
					_, _, err := client.ReadMessage()
					if !websocket.IsCloseError(err, websocket.CloseInvalidFramePayloadData) {
						t.Fatalf("容器超深应关闭 1007: %v", err)
					}
					select {
					case <-decoded:
						t.Fatal("非法信封进入业务回调")
					default:
					}
					return
				}
				wantOK := tc.pbOK
				if protocolName == SubprotocolJSON {
					wantOK = tc.jsonOK
				}
				err := await(t, decoded)
				if wantOK && err != nil || !wantOK && !errors.Is(err, ErrInvalidMessage) {
					t.Fatalf("入站深度边界变化: wantOK=%v, err=%v", wantOK, err)
				}
			})
		}
	}
}
