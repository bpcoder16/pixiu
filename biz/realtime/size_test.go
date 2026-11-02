package realtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"github.com/bpcoder16/pixiu/infra/websocketx"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func outboundSize(t *testing.T, protocolName string, payload proto.Message) int64 {
	t.Helper()
	var wire []byte
	var err error
	if protocolName == SubprotocolJSON {
		body, marshalErr := protojson.Marshal(payload)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		wire, err = json.Marshal(map[string]any{
			"kind":   "event",
			"scene":  "home",
			"action": "refresh",
			"data":   json.RawMessage(body),
		})
	} else {
		body, marshalErr := proto.Marshal(payload)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		wire, err = proto.Marshal(&protocol.Envelope{
			Kind:   protocol.Kind_EVENT,
			Scene:  "home",
			Action: "refresh",
			Body:   &protocol.Envelope_Data{Data: body},
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	return int64(len(wire))
}

func TestSendSizeBoundary(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, mode := range []string{"body", "spaces", "envelope", "exact", "default", "depth-and-size"} {
			t.Run(protocolName+"/"+mode, func(t *testing.T) {
				var payload proto.Message = wrapperspb.String("<>&sample")
				limit := outboundSize(t, protocolName, payload)
				switch mode {
				case "body":
					payload = wrapperspb.Bytes(make([]byte, 4096))
					limit = 64
				case "spaces":
					payload = wrapperspb.String(strings.Repeat(" ", 4096))
					limit = 64
				case "envelope":
					limit--
				case "default":
					payload = wrapperspb.Bytes(make([]byte, 64<<10))
					limit = 0 // 验证 Server 归一化的默认上限也传到了发送路径。
				case "depth-and-size":
					value := structpb.NewStringValue("x")
					for range 33 {
						value = structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{value}})
					}
					payload = value
					limit = outboundSize(t, protocolName, payload) - 1
				}
				opened := make(chan *Session, 1)
				cfg := baseConfig()
				cfg.Transport.MaxSendBytes = limit
				cfg.OnOpen = func(_ context.Context, s *Session) error {
					opened <- s
					return nil
				}
				_, h := startTestServer(t, cfg)
				client := dial(t, h, protocolName)
				session := await(t, opened)
				message := Outgoing{
					Kind:   Event,
					Scene:  "home",
					Action: "refresh",
					Data:   payload,
				}
				err := session.Send(context.Background(), message)
				want := payload
				if mode == "exact" {
					if err != nil {
						t.Fatalf("恰好达到上限应允许发送: %v", err)
					}
				} else {
					if !errors.Is(err, websocketx.ErrMessageTooLarge) {
						t.Fatalf("超长应返回尺寸错误: %v", err)
					}
					want = wrapperspb.String("ok")
					message.Data = want
					if err := session.Send(context.Background(), message); err != nil {
						t.Fatal(err)
					}
				}
				// 拒绝后仍能发送，且客户端看到的首条消息不能是超限正文。
				body := responseBody(t, client, "event")
				got := want.ProtoReflect().New().Interface()
				if protocolName == SubprotocolJSON {
					err = protojson.Unmarshal(body, got)
				} else {
					err = proto.Unmarshal(body, got)
				}
				if err != nil || !proto.Equal(got, want) {
					t.Fatalf("发送正文错误: %v", err)
				}
			})
		}
	}
}

// JSON 空白压缩不能改变是否接纳；覆盖原始正文可能比含信封帧还长的重复标量。
func TestSendJSONCompactSize(t *testing.T) {
	payload := &structpb.ListValue{}
	for range 512 {
		payload.Values = append(payload.Values, structpb.NewNumberValue(0))
	}
	limit := outboundSize(t, SubprotocolJSON, payload)
	opened := make(chan *Session, 1)
	cfg := baseConfig()
	cfg.Transport.MaxSendBytes = limit
	cfg.OnOpen = func(_ context.Context, s *Session) error {
		opened <- s
		return nil
	}
	_, h := startTestServer(t, cfg)
	client := dial(t, h, SubprotocolJSON)
	session := await(t, opened)
	if err := session.Send(context.Background(), Outgoing{
		Kind:   Event,
		Scene:  "home",
		Action: "refresh",
		Data:   payload,
	}); err != nil {
		t.Fatalf("压缩后完整帧恰好达到上限，应允许发送: %v", err)
	}
	var got structpb.ListValue
	if err := protojson.Unmarshal(responseBody(t, client, "event"), &got); err != nil || !proto.Equal(&got, payload) {
		t.Fatalf("正文错误: %v", err)
	}
}

// ProtoJSON 是否插入空白随构建变化，用固定合法 JSON 覆盖可压缩空白的下界约束。
func TestBodyLengthLowerBound(t *testing.T) {
	for _, body := range []string{
		"[0, 0, 0]",
		"\n {\t\"v\": 1 \r}\n",
		`{"v": "  a b  "}`,
		`{"v": "\" \\ \t 雪"}`,
	} {
		var compact bytes.Buffer
		if err := json.Compact(&compact, []byte(body)); err != nil {
			t.Fatal(err)
		}
		if bodyExceedsLimit(SubprotocolJSON, []byte(body), int64(compact.Len())) {
			t.Fatalf("JSON 下界不能高于压缩后的实际长度: %q", body)
		}
		if !bodyExceedsLimit(SubprotocolProtobuf, []byte(body), int64(len(body)-1)) {
			t.Fatal("PB 下界应计入全部原始字节")
		}
	}
	if !bodyExceedsLimit(SubprotocolJSON, []byte(`"abcdef"`), 3) {
		t.Fatal("明显超限的 JSON 正文未被预检查拒绝")
	}
}
