package realtime

import (
	"errors"
	"strings"
	"testing"

	"github.com/bpcoder16/pixiu/biz/realtime/protocol"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestJSONRequestKind(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		valid bool
	}{
		{
			name:  "request",
			field: `"kind":1,`,
			valid: true,
		},
		{
			name:  "response",
			field: `"kind":2,`,
		},
		{
			name:  "event",
			field: `"kind":3,`,
		},
		{
			name:  "unspecified",
			field: `"kind":0,`,
		},
		{
			name:  "unknown",
			field: `"kind":99,`,
		},
		{
			name:  "negative",
			field: `"kind":-1,`,
		},
		{name: "missing"},
		{
			name:  "null",
			field: `"kind":null,`,
		},
		{
			name:  "legacy-string",
			field: `"kind":"request",`,
		},
		{
			name:  "enum-name",
			field: `"kind":"REQUEST",`,
		},
		{
			name:  "numeric-string",
			field: `"kind":"1",`,
		},
		{
			name:  "float",
			field: `"kind":1.0,`,
		},
		{
			name:  "fraction",
			field: `"kind":1.5,`,
		},
		{
			name:  "overflow",
			field: `"kind":2147483648,`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wire := []byte(`{` + tc.field + `"requestId":"r1","scene":"home","action":"refresh","data":{}}`)
			message, err := decodeEnvelope(SubprotocolJSON, wire)
			if !tc.valid {
				if !errors.Is(err, ErrInvalidMessage) || message != nil {
					t.Fatalf("非法 kind 应拒绝: message=%v, err=%v", message, err)
				}
				return
			}
			if err != nil || message.RequestID() != "r1" || message.Scene() != "home" ||
				message.Action() != "refresh" || string(message.data) != "{}" {
				t.Fatalf("合法请求应保留信封与正文: message=%v, err=%v", message, err)
			}
		})
	}
}

func TestOutgoingType(t *testing.T) {
	for _, protocolName := range []string{SubprotocolJSON, SubprotocolProtobuf} {
		for _, message := range []struct {
			name string
			out  Outgoing
		}{
			{
				name: "response",
				out: Outgoing{
					Kind:      protocol.Kind_RESPONSE,
					RequestID: "r1",
					Data:      &emptypb.Empty{},
				},
			},
			{
				name: "error",
				out: Outgoing{
					Kind:      protocol.Kind_RESPONSE,
					RequestID: "r1",
					Error: &Error{
						Code:    "FAIL",
						Message: "业务失败",
					},
				},
			},
			{
				name: "event",
				out: Outgoing{
					Kind: protocol.Kind_EVENT,
					Data: &emptypb.Empty{},
				},
			},
		} {
			for _, tc := range []struct {
				name  string
				value string
				valid bool
			}{
				{name: "missing"},
				{
					name:  "invalid",
					value: "order/paid",
				},
				{
					name:  "custom",
					value: "order.paid-v2_1",
					valid: true,
				},
				{
					name:  "max",
					value: strings.Repeat("a", 128),
					valid: true,
				},
				{
					name:  "too-long",
					value: strings.Repeat("a", 129),
				},
			} {
				t.Run(protocolName+"/"+message.name+"/"+tc.name, func(t *testing.T) {
					out := message.out
					out.Type = tc.value
					_, err := encodeEnvelope(protocolName, out, 1<<20)
					if tc.valid {
						if err != nil {
							t.Fatalf("合法业务类型应允许发送: %v", err)
						}
					} else if !errors.Is(err, ErrInvalidMessage) {
						t.Fatalf("缺失或非法业务类型应拒绝: %v", err)
					}
				})
			}
		}
	}
}
