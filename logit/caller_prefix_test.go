package logit

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBuiltinCallerPrefixMatchesExistingEncoding(t *testing.T) {
	now := time.Date(2026, 9, 23, 12, 34, 56, 123000000, time.FixedZone("CST", 8*60*60))
	for _, tc := range []struct {
		name string
		file string
		line int
	}{
		{"normal", "handler/request.go", 42},
		{"zero line", "main.go", 0},
		{"special path", "目录/引号\"\\换行\n\u2028.go", 10086},
		{"invalid UTF8", "bad\xff.go", 9},
		{"unknown", "unknown", -1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := tc.file
			if tc.line >= 0 {
				caller += ":" + strconv.Itoa(tc.line)
			}
			for _, enc := range []struct {
				name string
				base Encoder
				fast callerPrefixEncoder
			}{
				{"text", DefaultTextEncoder, DefaultTextEncoder},
				{"json", DefaultJSONEncoder, DefaultJSONEncoder},
			} {
				got := enc.fast.appendPrefixCaller(nil, InfoLevel, now, tc.file, tc.line)
				want := enc.base.AppendPrefix(nil, InfoLevel, now, caller)
				if !bytes.Equal(got, want) {
					t.Errorf("%s prefix = %q, want %q", enc.name, got, want)
				}
			}
		})
	}
}

func TestBuiltinCallerPrefixUsesExactCallSite(t *testing.T) {
	l, buf := newTestLogger(t, OptCaller(true))
	_, _, line, _ := runtime.Caller(0)
	l.Info(context.Background(), "call site")
	want := fmt.Sprintf("logit/caller_prefix_test.go:%d", line+1)
	if !strings.Contains(buf.String(), want) {
		t.Errorf("caller = %q, want %q", buf.String(), want)
	}
}

type legacyCallerEncoder struct {
	caller string
}

func (e *legacyCallerEncoder) AppendPrefix(buf []byte, level Level, now time.Time, caller string) []byte {
	e.caller = caller
	return DefaultTextEncoder.AppendPrefix(buf, level, now, caller)
}

func (*legacyCallerEncoder) AppendField(buf []byte, f Field) []byte {
	return DefaultTextEncoder.AppendField(buf, f)
}

func (*legacyCallerEncoder) AppendMessage(buf []byte, msg string) []byte {
	return DefaultTextEncoder.AppendMessage(buf, msg)
}

func (*legacyCallerEncoder) Finish(buf []byte) []byte { return DefaultTextEncoder.Finish(buf) }

func TestCustomEncoderReceivesCallerString(t *testing.T) {
	enc := &legacyCallerEncoder{}
	l, _ := newTestLogger(t, OptEncoder(enc), OptCaller(true))
	l.Info(context.Background(), "custom encoder")
	if !strings.Contains(enc.caller, "logit/caller_prefix_test.go:") {
		t.Errorf("custom encoder caller = %q", enc.caller)
	}
}

type embeddedCallerEncoder struct {
	TextEncoder
	caller string
}

func (e *embeddedCallerEncoder) AppendPrefix(buf []byte, level Level, now time.Time, caller string) []byte {
	e.caller = caller
	return e.TextEncoder.AppendPrefix(buf, level, now, caller)
}

func TestEmbeddedCustomEncoderReceivesCallerString(t *testing.T) {
	enc := &embeddedCallerEncoder{}
	l, _ := newTestLogger(t, OptEncoder(enc), OptCaller(true))
	l.Info(context.Background(), "embedded custom encoder")
	if !strings.Contains(enc.caller, "logit/caller_prefix_test.go:") {
		t.Errorf("embedded custom encoder caller = %q", enc.caller)
	}
}
