package logit

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

// jsonRefString 用标准库(SetEscapeHTML(false))生成参考转义,验证逐字节一致。
func jsonRefString(t *testing.T, s string) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(s); err != nil {
		t.Fatalf("reference encode: %v", err)
	}
	return strings.TrimSuffix(buf.String(), "\n")
}

func checkEscaping(t *testing.T, s string) {
	t.Helper()
	got := string(appendJSONString(nil, s))
	want := jsonRefString(t, s)
	if got != want {
		t.Errorf("appendJSONString(%q) =\n %s\nwant\n %s", s, got, want)
	}
}

func TestJSONEscapingMatchesStdlib(t *testing.T) {
	corpus := []string{
		"",
		"plain",
		"中文与 emoji \U0001F600",
		`quote"inside`,
		`back\slash`,
		"tab\tnewline\ncr\r",
		"<html>&amp;</html>", // 不转义 HTML,与 SetEscapeHTML(false) 一致
		"linesep",            // U+2028/U+2029
		"mixed 中文 \"引号\" \\\t",
		strings.Repeat("long-clean-string-", 100),
		strings.Repeat("带\"引号\"的中文", 50),
	}
	// 全量控制字符
	for b := 0; b < 0x20; b++ {
		corpus = append(corpus, string([]byte{'a', byte(b), 'b'}))
	}
	// 畸形 UTF-8:孤立续字节、截断多字节序列
	corpus = append(corpus,
		"\xff",
		"a\xffb",
		"\xc3\x28",
		"\xe2\x82",
		"\xf0\x9f\x98",
		"\xed\xa0\x80", // 代理区,标准库替换为 U+FFFD
	)
	for _, s := range corpus {
		checkEscaping(t, s)
	}
}

func TestJSONEncoderFullLine(t *testing.T) {
	buf := &bytes.Buffer{}
	l := MustNew(OptEncoder(DefaultJSONEncoder), OptWriter(NewWriter(buf)))
	ctx := NewTraceContext(context.Background())

	l.Info(ctx, "user login", Int("uid", 42), Str("op", "login"))

	line := buf.String()
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON line: %q", line)
	}
	if !strings.HasPrefix(line, `{"level":"INFO","ts":"`) {
		t.Errorf("prefix wrong: %q", line[:40])
	}
	if !strings.HasSuffix(line, `,"msg":"user login"}`+"\n") {
		t.Errorf("suffix wrong: %q", line)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatal(err)
	}
	if m["level"] != "INFO" || m["uid"] != float64(42) || m["op"] != "login" || m["msg"] != "user login" {
		t.Errorf("decoded fields: %v", m)
	}
	if _, ok := m["logId"]; !ok {
		t.Error("logId from meta missing")
	}
}

func TestJSONEncoderTimestampPrefix(t *testing.T) {
	ts := time.Date(2026, 9, 15, 10, 0, 0, 123000000, time.FixedZone("CST", 8*60*60))
	got := string(DefaultJSONEncoder.AppendPrefix(nil, InfoLevel, ts, ""))
	want := `{"level":"INFO","ts":"2026-09-15T10:00:00.123+08:00",`
	if got != want {
		t.Errorf("timestamp prefix = %q, want %q", got, want)
	}
}

func TestJSONEncoderFieldValues(t *testing.T) {
	ts := time.Date(2026, 9, 15, 10, 0, 0, 123000000, time.FixedZone("CST", 8*3600))
	tests := []struct {
		desc string
		f    Field
		want string
	}{
		{"bool", Bool("k", true), `"k":true,`},
		{"bool false", Bool("k", false), `"k":false,`},
		{"int neg", Int("k", -7), `"k":-7,`},
		{"uint64 max", Uint64("k", 1<<63), `"k":9223372036854775808,`},
		{"float", Float64("k", 1.25), `"k":1.25,`},
		{"float small", Float64("k", 0.001), `"k":0.001,`},
		{"str escape", Str("k", "a\"b"), `"k":"a\"b",`},
		{"err", Err(errBoom), `"err":"boom",`},
		{"dur", Dur("k", 1500*time.Microsecond), `"k":1.500,`},
		{"dur sub-ms", Dur("k", 500*time.Microsecond), `"k":0.500,`},
		{"dur negative", Dur("k", -1500*time.Microsecond), `"k":-1.500,`},
		{"time", Time("k", ts), `"k":"2026-09-15T10:00:00.123+08:00",`},
		{"any struct", Any("k", struct{ A int }{1}), `"k":{"A":1},`},
		{"raw message", Any("k", json.RawMessage(`{"A":1}`)), `"k":{"A":1},`},
		{"invalid", Field{Key: "k"}, `"k":"<?>",`},
	}
	for _, tt := range tests {
		got := string(DefaultJSONEncoder.AppendField(nil, tt.f))
		if got != tt.want {
			t.Errorf("%s: %s, want %s", tt.desc, got, tt.want)
		}
	}
}

func TestJSONEncoderRawMessageFallbackMatchesStdlib(t *testing.T) {
	invalid := json.RawMessage(`{"broken":`)
	var nilPointer *json.RawMessage
	for _, value := range []any{invalid, json.RawMessage(nil), nilPointer} {
		encoded, err := json.Marshal(value)
		var want string
		if err != nil {
			want = `"payload":` + jsonRefString(t, err.Error()) + `,`
		} else {
			want = `"payload":` + string(encoded) + `,`
		}
		if got := string(DefaultJSONEncoder.AppendField(nil, Any("payload", value))); got != want {
			t.Fatalf("RawMessage fallback = %q, want %q", got, want)
		}
	}
}

func TestJSONRawMessageRemainsSingleLine(t *testing.T) {
	raw := json.RawMessage(`
	{
		"message": "保留 space 和转义 \n\t\"\\",
		"nested": [1,
			{"ok": true}]
	}
	`)
	original := append([]byte(nil), raw...)
	var want bytes.Buffer
	if err := json.Compact(&want, raw); err != nil {
		t.Fatal(err)
	}
	for name, value := range map[string]any{"value": raw, "pointer": &raw} {
		t.Run(name, func(t *testing.T) {
			var out bytes.Buffer
			l := MustNew(OptWriter(NewWriter(&out)), OptEncoder(DefaultJSONEncoder))
			l.Info(context.Background(), "request", Any("payload", value))
			if got := bytes.Count(out.Bytes(), []byte{'\n'}); got != 1 {
				t.Errorf("physical lines = %d, want 1: %q", got, out.String())
			}
			var row struct{ Payload json.RawMessage }
			if err := json.Unmarshal(out.Bytes(), &row); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(row.Payload, want.Bytes()) {
				t.Errorf("payload = %s, want %s", row.Payload, want.Bytes())
			}
			if !bytes.Equal(raw, original) {
				t.Fatal("input RawMessage was modified")
			}
		})
	}
}

func TestJSONEncoderSpecialFloatsRemainValidJSON(t *testing.T) {
	tests := []struct {
		name string
		v    float64
		want string
	}{
		{name: "nan", v: math.NaN(), want: `"NaN"`},
		{name: "positive infinity", v: math.Inf(1), want: `"+Inf"`},
		{name: "negative infinity", v: math.Inf(-1), want: `"-Inf"`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := string(DefaultJSONEncoder.AppendField(nil, Float64("k", tt.v)))
			if got != `"k":`+tt.want+`,` {
				t.Fatalf("special float = %q, want %q", got, `"k":`+tt.want+`,`)
			}
			line := []byte(`{` + strings.TrimSuffix(got, ",") + `}`)
			if !json.Valid(line) {
				t.Fatalf("special float produced invalid JSON: %q", line)
			}
		})
	}
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }

func TestJSONEncoderCallerAndFinish(t *testing.T) {
	buf := &bytes.Buffer{}
	l := MustNew(OptEncoder(DefaultJSONEncoder), OptWriter(NewWriter(buf)), OptCaller(true))
	ctx := WithContext(context.Background())

	l.Warn(ctx, "with caller")

	line := buf.String()
	if !strings.Contains(line, `"caller":"`) {
		t.Errorf("caller missing: %q", line)
	}
	if !strings.HasSuffix(line, "}\n") || strings.Contains(line, ",}") {
		t.Errorf("brace/comma handling wrong: %q", line)
	}
	if !json.Valid([]byte(line)) {
		t.Errorf("invalid JSON: %q", line)
	}
}

func TestJSONEncoderEmptyFieldsAndMessage(t *testing.T) {
	buf := &bytes.Buffer{}
	l := MustNew(OptEncoder(DefaultJSONEncoder), OptWriter(NewWriter(buf)))
	ctx := WithContext(context.Background())

	l.Info(ctx, "")

	line := buf.String()
	if !json.Valid([]byte(line)) {
		t.Errorf("invalid JSON: %q", line)
	}
	if !strings.HasSuffix(line, `"msg":""}`+"\n") {
		t.Errorf("empty msg suffix: %q", line)
	}
}

func TestJSONEncoderFieldMergeFromCtx(t *testing.T) {
	buf := &bytes.Buffer{}
	l := MustNew(OptEncoder(DefaultJSONEncoder), OptWriter(NewWriter(buf)))
	ctx := WithContext(context.Background())
	AddField(ctx, Str("uid", "42"))
	AddDebugField(ctx, Str("debugPayload", "x"))

	l.Info(ctx, "m", Str("uid", "call"))

	line := buf.String()
	if !strings.Contains(line, `"uid":"call"`) || strings.Count(line, `"uid"`) != 1 {
		t.Errorf("dedup failed: %q", line)
	}
	if strings.Contains(line, "debugPayload") {
		t.Errorf("debug-only field leaked: %q", line)
	}
	if !json.Valid([]byte(line)) {
		t.Errorf("invalid JSON: %q", line)
	}
}
