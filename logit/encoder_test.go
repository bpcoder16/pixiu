package logit

import (
	"bytes"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func encodeLine(level Level, fields []Field, msg string) string {
	enc := DefaultTextEncoder
	buf := enc.AppendPrefix(nil, level,
		time.Date(2026, 9, 14, 18, 0, 0, 0, time.FixedZone("CST", 8*60*60)),
		"log/x.go:42")
	for _, f := range fields {
		buf = enc.AppendField(buf, f)
	}
	buf = enc.AppendMessage(buf, msg)
	return string(enc.Finish(buf))
}

func TestTextEncoderBasicLine(t *testing.T) {
	got := encodeLine(InfoLevel, []Field{Int("uid", 42), Str("op", "login")}, "user login")
	want := "INFO: 2026-09-14T18:00:00.000+08:00 log/x.go:42 uid=[42] op=[login] msg=[user login]\n"
	if got != want {
		t.Errorf("line =\n%q\nwant\n%q", got, want)
	}
}

func TestTextEncoderFieldValues(t *testing.T) {
	ts := time.Date(2026, 9, 14, 18, 0, 0, 123000000, time.FixedZone("CST", 8*60*60))
	tests := []struct {
		desc string
		f    Field
		want string
	}{
		{"bool true", Bool("k", true), "k=[true] "},
		{"bool false", Bool("k", false), "k=[false] "},
		{"int neg", Int("k", -7), "k=[-7] "},
		{"uint64", Uint64("k", 7), "k=[7] "},
		{"float", Float64("k", 1.25), "k=[1.25] "},
		{"str", Str("k", "v"), "k=[v] "},
		{"err", Err(errors.New("boom")), "err=[boom] "},
		{"dur", Dur("k", 1500*time.Millisecond), "k=[1500.000] "},
		{"dur sub-ms", Dur("k", 500*time.Microsecond), "k=[0.500] "},
		{"dur negative", Dur("k", -1500*time.Millisecond), "k=[-1500.000] "},
		{"time", Time("k", ts), "k=[2026-09-14T18:00:00.123+08:00] "},
		{"any struct", Any("k", struct{ A int }{1}), "k=[{\"A\":1}] "},
	}
	for _, tt := range tests {
		got := string(DefaultTextEncoder.AppendField(nil, tt.f))
		if got != tt.want {
			t.Errorf("%s: %q, want %q", tt.desc, got, tt.want)
		}
	}
}

func TestTextEncoderNoCaller(t *testing.T) {
	buf := DefaultTextEncoder.AppendPrefix(nil, ErrorLevel,
		time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("CST", 8*60*60)), "")
	if string(buf) != "ERROR: 2026-01-02T03:04:05.000+08:00 " {
		t.Errorf("prefix without caller = %q", buf)
	}
}

func TestTextEncoderEscapesRecordBoundaries(t *testing.T) {
	got := encodeLine(InfoLevel, []Field{
		Str("text", "line1\nline2\r\nvalue]\\tail\t\x01"),
		Str("bad\nkey]", "value"),
		Err(errors.New("boom\nnext]")),
		Any("items", []string{"a]", "b\n"}),
	}, "message\nnext]\\tail")
	want := "INFO: 2026-09-14T18:00:00.000+08:00 log/x.go:42 " +
		"text=[line1\\nline2\\r\\nvalue\\]\\\\tail\\t\\x01] " +
		"bad\\nkey\\]=[value] " +
		"err=[boom\\nnext\\]] items=[[\"a\\]\",\"b\\\\n\"\\]] " +
		"msg=[message\\nnext\\]\\\\tail]\n"
	if got != want {
		t.Fatalf("escaped line =\n%q\nwant\n%q", got, want)
	}
	if bytes.Count([]byte(got), []byte{'\n'}) != 1 {
		t.Fatalf("one log call must produce one physical line: %q", got)
	}
}

func TestAppendFieldValueUnknown(t *testing.T) {
	f := Field{Key: "k", typ: invalidType}
	if got := string(appendFieldValue(nil, f)); got != "<?>" {
		t.Errorf("invalid type = %q", got)
	}
}

func TestCallerPath(t *testing.T) {
	path := CallerPath(0)
	if path == "unknown" || !bytes.Contains([]byte(path), []byte(":")) {
		t.Errorf("CallerPath(0) = %q", path)
	}
	if path == CallerPath(0) {
		// 同一行调用应稳定,不同行应不同——此处两次调用在同一行的不同位置
		t.Logf("same-line caller path: %s", path)
	}
	inner := func() string { return CallerPath(1) }
	outerPath := inner()
	if outerPath == "unknown" || len(outerPath) == 0 {
		t.Errorf("CallerPath(1) = %q", outerPath)
	}
}

func TestTrimCallerPath(t *testing.T) {
	// 本包文件一定命中 moduleRoot 前缀
	got := trimCallerPath(moduleRoot + "logit/logger.go")
	if got != "logit/logger.go" {
		t.Errorf("trim module path = %q", got)
	}
	if got := trimCallerPath("/home/u/go/pkg/mod/gin@v1/gin.go"); got != "gin@v1/gin.go" {
		t.Errorf("trim pkg/mod = %q", got)
	}
	if got := trimCallerPath("/home/u/work/order-service/handler/order.go"); got != "handler/order.go" {
		t.Errorf("trim external application path = %q", got)
	}
	if got := trimCallerPath("/order.go"); got != "order.go" {
		t.Errorf("trim root path = %q", got)
	}
}

func TestNewLogIDUUIDV4AndUnique(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := NewLogID()
		assertUUIDV4(t, id)
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate logid %s", id)
		}
		seen[id] = struct{}{}
	}
}

func TestNewLogIDConcurrentUnique(t *testing.T) {
	const (
		workers   = 16
		perWorker = 64
	)
	ids := make(chan string, workers*perWorker)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range perWorker {
				ids <- NewLogID()
			}
		}()
	}
	wg.Wait()
	close(ids)

	seen := make(map[string]struct{}, workers*perWorker)
	for id := range ids {
		assertUUIDV4(t, id)
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate concurrent logid %s", id)
		}
		seen[id] = struct{}{}
	}
}

func assertUUIDV4(t *testing.T, id string) {
	t.Helper()
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		t.Fatalf("logid %q is not a canonical UUID", id)
	}
	if id != strings.ToLower(id) {
		t.Fatalf("logid %q contains uppercase hex", id)
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(id, "-", ""))
	if err != nil {
		t.Fatalf("decode logid %q: %v", id, err)
	}
	if raw[6]&0xf0 != 0x40 {
		t.Fatalf("logid %q version = %x, want 4", id, raw[6]>>4)
	}
	if raw[8]&0xc0 != 0x80 {
		t.Fatalf("logid %q variant bits = %08b, want 10xxxxxx", id, raw[8])
	}
}
