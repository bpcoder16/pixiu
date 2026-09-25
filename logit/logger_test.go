package logit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/rotatefile"
)

// newTestLogger 返回写进 buf 的 logger 与 buf。
func newTestLogger(t *testing.T, opts ...Option) (Logger, *bytes.Buffer) {
	t.Helper()
	buf := &bytes.Buffer{}
	base := []Option{OptWriter(NewWriter(buf))}
	l, err := New(append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l, buf
}

func TestLoggerLineFormat(t *testing.T) {
	l, buf := newTestLogger(t)
	ctx := newTestContextWithLogID()

	l.Info(ctx, "user login", Int("uid", 42), Str("op", "login"))

	line := buf.String()
	if !strings.HasPrefix(line, "INFO: ") {
		t.Errorf("prefix missing: %q", line)
	}
	if !strings.Contains(line, "logId=[") {
		t.Errorf("logId from meta missing: %q", line)
	}
	if !strings.Contains(line, "uid=[42]") || !strings.Contains(line, "op=[login]") {
		t.Errorf("fields missing: %q", line)
	}
	if !strings.HasSuffix(line, "msg=[user login]\n") {
		t.Errorf("message suffix wrong: %q", line)
	}
}

func TestLoggerLevelFilter(t *testing.T) {
	l, buf := newTestLogger(t, OptMinLevel(WarnLevel))
	ctx := WithContext(context.Background())

	l.Debug(ctx, "d")
	l.Info(ctx, "i")
	l.Warn(ctx, "w")
	l.Error(ctx, "e")

	out := buf.String()
	if strings.Contains(out, "DEBUG") || strings.Contains(out, "INFO") {
		t.Errorf("filtered levels leaked: %q", out)
	}
	if !strings.Contains(out, "WARN") || !strings.Contains(out, "ERROR") {
		t.Errorf("warn/error missing: %q", out)
	}
	if l.Enabled(DebugLevel) || l.Enabled(InfoLevel) {
		t.Error("Enabled should respect minLevel")
	}
	if !l.Enabled(FatalLevel) {
		t.Error("fatal should be enabled")
	}
}

func TestLoggerFieldOrderAndDuplicateKeys(t *testing.T) {
	l, buf := newTestLogger(t)
	l = l.With(Str("uid", "with"))
	ctx := WithContext(context.Background())
	AddField(ctx, Str("uid", "ctx"), Str("stage", "ctx"), Str("uid", "ctx2"))
	AddMeta(ctx, Str("uid", "meta"), Str("trace", "meta"))

	l.Info(ctx, "m", Str("uid", "call"), Str("extra", "1"))

	line := buf.String()
	remaining := line
	for _, want := range []string{
		"uid=[with]", "uid=[meta]", "trace=[meta]", "uid=[ctx]",
		"stage=[ctx]", "uid=[ctx2]", "uid=[call]", "extra=[1]",
	} {
		at := strings.Index(remaining, want)
		if at < 0 {
			t.Fatalf("field %q missing or out of order: %q", want, line)
		}
		remaining = remaining[at+len(want):]
	}
	if got := strings.Count(line, "uid=["); got != 5 {
		t.Fatalf("uid count = %d, want 5: %q", got, line)
	}
}

func TestReservedFieldsRejectedByLogger(t *testing.T) {
	for _, key := range []string{levelKey, tsKey, callerKey, msgKey} {
		t.Run(key, func(t *testing.T) {
			l, buf := newTestLogger(t, OptEncoder(DefaultJSONEncoder))
			ctx := WithContext(context.Background())

			expectReservedFieldPanic(t, func() { l.With(Str(key, "wrong")) })
			expectReservedFieldPanic(t, func() {
				l.Info(ctx, "rejected", Str("safe", "value"), Str(key, "wrong"))
			})
			if buf.Len() != 0 {
				t.Errorf("保留字段不应产生部分日志: %q", buf.String())
			}
			disabled, _ := newTestLogger(t, OptMinLevel(WarnLevel))
			expectReservedFieldPanic(t, func() {
				disabled.Info(ctx, "disabled", Str(key, "wrong"))
			})
		})
	}
}

func TestLoggerAcceptsLongUnicodeFieldKeys(t *testing.T) {
	for _, key := range []string{strings.Repeat("a", 33), strings.Repeat("中", 33), strings.Repeat("🌟", 33), strings.Repeat("e\u0301", 17), strings.Repeat("x", 1024)} {
		l, buf := newTestLogger(t)
		l.With(Str(key, "with")).Info(context.Background(), "long key", Str(key, "call"))
		line := buf.String()
		first, last := strings.Index(line, key+"=[with]"), strings.Index(line, key+"=[call]")
		if strings.Count(line, key+"=[") != 2 || first < 0 || last < first {
			t.Fatalf("长字段名重复输出顺序错误: %q", line)
		}
		disabled, _ := newTestLogger(t, OptMinLevel(WarnLevel))
		disabled.Info(context.Background(), "disabled", Str(key, "accepted"))
	}
}

func TestLoggerWithPredefinedFields(t *testing.T) {
	l, buf := newTestLogger(t)
	svc := l.With(Str("mod", "OrderService"))
	ctx := WithContext(context.Background())

	svc.Info(ctx, "created", Int64("id", 7))
	l.Info(ctx, "plain")

	out := buf.String()
	if !strings.Contains(out, "mod=[OrderService]") {
		t.Errorf("With field missing: %q", out)
	}
	plainLine := strings.Split(strings.TrimSpace(out), "\n")[1]
	if strings.Contains(plainLine, "mod=[") {
		t.Errorf("With field leaked into base logger: %q", plainLine)
	}
}

func TestLoggerFieldVisibilityInLine(t *testing.T) {
	l, buf := newTestLogger(t)
	ctx := WithContext(context.Background())
	AddDebugField(ctx, Str("debugPayload", "x"))

	l.Info(ctx, "info line")
	l.Debug(ctx, "debug line")

	out := buf.String()
	infoLine := strings.Split(out, "\n")[0]
	if strings.Contains(infoLine, "debugPayload") {
		t.Errorf("debug-only field leaked into info line: %q", infoLine)
	}
	if !strings.Contains(out, "debugPayload") {
		t.Errorf("debug field should appear in debug line: %q", out)
	}
}

func TestLoggerFilterKeys(t *testing.T) {
	l, buf := newTestLogger(t, OptFilterKeys("password"))
	ctx := WithContext(context.Background())

	l.Info(ctx, "login", Str("password", "secret"), Str("user", "u"))

	line := buf.String()
	if strings.Contains(line, "secret") {
		t.Errorf("filtered value leaked: %q", line)
	}
	if !strings.Contains(line, "password=[***]") {
		t.Errorf("masked value missing: %q", line)
	}
	if !strings.Contains(line, "user=[u]") {
		t.Errorf("normal field missing: %q", line)
	}
}

func TestLoggerFilterKeysWithDuplicateFields(t *testing.T) {
	l, buf := newTestLogger(t, OptFilterKeys("password"))
	ctx := WithContext(context.Background())
	AddField(ctx, Str("password", "ctx-secret"))
	called := false

	l.Info(ctx, "login", Defer("password", func() Field {
		called = true
		return Str("password", "defer-secret")
	}), Str("password", "call-secret"))

	line := buf.String()
	if called || strings.Contains(line, "secret") {
		t.Fatalf("filtered duplicate field was evaluated or leaked: %q", line)
	}
	if got := strings.Count(line, "password=[***]"); got != 3 {
		t.Fatalf("masked duplicate count = %d, want 3: %q", got, line)
	}
}

func TestLoggerDeferLazyAndFiltered(t *testing.T) {
	l, buf := newTestLogger(t, OptMinLevel(InfoLevel))
	ctx := WithContext(context.Background())

	called := false
	l.Debug(ctx, "dropped", Defer("x", func() Field {
		called = true
		return Int("x", 1)
	})) // 级别未通过,Defer 不应求值
	if called {
		t.Error("Defer evaluated on disabled level")
	}

	l.Info(ctx, "kept", Defer("y", func() Field {
		return Str("inner", "v")
	}))
	line := buf.String()
	if !strings.Contains(line, "y=[v]") {
		t.Errorf("Defer resolved with original key: %q", line)
	}
}

func TestLoggerDeferredContextFieldCanMutateContext(t *testing.T) {
	l, buf := newTestLogger(t)
	ctx := WithContext(context.Background())
	AddField(ctx, Defer("dynamic", func() Field {
		AddField(ctx, Str("added", "from-defer"))
		return Str("ignored", "resolved")
	}))

	done := make(chan struct{})
	go func() {
		l.Info(ctx, "defer mutates context")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Defer callback ran while context store read lock was held")
	}
	if !strings.Contains(buf.String(), "dynamic=[resolved]") {
		t.Fatalf("resolved deferred field missing: %q", buf.String())
	}
	if !slices.Contains(collectCtxFieldValues(ctx, InfoLevel), "added=from-defer") {
		t.Fatal("context mutation from Defer was lost")
	}
}

func TestLoggerResolvesEveryDeferredField(t *testing.T) {
	l, buf := newTestLogger(t)
	ctx := WithContext(context.Background())
	called := false
	AddField(ctx, Defer("same", func() Field {
		called = true
		return Str("same", "old")
	}))

	l.Info(ctx, "override", Str("same", "new"))
	if !called {
		t.Fatal("earlier deferred field should be evaluated")
	}
	line := buf.String()
	if old, next := strings.Index(line, "same=[old]"), strings.Index(line, "same=[new]"); old < 0 || next < old {
		t.Fatalf("deferred and call fields should both appear in order: %q", line)
	}
}

func TestLoggerDispatch(t *testing.T) {
	all := &bytes.Buffer{}
	wf := &bytes.Buffer{}
	l, err := New(
		OptDispatch(
			Target{Levels: []Level{DebugLevel, InfoLevel}, Writer: NewWriter(all)},
			Target{Levels: []Level{WarnLevel, ErrorLevel, FatalLevel}, Writer: NewWriter(wf)},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithContext(context.Background())

	l.Info(ctx, "ok")
	l.Error(ctx, "bad")

	if !strings.Contains(all.String(), "ok") || strings.Contains(all.String(), "bad") {
		t.Errorf("main file wrong: %q", all.String())
	}
	if !strings.Contains(wf.String(), "bad") || strings.Contains(wf.String(), "ok") {
		t.Errorf("wf file wrong: %q", wf.String())
	}
}

func TestLoggerCaller(t *testing.T) {
	l, buf := newTestLogger(t, OptCaller(true))
	ctx := WithContext(context.Background())
	l.Info(ctx, "with caller")

	if !strings.Contains(buf.String(), "logit/logger_test.go") {
		t.Errorf("caller missing: %q", buf.String())
	}
	if strings.Contains(buf.String(), moduleRoot) {
		t.Errorf("caller should be relative to module root: %q", buf.String())
	}
}

func TestFatalWritesThenExits(t *testing.T) {
	l, buf := newTestLogger(t)
	cl := l.(*coreLogger)
	code := -1
	cl.exit = func(c int) { code = c }

	ctx := WithContext(context.Background())
	l.Fatal(ctx, "fatal!", Int("code", 3))

	if !strings.Contains(buf.String(), "FATAL") || !strings.Contains(buf.String(), "fatal!") {
		t.Errorf("fatal line wrong: %q", buf.String())
	}
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

type fatalSyncWriter struct {
	buf       bytes.Buffer
	syncCount int
	syncErr   error
	key       WriterKey
}

func (w *fatalSyncWriter) Write(p []byte) (int, error) { return w.buf.Write(p) }
func (w *fatalSyncWriter) Close() error                { return nil }
func (w *fatalSyncWriter) WriterKey() WriterKey        { return w.key }
func (w *fatalSyncWriter) Sync() error {
	w.syncCount++
	return w.syncErr
}

func TestFatalSyncsBeforeExit(t *testing.T) {
	w := &fatalSyncWriter{key: NewWriterKey()}
	l := MustNew(OptWriter(w)).(*coreLogger)
	l.exit = func(code int) {
		if code != 1 || w.syncCount != 1 || !strings.Contains(w.buf.String(), "fatal!") {
			t.Errorf("exit before durable write: code=%d syncs=%d line=%q", code, w.syncCount, w.buf.String())
		}
	}
	l.Fatal(context.Background(), "fatal!")
}

func TestFatalSyncErrorIsObserved(t *testing.T) {
	want := errors.New("sync failed")
	w := &fatalSyncWriter{syncErr: want, key: NewWriterKey()}
	var callbackErr error
	l := MustNew(OptWriter(w), OptNoExit(), OptOnWriteError(func(err error) {
		callbackErr = err
	})).(*coreLogger)
	l.Fatal(WithContext(context.Background()), "fatal!")
	if w.syncCount != 1 || l.WriteErrors() != 1 || !errors.Is(l.LastWriteError(), want) {
		t.Fatalf("sync failure not tracked: syncs=%d errors=%d last=%v", w.syncCount, l.WriteErrors(), l.LastWriteError())
	}
	if !errors.Is(callbackErr, want) {
		t.Fatalf("sync failure callback = %v, want %v", callbackErr, want)
	}
}

func TestOnWriteErrorSerializesAcrossWithLoggers(t *testing.T) {
	const calls = 64
	want := errors.New("write failed")
	closes := 0
	w := &countedStatsWriter{key: NewWriterKey(), writeErr: want, closes: &closes}
	var active atomic.Int32
	delivered := 0 // 回调内的普通变量应可安全累加。
	var overlapped atomic.Bool
	l := MustNew(OptWriter(w), OptOnWriteError(func(err error) {
		if !errors.Is(err, want) {
			t.Errorf("callback error = %v, want %v", err, want)
		}
		if active.Add(1) != 1 {
			overlapped.Store(true)
		}
		// 延长回调执行时间，使并发写入时的重叠可稳定观察。
		time.Sleep(time.Millisecond)
		delivered++
		active.Add(-1)
	}))
	child := l.With(Str("mod", "child"))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			if i%2 == 0 {
				l.Info(context.Background(), "root")
			} else {
				child.Info(context.Background(), "child")
			}
		}(i)
	}
	close(start)
	wg.Wait()
	if overlapped.Load() {
		t.Error("write error callbacks overlapped")
	}
	if got := delivered; got != calls {
		t.Errorf("callback calls = %d, want %d", got, calls)
	}
	if got := l.(WriteErrorStats).WriteErrors(); got != calls {
		t.Errorf("write errors = %d, want %d", got, calls)
	}
	if err := Close(l); err != nil {
		t.Fatal(err)
	}
}

func TestFatalExitsWithoutFatalTarget(t *testing.T) {
	buf := &bytes.Buffer{}
	l := MustNew(OptDispatch(Target{
		Levels: []Level{InfoLevel},
		Writer: NewWriter(buf),
	}))
	cl := l.(*coreLogger)
	code := -1
	cl.exit = func(c int) { code = c }

	l.Fatal(context.Background(), "fatal without route")

	if code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if buf.Len() != 0 {
		t.Fatalf("unrouted Fatal unexpectedly wrote %q", buf.String())
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := New(); err == nil {
		t.Error("no target should fail")
	}
	if _, err := New(OptDispatch(Target{Levels: []Level{InfoLevel}})); err == nil {
		t.Error("nil writer should fail")
	}
	if _, err := New(OptDispatch(Target{Writer: NewWriter(&bytes.Buffer{})})); err == nil {
		t.Error("empty levels should fail")
	}
	if _, err := New(OptDispatch(Target{Levels: []Level{AllLevels}, Writer: NewWriter(&bytes.Buffer{})})); err == nil {
		t.Error("multi-bit level should fail")
	}
	if _, err := New(OptMinLevel(UnknownLevel), OptWriter(NewWriter(&bytes.Buffer{}))); err == nil {
		t.Error("unknown minimum level should fail")
	}
	if _, err := New(OptMinLevel(AllLevels), OptWriter(NewWriter(&bytes.Buffer{}))); err == nil {
		t.Error("multi-bit minimum level should fail")
	}
}

func TestOptEncoderPanicsOnNil(t *testing.T) {
	var typedNil *JSONEncoder
	for _, enc := range []Encoder{nil, typedNil} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("OptEncoder(%T) 未在调用时 panic", enc)
				}
			}()
			_ = OptEncoder(enc)
		}()
	}
}

func TestRuntimeMinLevelSharedWithChild(t *testing.T) {
	l, _ := newTestLogger(t)
	child := l.With(Str("mod", "child"))

	if err := SetMinLevel(l, ErrorLevel); err != nil {
		t.Fatalf("SetMinLevel: %v", err)
	}
	if l.Enabled(InfoLevel) || child.Enabled(WarnLevel) {
		t.Fatal("runtime minimum level was not applied to parent and child")
	}
	if !l.Enabled(ErrorLevel) || !child.Enabled(FatalLevel) {
		t.Fatal("enabled levels above runtime minimum were lost")
	}
	controller := l.(LevelController)
	if got := controller.MinLevel(); got != ErrorLevel {
		t.Fatalf("MinLevel = %v, want ERROR", got)
	}
	if err := controller.SetMinLevel(AllLevels); err == nil {
		t.Fatal("invalid runtime minimum level must fail")
	}
	if got := controller.MinLevel(); got != ErrorLevel {
		t.Fatalf("invalid update changed MinLevel to %v", got)
	}
}

type uncomparableWriter struct {
	state  []byte
	closes *int
	key    WriterKey
}

func (uncomparableWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w uncomparableWriter) WriterKey() WriterKey      { return w.key }
func (w uncomparableWriter) Close() error {
	*w.closes++
	return nil
}

func TestCloseAcceptsUncomparableWriter(t *testing.T) {
	closes := 0
	w := uncomparableWriter{state: []byte("state"), closes: &closes, key: NewWriterKey()}
	l := MustNew(OptDispatch(
		Target{Levels: []Level{InfoLevel}, Writer: w},
		Target{Levels: []Level{ErrorLevel}, Writer: w},
	))
	if err := Close(l); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if closes != 1 {
		t.Fatalf("close calls = %d, want 1", closes)
	}
}

type interfaceFieldWriter struct {
	state  any
	closes *int
	key    WriterKey
}

func (interfaceFieldWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w interfaceFieldWriter) WriterKey() WriterKey      { return w.key }
func (w interfaceFieldWriter) Close() error {
	*w.closes++
	return nil
}

func TestCloseAcceptsComparableTypeWithUncomparableValue(t *testing.T) {
	closes := 0
	w := interfaceFieldWriter{state: []byte("state"), closes: &closes, key: NewWriterKey()}
	l := MustNew(OptWriter(w))
	if err := Close(l); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if closes != 1 {
		t.Fatalf("close calls = %d, want 1", closes)
	}
}

type customCloseLogger struct {
	Logger
	closeErr error
	closed   bool
}

func (l *customCloseLogger) Close() error {
	l.closed = true
	return l.closeErr
}

func TestCloseCustomLogger(t *testing.T) {
	base := MustNew(OptWriter(NewWriter(&bytes.Buffer{})))
	wantErr := errors.New("custom close failed")
	custom := &customCloseLogger{Logger: base, closeErr: wantErr}
	if err := Close(custom); !errors.Is(err, wantErr) || !custom.closed {
		t.Fatalf("Close(custom) = %v, called = %v", err, custom.closed)
	}
	withoutClose := &struct{ Logger }{Logger: base}
	if err := Close(withoutClose); !errors.Is(err, ErrLoggerCloseUnsupported) {
		t.Fatalf("Close 对不支持关闭的自定义 Logger 返回 %v", err)
	}
	if err := Close(nil); !errors.Is(err, ErrLoggerCloseUnsupported) {
		t.Fatalf("Close(nil) 返回 %v", err)
	}
}

func TestNewRejectsZeroWriterKey(t *testing.T) {
	w := uncomparableWriter{state: []byte("state"), closes: new(int)}
	if _, err := New(OptWriter(w)); err == nil {
		t.Fatal("New accepted Writer with zero WriterKey")
	}
}

func TestNewRejectsTypedNilWriter(t *testing.T) {
	var w *fatalSyncWriter
	if _, err := New(OptWriter(w)); err == nil {
		t.Fatal("New accepted typed nil Writer")
	}
}

func TestNewWriterKeysAreStableAndDistinct(t *testing.T) {
	first := NewWriter(&bytes.Buffer{})
	second := NewWriter(&bytes.Buffer{})
	if first.WriterKey() == (WriterKey{}) || second.WriterKey() == (WriterKey{}) {
		t.Fatal("NewWriter returned zero key")
	}
	if first.WriterKey() == second.WriterKey() {
		t.Fatal("different Writers share one key")
	}
}

type countedStatsWriter struct {
	key      WriterKey
	err      error
	writeErr error
	closes   *int
}

func (w *countedStatsWriter) Write(p []byte) (int, error) { return len(p), w.writeErr }
func (w *countedStatsWriter) WriterKey() WriterKey        { return w.key }
func (w countedStatsWriter) Close() error {
	*w.closes++
	return nil
}
func (countedStatsWriter) WriteErrors() int64 { return 3 }
func (w countedStatsWriter) LastWriteError() error {
	return w.err
}

func TestLoggerIgnoresSharedWriterStats(t *testing.T) {
	closes := 0
	writerErr := errors.New("writer's prior error")
	writeErr := errors.New("current write failed")
	w := &countedStatsWriter{key: NewWriterKey(), err: writerErr, closes: &closes}
	l := MustNew(OptDispatch(
		Target{Levels: []Level{InfoLevel}, Writer: w},
		Target{Levels: []Level{ErrorLevel}, Writer: w},
	))
	stats := l.(WriteErrorStats)
	l.Info(context.Background(), "successful write")
	if got := stats.WriteErrors(); got != 0 || stats.LastWriteError() != nil {
		t.Fatalf("Writer's own stats leaked into Logger: count=%d, last=%v", got, stats.LastWriteError())
	}
	w.writeErr = writeErr
	l.Info(context.Background(), "failed info")
	l.Error(context.Background(), "failed error")
	if got := stats.WriteErrors(); got != 2 {
		t.Fatalf("WriteErrors = %d, want 2", got)
	}
	if got := stats.LastWriteError(); !errors.Is(got, writeErr) {
		t.Fatalf("LastWriteError = %v, want %v", got, writeErr)
	}
	if err := Close(l); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if closes != 1 {
		t.Fatalf("close calls = %d, want 1", closes)
	}
}

func TestCloseSharedRotateWriterOnce(t *testing.T) {
	f, err := rotatefile.New(filepath.Join(t.TempDir(), "app.log"))
	if err != nil {
		t.Fatalf("rotatefile.New: %v", err)
	}
	w := NewWriter(f)
	t.Cleanup(func() { _ = w.Close() })
	l := MustNew(OptDispatch(
		Target{Levels: []Level{InfoLevel}, Writer: w},
		Target{Levels: []Level{ErrorLevel}, Writer: w},
	))
	if err := Close(l); err != nil {
		t.Fatalf("shared rotate Writer was closed more than once: %v", err)
	}
}

func TestOptFileNameOverriddenDoesNotOpenFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unused.log")
	buf := &bytes.Buffer{}
	l, err := New(OptFileName(path), OptWriter(NewWriter(buf)))
	if err != nil {
		t.Fatal(err)
	}
	l.Info(context.Background(), "final writer")
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("overridden OptFileName should not create a file, stat err = %v", err)
	}
}

func TestLoggerManyFieldsPreserveDuplicateKeys(t *testing.T) {
	l, buf := newTestLogger(t)
	const count = 130
	fields := make([]Field, 0, count+1)
	for i := 0; i < count; i++ {
		fields = append(fields, Int(fmt.Sprintf("k%03d", i), i))
	}
	key := fmt.Sprintf("k%03d", count-1)
	fields = append(fields, Int(key, 999))

	l.Info(context.Background(), "many fields", fields...)
	line := buf.String()
	if got := strings.Count(line, key+"=["); got != 2 {
		t.Fatalf("duplicate key count = %d, want 2: %q", got, line)
	}
	if first, last := strings.Index(line, key+"=[129]"), strings.Index(line, key+"=[999]"); first < 0 || last < first {
		t.Fatalf("duplicate key order is wrong: %q", line)
	}
}

func TestLoggerLongPrefixKeysPreserveDuplicates(t *testing.T) {
	for _, prefix := range []string{strings.Repeat("shared-prefix-", 3), strings.Repeat("shared", 170)} {
		l, buf := newTestLogger(t)
		const count = 32
		fields := make([]Field, 0, count+1)
		for i := 0; i < count; i++ {
			fields = append(fields, Int(fmt.Sprintf("%sk%02d", prefix, i), i))
		}
		fields = append(fields, Int(prefix+"k00", 999))

		l.Info(context.Background(), "same prefix", fields...)
		line := buf.String()
		for i := 0; i < count; i++ {
			key := fmt.Sprintf("%sk%02d", prefix, i)
			want := 1
			if i == 0 {
				want = 2
			}
			if got := strings.Count(line, key+"=["); got != want {
				t.Fatalf("key %q count = %d, want %d: %q", key, got, want, line)
			}
		}
		if !strings.Contains(line, prefix+"k00=[0]") || !strings.Contains(line, prefix+"k00=[999]") {
			t.Fatalf("duplicate key values missing: %q", line)
		}
		if strings.Index(line, prefix+"k00=[999]") < strings.Index(line, prefix+"k31=[31]") {
			t.Fatalf("duplicate key should remain at the end: %q", line)
		}
	}
}

func TestLoggerSimilarLongKeysRemainDistinct(t *testing.T) {
	l, buf := newTestLogger(t)
	base := strings.Repeat("x", 1024)
	first := base[:100] + "a" + base[101:]
	second := base[:100] + "b" + base[101:]
	l.Info(context.Background(), "long keys", Int(first, 1), Int(second, 2), Int(first, 3))
	line := buf.String()
	if strings.Count(line, first+"=[") != 2 || !strings.Contains(line, first+"=[1]") || !strings.Contains(line, first+"=[3]") {
		t.Fatalf("第一个长字段名的重复输出错误: %q", line)
	}
	if strings.Count(line, second+"=[") != 1 || !strings.Contains(line, second+"=[2]") {
		t.Fatalf("相似长字段名未正确输出: %q", line)
	}
}

func TestOpenFileWrites(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "app.log")
	l := MustNew(OptFileName(path))
	ctx := WithContext(context.Background())
	l.Info(ctx, "file log")
	if err := Close(l); err != nil {
		t.Fatalf("Close: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "file log") {
		t.Errorf("file content: %q", data)
	}
}

func TestOutputCallDepth(t *testing.T) {
	l, buf := newTestLogger(t, OptCaller(true))
	ctx := WithContext(context.Background())

	wrap := func() {
		l.Output(ctx, InfoLevel, 1, "wrapped") // 0 = wrap 自己
	}
	wrap()
	if !strings.Contains(buf.String(), "logger_test.go") {
		t.Errorf("callDepth resolution wrong: %q", buf.String())
	}
}

// lockedBuffer 是并发安全的 bytes.Buffer,用于并发测试。
// (生产 writer 契约:单次 Write 并发安全,如 O_APPEND 文件;bytes.Buffer 不满足。)
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestLoggerConcurrent(t *testing.T) {
	buf := &lockedBuffer{}
	l := MustNew(OptWriter(NewWriter(buf)))
	ctx := newTestContextWithLogID()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.Info(ctx, "concurrent", Int("g", n), Int("j", j))
			}
		}(i)
	}
	wg.Wait()
	if got := strings.Count(buf.String(), "\n"); got != 800 {
		t.Errorf("lines = %d, want 800", got)
	}
}

func TestOptFileNameError(t *testing.T) {
	// 无权限路径 → New 应返回错误,而不是构造成功后静默丢日志
	if _, err := New(OptFileName("/proc/definitely-not-writable/log.txt")); err == nil {
		t.Error("unwritable path should fail New")
	}
}

// TestRequestResponseLargeBodies 覆盖典型的业务访问日志形态:
// 11 个 key + KB 级 reqBody/response 全量记录。
func TestRequestResponseLargeBodies(t *testing.T) {
	reqBody := strings.Repeat(`{"orderId":10086,"items":[{"skuId":1,"cnt":2,"price":"12.50"},{"skuId":7,"cnt":1,"price":"99.00"}],"address":{"province":"GD","city":"SZ","detail":"tech-park-south-5-801"},"remark":"weekend-delivery","couponIds":[1001,1002]}`, 3)
	respBody := strings.Repeat(`{"code":0,"msg":"ok","data":{"status":"PAID","payTime":"2026-09-15 10:00:00","amount":"124.00","discounts":[{"type":"coupon","amount":"10.00"}],"traceNo":"20260915100012345678"}}`, 4)

	buf := &bytes.Buffer{}
	l := MustNew(OptEncoder(DefaultJSONEncoder), OptWriter(NewWriter(buf)), OptFilterKeys("password"))
	ctx := newTestContextWithLogID()
	AddField(ctx, Str("userId", "u_10086"))

	l.Info(ctx, "http access",
		Dur("costTime", 15*time.Millisecond),
		Str("clientIP", "10.20.30.40"),
		Str("method", "POST"),
		Str("uri", "/api/v1/app/order/create"),
		Str("password", "secret"),
		Str("reqBody", reqBody),
		Str("response", respBody),
		Int("statusCode", 200),
	)

	line := buf.String()
	if !json.Valid([]byte(line)) {
		t.Fatalf("invalid JSON for large bodies: %d bytes", len(line))
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatal(err)
	}
	if m["reqBody"] != reqBody || m["response"] != respBody {
		t.Errorf("large bodies corrupted: req=%d resp=%d bytes", len(m["reqBody"].(string)), len(m["response"].(string)))
	}
	if m["password"] != "***" {
		t.Errorf("mask failed: %v", m["password"])
	}
	if m["statusCode"] != float64(200) || m["userId"] != "u_10086" || m["logId"] == nil {
		t.Errorf("fields wrong: %v", m)
	}
}

// TestLargeLineSynchronousWrite 验证 KB 级行在同步 Writer 中不撕裂。
func TestLargeLineSynchronousWrite(t *testing.T) {
	big := strings.Repeat(`{"k":"中文备注与payload混合内容abcdefg"}`, 80) // ~3KB
	buf := &lockedBuffer{}
	l := MustNew(OptWriter(NewWriter(buf)))
	ctx := WithContext(context.Background())

	for i := 0; i < 5; i++ {
		l.Info(ctx, "big line", Int("i", i), Str("body", big))
	}
	if err := Close(l); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if got := strings.Count(out, "\n"); got != 5 {
		t.Errorf("lines = %d, want 5", got)
	}
	for i := 0; i < 5; i++ {
		marker := fmt.Sprintf("i=[%d]", i)
		if !strings.Contains(out, marker) {
			t.Errorf("line %d missing", i)
		}
	}
	if c := strings.Count(out, big); c != 5 {
		t.Errorf("big body occurrences = %d, want 5 (torn or lost)", c)
	}
}
