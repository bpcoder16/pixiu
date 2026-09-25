package logit

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func capturePanicLogger(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	SetPanicLogger(NewWriter(buf))
	t.Cleanup(func() { panicLoggerPtr.Store(nil) })
	return buf
}

func TestReportPanicMultilineStack(t *testing.T) {
	buf := capturePanicLogger(t)
	ctx := newTestContextWithLogID()

	if err := ReportPanic(ctx, "boom", Str("where", "line1\nline2"), Str("stack", "user\nvalue")); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.HasPrefix(out, "FATAL:") {
		t.Errorf("should log at FATAL level (sync bypass): %q", out[:min(len(out), 40)])
	}
	if strings.Count(out, "\n") < 3 {
		t.Errorf("panic stack should contain physical newlines, got %d", strings.Count(out, "\n"))
	}
	if !strings.Contains(out, "panic=[boom]") {
		t.Errorf("panic value missing: %q", out)
	}
	if !strings.Contains(out, "stack=[\ngoroutine") {
		t.Errorf("stack should preserve physical newlines: %q", out)
	}
	if !strings.Contains(out, "pid=[") || !strings.Contains(out, "processStart=[") {
		t.Errorf("pid/processStart missing: %q", out)
	}
	if !strings.Contains(out, "logId=[") {
		t.Errorf("logId from ctx missing: %q", out)
	}
	if !strings.Contains(out, `where=[line1\nline2]`) || !strings.Contains(out, `stack=[user\nvalue]`) {
		t.Errorf("ordinary fields should keep text escaping: %q", out)
	}
}

func TestReportPanicErrorValue(t *testing.T) {
	buf := capturePanicLogger(t)
	ctx := WithContext(context.Background())

	if err := ReportPanic(ctx, errBoom); err != nil { // 复用 encoder_json_test 的 errBoom
		t.Fatal(err)
	}

	if !strings.Contains(buf.String(), "panic=[boom]") {
		t.Errorf("error value should be stringified: %q", buf.String())
	}
}

func TestReportPanicDeepStack(t *testing.T) {
	buf := &bytes.Buffer{}
	SetPanicLogger(NewWriter(buf))
	t.Cleanup(func() { panicLoggerPtr.Store(nil) })

	var report func(int)
	report = func(depth int) {
		if depth == 0 {
			if err := ReportPanic(context.Background(), "deep"); err != nil {
				t.Fatal(err)
			}
			return
		}
		report(depth - 1)
	}
	report(200)

	if !strings.Contains(buf.String(), "testing.tRunner") {
		t.Fatalf("深调用栈被截断，缺少底部的 testing.tRunner；日志长度=%d", buf.Len())
	}
}

func TestSetPanicLoggerFixedConfiguration(t *testing.T) {
	w := &fatalSyncWriter{key: NewWriterKey()}
	SetPanicLogger(w)
	t.Cleanup(func() { panicLoggerPtr.Store(nil) })
	l := PanicLogger()
	if l == nil || !l.Enabled(FatalLevel) {
		t.Fatal("panic Logger 必须路由 Fatal")
	}
	for _, level := range []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel} {
		if l.Enabled(level) {
			t.Errorf("panic Logger 不应路由 %s", level)
		}
	}
	if err := ReportPanic(context.Background(), "configured"); err != nil {
		t.Fatal(err)
	}
	if w.syncCount != 1 {
		t.Errorf("Fatal 应同步目标，次数=%d", w.syncCount)
	}
	prefix, _, _ := strings.Cut(w.buf.String(), " pid=[")
	if parts := strings.Fields(prefix); len(parts) != 2 || parts[0] != "FATAL:" {
		t.Errorf("预期无 caller 的文本前缀，得到 %q", prefix)
	}
}

func TestSetPanicLoggerRejectsInvalidWriter(t *testing.T) {
	var typedNil *fatalSyncWriter
	for _, w := range []Writer{nil, typedNil, &fatalSyncWriter{}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("SetPanicLogger(%T) 未在配置时 panic", w)
				}
			}()
			SetPanicLogger(w)
		}()
	}
}

func TestRecoverAndReport(t *testing.T) {
	buf := capturePanicLogger(t)
	ctx := WithContext(context.Background())

	func() {
		defer RecoverAndReport(ctx)
		panic("inner panic")
	}()

	// 能执行到这里说明 panic 被 recover 且进程未退出
	if !strings.Contains(buf.String(), "panic=[inner panic]") {
		t.Errorf("RecoverAndReport did not log: %q", buf.String())
	}
}

func TestRecoverAndReportNoPanic(t *testing.T) {
	buf := capturePanicLogger(t)
	ctx := WithContext(context.Background())

	func() {
		defer RecoverAndReport(ctx)
	}()
	if buf.Len() != 0 {
		t.Errorf("no panic should not log: %q", buf.String())
	}
}

func TestReportPanicWithoutLogger(t *testing.T) {
	previous := panicLoggerPtr.Swap(nil)
	t.Cleanup(func() { panicLoggerPtr.Store(previous) })

	if got := PanicLogger(); got != nil {
		t.Fatal("未配置时不应创建默认 panic Logger")
	}
	if err := ReportPanic(WithContext(context.Background()), "boom"); !errors.Is(err, ErrPanicLoggerNotConfigured) {
		t.Fatalf("ReportPanic 错误 = %v, 期望 %v", err, ErrPanicLoggerNotConfigured)
	}
	if got := PanicLogger(); got != nil {
		t.Fatal("ReportPanic 不应创建默认 panic Logger")
	}
}

func TestRecoverAndReportWithoutLoggerRepanics(t *testing.T) {
	previous := panicLoggerPtr.Swap(nil)
	t.Cleanup(func() { panicLoggerPtr.Store(previous) })
	original := errors.New("original panic")
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		func() {
			defer RecoverAndReport(WithContext(context.Background()))
			panic(original)
		}()
	}()
	if recovered != original {
		t.Fatalf("重新抛出的 panic = %v, 期望原值 %v", recovered, original)
	}
}

func TestNamedLoggers(t *testing.T) {
	main := &bytes.Buffer{}
	req := &bytes.Buffer{}
	previous := Default()
	SetDefault(MustNew(OptWriter(NewWriter(main))))
	t.Cleanup(func() {
		SetDefault(previous)
		namedLoggers.Delete("request")
	})

	requestLogger := MustNew(OptWriter(NewWriter(req)))
	SetNamed("request", requestLogger)

	ctx := WithContext(context.Background())
	Named("request").Info(ctx, "to request file")
	Named("cron").Info(ctx, "cron falls back to default")
	Info(ctx, "to default")

	if !strings.Contains(req.String(), "to request file") {
		t.Errorf("named logger output: %q", req.String())
	}
	if strings.Contains(req.String(), "cron") {
		t.Error("fallback should not write to named logger")
	}
	out := main.String()
	if !strings.Contains(out, "cron falls back to default") || !strings.Contains(out, "to default") {
		t.Errorf("default fallback: %q", out)
	}
}

func TestSetNamedRejectsNilLogger(t *testing.T) {
	const name = "nil-registration"
	original := MustNew(OptWriter(NewWriter(&bytes.Buffer{})))
	SetNamed(name, original)
	t.Cleanup(func() { namedLoggers.Delete(name) })

	for _, input := range []struct {
		name   string
		logger Logger
	}{
		{name: "nil"},
		{name: "typed nil", logger: (*coreLogger)(nil)},
	} {
		t.Run(input.name, func(t *testing.T) {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("nil Logger 未在注册时 panic")
					}
				}()
				SetNamed(name, input.logger)
			}()
			if got := Named(name); got != original {
				t.Errorf("无效注册改变了命名 Logger: %T", got)
			}
		})
	}
}
