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
	SetPanicLogger(MustNew(OptWriter(NewWriter(buf)), OptNoExit()))
	t.Cleanup(func() { panicLoggerPtr.Store(nil) })
	return buf
}

func TestReportPanicSingleLine(t *testing.T) {
	buf := capturePanicLogger(t)
	ctx := NewTraceContext(context.Background())

	if err := ReportPanic(ctx, "boom", Str("where", "TestReportPanic")); err != nil {
		t.Fatal(err)
	}

	out := buf.String()
	if !strings.HasPrefix(out, "FATAL:") {
		t.Errorf("should log at FATAL level (sync bypass): %q", out[:min(len(out), 40)])
	}
	if strings.Count(out, "\n") != 1 {
		t.Errorf("panic log must be single line, got %d newlines", strings.Count(out, "\n"))
	}
	if !strings.Contains(out, "panic=[boom]") {
		t.Errorf("panic value missing: %q", out)
	}
	if !strings.Contains(out, `stack=[goroutine`) {
		t.Errorf("escaped stack missing: %q", out)
	}
	if !strings.Contains(out, `\n`) {
		t.Errorf("stack newlines should be escaped: %q", out)
	}
	if !strings.Contains(out, "pid=[") || !strings.Contains(out, "processStart=[") {
		t.Errorf("pid/processStart missing: %q", out)
	}
	if !strings.Contains(out, "logId=[") {
		t.Errorf("logId from ctx missing: %q", out)
	}
	if !strings.Contains(out, "where=[TestReportPanic]") {
		t.Errorf("custom field missing: %q", out)
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
