package logit_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/logit"
)

func TestDefaultUsesStdoutAndStandardStreamsStayOpen(t *testing.T) {
	const helperEnv = "PIXIU_TEST_STANDARD_STREAMS"
	if os.Getenv(helperEnv) == "1" {
		logit.Info(context.Background(), "default stdout info")
		logit.Error(context.Background(), "default stdout error")
		if err := logit.Close(logit.Default()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stdout.WriteString("stdout still open\n"); err != nil {
			t.Fatal(err)
		}
		if err := logit.Close(logit.MustNew(logit.OptWriter(logit.Stderr()))); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stderr.WriteString("stderr still open\n"); err != nil {
			t.Fatal(err)
		}
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestDefaultUsesStdoutAndStandardStreamsStayOpen$")
	cmd.Env = append(os.Environ(), helperEnv+"=1")
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("子进程失败: %v; stdout=%q, stderr=%q", err, stdout.String(), stderr.String())
	}
	for _, want := range []string{"default stdout info", "default stdout error", "stdout still open"} {
		if !strings.Contains(stdout.String(), want) || strings.Contains(stderr.String(), want) {
			t.Errorf("%q 应只写入 stdout: stdout=%q, stderr=%q", want, stdout.String(), stderr.String())
		}
	}
	if !strings.Contains(stderr.String(), "stderr still open") {
		t.Errorf("标准错误输出被关闭: %q", stderr.String())
	}
}

// capture 临时替换默认 logger,并在测试结束后恢复。
func capture(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	old := logit.Default()
	logit.SetDefault(logit.MustNew(logit.OptWriter(logit.NewWriter(buf))))
	t.Cleanup(func() { logit.SetDefault(old) })
	return buf
}

func TestFacadeBasic(t *testing.T) {
	buf := capture(t)
	ctx := logit.WithContext(context.Background())
	logit.AddMeta(ctx, logit.Str("logId", logit.NewLogID()))

	logit.Info(ctx, "user login", logit.Int("uid", 42))
	logit.Error(ctx, "query failed", logit.Str("mod", "Order"))

	out := buf.String()
	if !strings.Contains(out, "INFO") || !strings.Contains(out, "uid=[42]") {
		t.Errorf("info line: %q", out)
	}
	if !strings.Contains(out, "ERROR") {
		t.Errorf("error line missing: %q", out)
	}
}

func TestFacadeCallerPointsToBusinessCall(t *testing.T) {
	buf := &bytes.Buffer{}
	old := logit.Default()
	logit.SetDefault(logit.MustNew(
		logit.OptWriter(logit.NewWriter(buf)),
		logit.OptCaller(true),
	))
	t.Cleanup(func() { logit.SetDefault(old) })

	logit.Info(context.Background(), "caller")
	out := buf.String()
	if !strings.Contains(out, "logit/global_test.go") {
		t.Fatalf("facade caller should point to business call: %q", out)
	}
	if strings.Contains(out, "logit/global.go") {
		t.Fatalf("facade caller leaked implementation frame: %q", out)
	}
}

func TestFacadeRoutesByContextName(t *testing.T) {
	defaultBuf := capture(t)
	var namedBuf bytes.Buffer
	name := t.Name()
	named := logit.MustNew(logit.OptWriter(logit.NewWriter(&namedBuf)))
	logit.SetNamed(name, named)

	ctx := logit.WithLoggerName(context.Background(), name)
	if got := logit.LoggerFromContext(ctx); got != named {
		t.Fatalf("context 选中的 Logger = %v, want %v", got, named)
	}
	logit.Info(ctx, "named info")
	logit.Warn(context.WithValue(ctx, routeTestKey{}, true), "inherited name")
	logit.Output(ctx, logit.InfoLevel, 0, "named output")

	logit.Info(context.Background(), "default info")
	logit.Info(logit.WithLoggerName(ctx, ""), "empty name")
	logit.Info(logit.WithLoggerName(ctx, name+"-missing"), "unregistered name")
	if got := logit.LoggerFromContext(nil); got != logit.Default() {
		t.Fatalf("nil context 应返回默认 Logger: %v", got)
	}
	if got := namedBuf.String(); !strings.Contains(got, "named info") || !strings.Contains(got, "inherited name") || !strings.Contains(got, "named output") || strings.Contains(got, "default info") {
		t.Fatalf("命名 Logger 输出: %q", got)
	}
	if got := defaultBuf.String(); !strings.Contains(got, "default info") || !strings.Contains(got, "empty name") || !strings.Contains(got, "unregistered name") || strings.Contains(got, "named info") {
		t.Fatalf("默认 Logger 输出: %q", got)
	}
}

type routeTestKey struct{}

func TestFacadeOutputCallerDepth(t *testing.T) {
	var buf bytes.Buffer
	name := t.Name()
	logit.SetNamed(name, logit.MustNew(logit.OptWriter(logit.NewWriter(&buf)), logit.OptCaller(true)))
	ctx := logit.WithLoggerName(context.Background(), name)
	_, _, directLine, _ := runtime.Caller(0)
	logit.Output(ctx, logit.InfoLevel, 0, "direct output")
	wrap := func() { logit.Output(ctx, logit.InfoLevel, 1, "wrapped output") }
	_, _, wrappedLine, _ := runtime.Caller(0)
	wrap()
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("Output 应写两条日志，实际 %d 条: %q", len(lines), buf.String())
	}
	for i, want := range []string{fmt.Sprintf("global_test.go:%d", directLine+1), fmt.Sprintf("global_test.go:%d", wrappedLine+1)} {
		line := lines[i]
		if !strings.Contains(line, want) {
			t.Errorf("第 %d 条 caller = %q, want %q", i, line, want)
		}
	}
}

func TestFacadeContextAwareEnabled(t *testing.T) {
	old := logit.Default()
	logit.SetDefault(logit.MustNew(logit.OptWriter(logit.NewWriter(&bytes.Buffer{})), logit.OptMinLevel(logit.ErrorLevel)))
	t.Cleanup(func() { logit.SetDefault(old) })
	name := t.Name()
	logit.SetNamed(name, logit.MustNew(logit.OptWriter(logit.NewWriter(&bytes.Buffer{}))))
	ctx := logit.WithLoggerName(context.Background(), name)
	checks := []struct {
		name        string
		enabled     func(context.Context) bool
		defaultWant bool
	}{
		{name: "Debug", enabled: logit.DebugEnabled},
		{name: "Info", enabled: logit.InfoEnabled},
		{name: "Warn", enabled: logit.WarnEnabled},
		{name: "Error", enabled: logit.ErrorEnabled, defaultWant: true},
	}
	for _, check := range checks {
		if got := check.enabled(context.Background()); got != check.defaultWant {
			t.Errorf("默认 Logger 的 %sEnabled = %v, want %v", check.name, got, check.defaultWant)
		}
		if got := check.enabled(ctx); !got {
			t.Errorf("命名 Logger 的 %sEnabled = false, want true", check.name)
		}
		if got := check.enabled(logit.WithLoggerName(ctx, name+"-missing")); got != check.defaultWant {
			t.Errorf("未注册命名 Logger 的 %sEnabled = %v, want %v", check.name, got, check.defaultWant)
		}
	}
	logit.SetNamed(name, logit.MustNew(logit.OptWriter(logit.NewWriter(&bytes.Buffer{})), logit.OptMinLevel(logit.FatalLevel)))
	if logit.ErrorEnabled(ctx) || !logit.LoggerFromContext(ctx).Enabled(logit.FatalLevel) {
		t.Fatal("命名 Logger 应关闭 Error,保留 Fatal")
	}
	if logit.With(logit.Str("mod", "default")).Enabled(logit.InfoLevel) {
		t.Fatal("包级 With 应基于默认 Logger")
	}
}

func TestFacadeLogIDChain(t *testing.T) {
	buf := capture(t)
	ctx := logit.WithContext(context.Background())
	id := logit.NewLogID()
	logit.AddMeta(ctx, logit.Str("logId", id))

	// 标准库派生的 context 应携带同一 logId。
	child, cancel := context.WithCancel(ctx)
	defer cancel()

	logit.Info(ctx, "in parent")
	logit.Info(child, "in child")

	if id == "" {
		t.Fatal("no logId")
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d", len(lines))
	}
	for _, line := range lines {
		if !strings.Contains(line, "logId=["+id+"]") {
			t.Errorf("logId %s missing in %q", id, line)
		}
	}
}

func TestFacadeWithModule(t *testing.T) {
	buf := capture(t)
	svc := logit.With(logit.Str("mod", "FinanceSettlement"))
	ctx := logit.WithContext(context.Background())

	svc.Error(ctx, "transfer failed", logit.Str("biz", "x"))

	if !strings.Contains(buf.String(), "mod=[FinanceSettlement]") {
		t.Errorf("module field missing: %q", buf.String())
	}
}

func TestFacadeEnabledGuard(t *testing.T) {
	capture(t)
	ctx := context.Background()
	if !logit.InfoEnabled(ctx) || !logit.DebugEnabled(ctx) {
		t.Error("default logger should enable all levels")
	}
	logit.SetDefault(logit.MustNew(logit.OptWriter(logit.Stderr()), logit.OptMinLevel(logit.WarnLevel)))
	if logit.InfoEnabled(ctx) {
		t.Error("info should be disabled under warn min level")
	}
}

func TestFacadeConcurrentSetDefault(t *testing.T) {
	old := logit.Default()
	t.Cleanup(func() { logit.SetDefault(old) })
	ctx := logit.WithContext(context.Background())
	const workers = 4
	buffers := make([]*lockedBuffer, workers)
	loggers := make([]logit.Logger, workers)
	for i := range loggers {
		buffers[i] = &lockedBuffer{}
		loggers[i] = logit.MustNew(logit.OptWriter(logit.NewWriter(buffers[i])))
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			logit.SetDefault(loggers[n])
			logit.Info(ctx, "concurrent default", logit.Int("g", n))
		}(i)
	}
	wg.Wait()
	var logged int
	for _, buf := range buffers {
		logged += strings.Count(buf.String(), "concurrent default")
	}
	if logged != workers {
		t.Fatalf("并发替换期间写入 %d 条日志, want %d", logged, workers)
	}
}

type typedNilLogger struct{ logit.Logger }

func TestGlobalLoggerRegistrationRejectsNil(t *testing.T) {
	original := logit.Default()
	t.Cleanup(func() { logit.SetDefault(original) })
	for _, input := range []struct {
		name   string
		logger logit.Logger
	}{
		{name: "nil"},
		{name: "typed nil", logger: (*typedNilLogger)(nil)},
	} {
		t.Run(input.name, func(t *testing.T) {
			func() {
				defer func() {
					if recover() == nil {
						t.Error("nil Logger 未在注册时 panic")
					}
				}()
				logit.SetDefault(input.logger)
			}()
			if got := logit.Default(); got != original {
				t.Errorf("无效注册改变了默认 Logger: %T", got)
			}
		})
	}
}

// lockedBuffer 是并发安全的 bytes.Buffer(bytes.Buffer 不满足 Writer 并发契约)。
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

func TestFacadeSingleImport(t *testing.T) {
	// 固定设计决策:业务只 import logit 即可完成字段构造 + 链路 API + 日志输出。
	ctx := logit.WithContext(context.Background())
	logit.AddField(ctx, logit.Str("k", "v"), logit.Int("n", 1))
	var _ logit.Field = logit.Bool("b", true)
	buf := capture(t)
	logit.Info(ctx, "aliases ok", logit.Dur("cost", 0))
	if !strings.Contains(buf.String(), "aliases ok") {
		t.Errorf("call failed: %q", buf.String())
	}
}

// TestDirectConstruction 固定另一个使用姿势:不经全局门面,直接构造实例注入。
func TestDirectConstruction(t *testing.T) {
	buf := &bytes.Buffer{}
	logger := logit.MustNew(
		logit.OptWriter(logit.NewWriter(buf)),
		logit.OptMinLevel(logit.InfoLevel),
		logit.OptFilterKeys("token"),
	)
	ctx := logit.WithContext(context.Background())
	logit.AddMeta(ctx, logit.Str("logId", logit.NewLogID()))
	logger.Info(ctx, "direct", logit.Str("token", "secret"))
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("filter failed: %q", buf.String())
	}
}
