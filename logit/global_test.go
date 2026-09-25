package logit_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
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

// capture 用 Swap 捕获默认 logger 的输出,并在测试结束后恢复。
func capture(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	old := logit.Swap(logit.MustNew(logit.OptWriter(logit.NewWriter(buf))))
	t.Cleanup(func() { logit.Swap(old) })
	return buf
}

func TestFacadeBasic(t *testing.T) {
	buf := capture(t)
	ctx := logit.NewTraceContext(context.Background())

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
	old := logit.Swap(logit.MustNew(
		logit.OptWriter(logit.NewWriter(buf)),
		logit.OptCaller(true),
	))
	t.Cleanup(func() { logit.Swap(old) })

	logit.Info(context.Background(), "caller")
	out := buf.String()
	if !strings.Contains(out, "logit/global_test.go") {
		t.Fatalf("facade caller should point to business call: %q", out)
	}
	if strings.Contains(out, "logit/global.go") {
		t.Fatalf("facade caller leaked implementation frame: %q", out)
	}
}

func TestFacadeLogIDChain(t *testing.T) {
	buf := capture(t)
	ctx := logit.NewTraceContext(context.Background())

	// 标准库派生的 context 应携带同一 logId。
	child, cancel := context.WithCancel(ctx)
	defer cancel()

	logit.Info(ctx, "in parent")
	logit.Info(child, "in child")

	id := logit.LogID(ctx)
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
	if !logit.InfoEnabled() || !logit.DebugEnabled() {
		t.Error("default logger should enable all levels")
	}
	logit.SetDefault(logit.MustNew(logit.OptWriter(logit.Stderr()), logit.OptMinLevel(logit.WarnLevel)))
	t.Cleanup(func() {
		logit.SetDefault(logit.MustNew(logit.OptWriter(logit.Stderr())))
	})
	if logit.InfoEnabled() {
		t.Error("info should be disabled under warn min level")
	}
}

func TestFacadeConcurrentSwap(t *testing.T) {
	base := capture(t)
	ctx := logit.WithContext(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Writer 契约要求并发安全:用锁保护的 buffer,bytes.Buffer 本身不是。
			buf := &lockedBuffer{}
			logit.Swap(logit.MustNew(logit.OptWriter(logit.NewWriter(buf))))
			logit.Info(ctx, "swapped", logit.Int("g", n))
			logit.Swap(logit.MustNew(logit.OptWriter(logit.Stderr())))
		}(i)
	}
	wg.Wait()
	_ = base
}

func TestSwapIsAtomicExchange(t *testing.T) {
	original := logit.Default()
	t.Cleanup(func() { logit.SetDefault(original) })

	const swaps = 512
	for round := 0; round < 5; round++ {
		loggers := make([]logit.Logger, swaps)
		for i := range loggers {
			loggers[i] = logit.MustNew(logit.OptWriter(logit.NewWriter(&bytes.Buffer{})))
		}
		start := make(chan struct{})
		olds := make(chan logit.Logger, swaps)
		var wg sync.WaitGroup
		for _, logger := range loggers {
			wg.Add(1)
			go func(logger logit.Logger) {
				defer wg.Done()
				<-start
				olds <- logit.Swap(logger)
			}(logger)
		}
		close(start)
		wg.Wait()
		close(olds)

		seen := make(map[logit.Logger]struct{}, swaps)
		for old := range olds {
			if _, duplicate := seen[old]; duplicate {
				t.Fatalf("round %d returned the same old Logger more than once", round)
			}
			seen[old] = struct{}{}
		}
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
	ctx := logit.NewTraceContext(context.Background())
	logger.Info(ctx, "direct", logit.Str("token", "secret"))
	if strings.Contains(buf.String(), "secret") {
		t.Errorf("filter failed: %q", buf.String())
	}
}
