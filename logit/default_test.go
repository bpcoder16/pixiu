package logit_test

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bpcoder16/pixiu/logit"
)

func TestUnconfiguredDefaultIsSilent(t *testing.T) {
	const helperEnv = "PIXIU_TEST_UNCONFIGURED_LOGGER"
	if os.Getenv(helperEnv) != "1" {
		// 新进程保证检查包加载后的初始状态，不依赖其他测试的注册顺序。
		cmd := exec.Command(os.Args[0], "-test.run=^TestUnconfiguredDefaultIsSilent$")
		cmd.Env = append(os.Environ(), helperEnv+"=1")
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("未配置日志的子进程失败: %v; stdout=%q stderr=%q", err, stdout.String(), stderr.String())
		}
		if stdout.String() != "PASS\n" || stderr.Len() != 0 {
			t.Fatalf("未配置时不应产生测试框架以外的输出: stdout=%q stderr=%q", stdout.String(), stderr.String())
		}
		return
	}

	deferred := logit.Defer("deferred", func() logit.Field {
		panic("未配置日志时不应执行 Defer 回调")
	})
	// 空操作 Logger 不校验输出字段；显式构造的 Logger 仍按原约定拒绝保留键。
	fields := []logit.Field{logit.Str("msg", "reserved"), deferred}
	ctx := logit.WithStart(context.Background())
	logit.AddDownstreamDurationAuto(ctx, "redis", time.Millisecond)
	for _, current := range []context.Context{nil, context.Background(), logit.WithLoggerName(ctx, "missing")} {
		if logit.DebugEnabled(current) || logit.InfoEnabled(current) || logit.WarnEnabled(current) || logit.ErrorEnabled(current) {
			t.Fatal("未配置时不应启用任何日志级别")
		}
		logit.Debug(current, "silent", fields...)
		logit.Info(current, "silent", fields...)
		logit.Warn(current, "silent", fields...)
		logit.Error(current, "silent", fields...)
		logit.Fatal(current, "silent", fields...)
		logit.Output(current, logit.FatalLevel, 0, "silent", fields...)
		logit.InfoDuration(current, "silent", fields...)
	}
	initial := logit.Default()
	if initial == nil {
		t.Fatal("Default 不应返回 nil")
	}
	for _, logger := range []logit.Logger{initial, logit.Named("missing"), logit.With(fields...), initial.With(fields...)} {
		for _, level := range []logit.Level{0, logit.DebugLevel, logit.InfoLevel, logit.WarnLevel, logit.ErrorLevel, logit.FatalLevel, logit.AllLevels} {
			if logger.Enabled(level) {
				t.Fatalf("空操作 Logger 启用了级别 %v", level)
			}
			logger.Output(ctx, level, 0, "silent", fields...)
		}
		logger.Debug(ctx, "silent", fields...)
		logger.Info(ctx, "silent", fields...)
		logger.Warn(ctx, "silent", fields...)
		logger.Error(ctx, "silent", fields...)
		logger.Fatal(ctx, "silent", fields...)
		if err := logit.Close(logger); err != nil {
			t.Fatalf("关闭空操作 Logger: %v", err)
		}
	}
	// 即使已开启请求计时，空操作路径也不应创建耗时汇总切片。
	if got := testing.AllocsPerRun(100, func() { logit.InfoDuration(ctx, "silent") }); got != 0 {
		t.Fatalf("空操作 InfoDuration 分配了内存: %v", got)
	}

	var output bytes.Buffer
	configured := logit.MustNew(logit.OptWriter(logit.NewWriter(&output)))
	t.Cleanup(func() { _ = logit.Close(configured) })
	configured.Info(ctx, "standalone")
	logit.SetNamed("configured", configured)
	logit.Info(logit.WithLoggerName(ctx, "configured"), "named")
	logit.Info(ctx, "still silent")
	if got := output.String(); strings.Count(got, "\n") != 2 || !strings.Contains(got, "standalone") || !strings.Contains(got, "named") {
		t.Fatalf("独立和命名 Logger 应在默认未配置时正常输出: %q", got)
	}
	output.Reset()
	logit.SetDefault(configured)
	logit.Info(ctx, "configured default")
	initial.Info(ctx, "old reference stays silent")
	if got := output.String(); strings.Count(got, "\n") != 1 || !strings.Contains(got, "configured default") {
		t.Fatalf("设置默认 Logger 后的输出错误: %q", got)
	}
}
