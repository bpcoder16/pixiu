package bootstrap

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/infra/env"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/bpcoder16/pixiu/rotatefile"
)

func initLog(cfg httpconfig.LogConfig, resources *lifecycle.Stack) error {
	var encoder logit.Encoder
	switch cfg.Format {
	case "text":
		encoder = logit.DefaultTextEncoder
	case "json":
		encoder = logit.DefaultJSONEncoder
	default:
		return fmt.Errorf("log.format must be text or json, got %q", cfg.Format)
	}
	level := logit.DebugLevel
	if env.RunMode() == env.RunModeRelease {
		level = logit.InfoLevel
	}
	if cfg.Rotate.Every == 0 {
		cfg.Rotate.Every = time.Hour
	}
	if cfg.Rotate.MaxFiles == 0 {
		cfg.Rotate.MaxFiles = 48
	}
	if cfg.Rotate.Every != time.Hour && cfg.Rotate.Every != 24*time.Hour {
		return fmt.Errorf("log.rotate.every must be 1h or 24h, got %s", cfg.Rotate.Every)
	}
	if cfg.Rotate.MaxFiles < 3 {
		return fmt.Errorf("log.rotate.maxFiles must be at least 3, got %d", cfg.Rotate.MaxFiles)
	}
	appName := env.AppName()
	if err := validateLogFilePart(appName); err != nil {
		return fmt.Errorf("env.appName: %w", err)
	}
	// 先校验所有名字及其分流文件名，避免同一路径被多个 Writer 打开。
	names := append([]string{""}, cfg.Names...)
	paths := make(map[string]bool)
	for _, name := range names {
		if name != "" {
			if err := validateLogFilePart(name); err != nil {
				return fmt.Errorf("log.names: %w", err)
			}
		}
		for _, suffix := range []string{".info", ".debug", ".wf"} {
			path := logBaseName(appName, name) + suffix + ".log"
			if paths[path] {
				return fmt.Errorf("log.names: duplicate log file %q", path)
			}
			paths[path] = true
		}
	}
	if cfg.Dir == "" {
		cfg.Dir = "log"
	}
	if !filepath.IsAbs(cfg.Dir) {
		cfg.Dir = filepath.Join(env.RootDirPath(), cfg.Dir)
	}
	if err := prepareLogDirectory(cfg.Dir); err != nil {
		return err
	}

	loggers := make([]logit.Logger, 0, len(names))
	for _, name := range names {
		logger, err := newLogLogger(cfg, logBaseName(appName, name), encoder, level)
		if err != nil {
			return err
		}
		// 成功构造的 Logger 交给应用栈，后续初始化失败也由 main 的 defer 收尾。
		if registerErr := resources.Register(func() error { return logit.Close(logger) }); registerErr != nil {
			return errors.Join(registerErr, logit.Close(logger))
		}
		loggers = append(loggers, logger)
	}
	// 全部创建并登记成功后再发布，避免暴露半初始化的日志配置。
	logit.SetDefault(loggers[0])
	for i, name := range cfg.Names {
		logit.SetNamed(name, loggers[i+1])
	}
	return nil
}

func validateLogFilePart(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed != value || strings.ContainsAny(value, "/\\\x00") {
		return fmt.Errorf("invalid log file name component %q", value)
	}
	return nil
}

func logBaseName(appName, name string) string {
	if name == "" {
		return appName
	}
	return appName + "." + name
}

func prepareLogDirectory(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create log.dir directory %q: %w", dir, err)
	}
	// 实际创建文件检查当前进程权限，兼容 ACL；权限位本身不足以判断可写性。
	probe, err := os.CreateTemp(dir, ".pixiu-log-write-*")
	if err != nil {
		return fmt.Errorf("log.dir directory %q is not writable: %w", dir, err)
	}
	if cleanupErr := errors.Join(probe.Close(), os.Remove(probe.Name())); cleanupErr != nil {
		return fmt.Errorf("check log.dir directory %q: %w", dir, cleanupErr)
	}
	return nil
}

func newLogLogger(cfg httpconfig.LogConfig, base string, encoder logit.Encoder, level logit.Level) (logger logit.Logger, err error) {
	targets := make([]logit.Target, 0, 3)
	// Logger 尚未创建成功时，由构造函数清理已打开的 Writer。
	defer func() {
		if err != nil {
			for _, target := range slices.Backward(targets) {
				err = errors.Join(err, target.Writer.Close())
			}
		}
	}()
	for _, group := range []struct {
		suffix string
		levels []logit.Level
	}{
		{suffix: ".info", levels: []logit.Level{logit.InfoLevel}},
		{suffix: ".debug", levels: []logit.Level{logit.DebugLevel}},
		{suffix: ".wf", levels: []logit.Level{logit.WarnLevel, logit.ErrorLevel, logit.FatalLevel}},
	} {
		path := filepath.Join(cfg.Dir, base+group.suffix+".log")
		file, openErr := rotatefile.New(path,
			rotatefile.OptEvery(cfg.Rotate.Every),
			rotatefile.OptMaxFiles(cfg.Rotate.MaxFiles),
		)
		if openErr != nil {
			return nil, fmt.Errorf("open log file %q: %w", path, openErr)
		}
		writer := logit.NewWriter(file)
		targets = append(targets, logit.Target{Levels: group.levels, Writer: writer})
	}
	return logit.New(
		logit.OptDispatch(targets...),
		logit.OptEncoder(encoder),
		logit.OptMinLevel(level),
		logit.OptCaller(cfg.Caller),
	)
}
