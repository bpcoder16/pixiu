package logit

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// Option 是 Logger 的构造选项,支持配置文件与纯编程两种入口(P0 提供编程式,
// 配置文件加载由上层 app 模块解析后转成 Option 传入)。
type Option func(*config)

type config struct {
	encoder      Encoder
	minLevel     Level
	caller       bool
	filterKeys   []string
	targets      []Target
	fileName     string
	noExit       bool
	onWriteError func(error)
}

// OptMinLevel 设置最低输出级别,低于该级别的日志直接丢弃,默认 DebugLevel。
func OptMinLevel(l Level) Option {
	return func(c *config) { c.minLevel = l }
}

// OptCaller 开启行前缀中的调用点定位(默认关闭,开销约百纳秒)。
func OptCaller(on bool) Option {
	return func(c *config) { c.caller = on }
}

// OptFilterKeys 设置脱敏字段名:命中字段值统一打码为 ***。
func OptFilterKeys(keys ...string) Option {
	return func(c *config) { c.filterKeys = append(c.filterKeys, keys...) }
}

// OptOnWriteError 注册写入错误回调。日志调用仍不返回该错误;回调应快速完成,
// 且不要写回同一个持续失败的 Logger。
func OptOnWriteError(fn func(error)) Option {
	return func(c *config) { c.onWriteError = fn }
}

// OptNoExit 让 Fatal 级别日志写入并尽力 Sync 后不退出进程。
// 用于 panic 日志等需要记录 Fatal、又要继续运行的 Logger。
func OptNoExit() Option {
	return func(c *config) { c.noExit = true }
}

// OptEncoder 指定编码器,默认 DefaultTextEncoder；nil 编码器立即 panic。
func OptEncoder(enc Encoder) Option {
	if isNilInterface(enc) {
		panic("logit: nil encoder")
	}
	return func(c *config) { c.encoder = enc }
}

// OptWriter 设置单一目标:全部级别写到 w。
func OptWriter(w Writer) Option {
	return func(c *config) {
		c.fileName = ""
		c.targets = []Target{{Levels: allLevelList(), Writer: w}}
	}
}

// OptFileName 设置绝对路径的追加模式日志文件作为单一目标。文件延迟到 New 最终校验后打开;
// 打开失败会让 New/MustNew 返回错误(启动期暴露,而不是静默丢日志)。
func OptFileName(name string) Option {
	return func(c *config) {
		c.fileName = name
		c.targets = nil
	}
}

// OptDispatch 设置分级分发目标(如 Warn 以上写 .wf 文件)。
// 同一 Writer 可被多个 Target 复用。
func OptDispatch(targets ...Target) Option {
	return func(c *config) {
		c.fileName = ""
		c.targets = targets
	}
}

func allLevelList() []Level {
	return []Level{DebugLevel, InfoLevel, WarnLevel, ErrorLevel, FatalLevel}
}

// New 构造 Logger。无目标、空 Writer 或空 Levels 的 Target 视为配置错误。
func New(opts ...Option) (Logger, error) {
	c := config{
		encoder:  DefaultTextEncoder,
		minLevel: DebugLevel,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	if c.encoder == nil {
		return nil, errors.New("log: nil encoder")
	}
	if bitIndex(c.minLevel) < 0 {
		return nil, fmt.Errorf("log: invalid minimum level %d", c.minLevel)
	}
	if c.fileName != "" {
		w, err := OpenFile(c.fileName)
		if err != nil {
			return nil, err
		}
		c.targets = []Target{{Levels: allLevelList(), Writer: w}}
	}
	if len(c.targets) == 0 {
		return nil, errors.New("log: no writer target configured")
	}
	targets := make([]Target, len(c.targets))
	for i, t := range c.targets {
		targets[i] = t
		targets[i].Levels = append([]Level(nil), t.Levels...)
	}
	minLevel := &atomic.Uint32{}
	minLevel.Store(uint32(c.minLevel))
	l := &coreLogger{
		encoder:      c.encoder,
		minLevel:     minLevel,
		caller:       c.caller,
		targets:      targets,
		pool:         &sync.Pool{New: func() any { return &lineState{buf: make([]byte, 0, 256), order: make([]Field, 0, 12)} }},
		writeState:   &writeErrorTracker{},
		onWriteError: c.onWriteError,
	}
	if !c.noExit {
		l.exit = os.Exit
	}
	if len(c.filterKeys) > 0 {
		l.filterKeys = make(map[string]struct{}, len(c.filterKeys))
		for _, k := range c.filterKeys {
			l.filterKeys[k] = struct{}{}
		}
	}
	for _, t := range targets {
		if isNilInterface(t.Writer) {
			return nil, errors.New("log: target with nil writer")
		}
		if t.Writer.WriterKey() == (WriterKey{}) {
			return nil, errors.New("log: target with zero writer key")
		}
		if len(t.Levels) == 0 {
			return nil, errors.New("log: target with empty levels")
		}
		for _, lv := range t.Levels {
			i := bitIndex(lv)
			if i < 0 {
				return nil, fmt.Errorf("log: invalid level %d in target", lv)
			}
			l.levelWriter[i] = t.Writer
		}
	}
	return l, nil
}

// MustNew 构造 Logger,失败 panic(仅用于启动期)。
func MustNew(opts ...Option) Logger {
	l, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return l
}

// Close 关闭 Logger 的全部目标 writer(应用优雅关闭的最后一步调用)。
func Close(l Logger) error {
	cl, ok := l.(*coreLogger)
	if !ok {
		return nil
	}
	var firstErr error
	for _, w := range uniqueTargetWriters(cl.targets) {
		if err := w.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// uniqueTargetWriters 按稳定的 WriterKey 去重，不依赖 Writer 动态值是否可比较。
func uniqueTargetWriters(targets []Target) []Writer {
	writers := make([]Writer, 0, len(targets))
	seen := make(map[WriterKey]struct{}, len(targets))
	for _, target := range targets {
		w := target.Writer
		key := w.WriterKey()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		writers = append(writers, w)
	}
	return writers
}
