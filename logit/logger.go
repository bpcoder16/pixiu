package logit

import (
	"context"
	"errors"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

// Logger 是 pixiu 日志的核心接口。业务代码只依赖它,不依赖任何具体实现。
type Logger interface {
	// Debug 打印 Debug 级别日志。
	Debug(ctx context.Context, msg string, fields ...Field)
	// Info 打印 Info 级别日志。
	Info(ctx context.Context, msg string, fields ...Field)
	// Warn 打印 Warn 级别日志。
	Warn(ctx context.Context, msg string, fields ...Field)
	// Error 打印 Error 级别日志。
	Error(ctx context.Context, msg string, fields ...Field)
	// Fatal 打印 Fatal 级别日志,尽力 Sync 后退出进程。
	Fatal(ctx context.Context, msg string, fields ...Field)
	// Output 打印指定级别日志;callDepth 用于二次封装时修正 caller 定位,
	// 0 表示 Output 的直接调用者。
	Output(ctx context.Context, level Level, callDepth int, msg string, fields ...Field)
	// Enabled 报告该级别日志是否会被输出,供热路径手动守卫:
	// if logit.DebugEnabled() { ... } 可彻底省掉字段求值与变参分配。
	Enabled(level Level) bool
	// With 返回预埋了固定字段的子 Logger(如模块级 mod 字段),不改变其余行为。
	// 传入保留字段名会 panic。
	With(fields ...Field) Logger
}

// LevelController 是 Logger 的可选运行时级别控制能力。
type LevelController interface {
	SetMinLevel(Level) error
	MinLevel() Level
}

// ErrLevelControlUnsupported 表示 Logger 没有实现运行时级别调整。
var ErrLevelControlUnsupported = errors.New("logit: logger does not support runtime level control")

// SetMinLevel 在 Logger 支持时原子调整最低输出级别。
func SetMinLevel(l Logger, level Level) error {
	controller, ok := l.(LevelController)
	if !ok {
		return ErrLevelControlUnsupported
	}
	return controller.SetMinLevel(level)
}

// Target 把若干级别路由到一个 Writer,实现分级文件(如 .wf)。
type Target struct {
	Levels []Level
	Writer Writer
}

// coreLogger 是 Logger 的实现:编码后经 Target 分发到 Writer;
// 同步直写与轮转均由 Writer 层提供。
type coreLogger struct {
	encoder    Encoder
	minLevel   *atomic.Uint32
	caller     bool
	filterKeys map[string]struct{}
	base       []Field
	targets    []Target
	// levelWriter 按级别比特位(Debug=0 … Fatal=4)直接索引目标 writer,分发零查找。
	levelWriter [5]Writer
	// pool 为指针:With 克隆 Logger 时共享同一个行缓冲池,避免拷贝 sync.Pool。
	pool *sync.Pool // *lineState
	exit func(int)

	writeState   *writeErrorTracker
	onWriteError func(error)
}

var _ Logger = (*coreLogger)(nil)

// lineState 是每条日志记录的复用状态:编码缓冲 + 字段暂存。
type lineState struct {
	buf   []byte
	order []Field
}

// bitIndex 返回级别比特位序号(Debug=0 … Fatal=4);非法值返回 -1。
// TrailingZeros16 是单条指令(TZCNT),禁用路径热身检查零循环。
func bitIndex(l Level) int {
	if l == 0 || l > FatalLevel || l&(l-1) != 0 {
		return -1
	}
	return bits.TrailingZeros16(uint16(l))
}

// enabledIndex 在一次最低级别原子读取中完成校验与路由检查。
func (l *coreLogger) enabledIndex(level Level) int {
	i := bitIndex(level)
	if i < 0 {
		return -1
	}
	if level < Level(l.minLevel.Load()) || l.levelWriter[i] == nil {
		return -1
	}
	return i
}

func (l *coreLogger) Enabled(level Level) bool { return l.enabledIndex(level) >= 0 }

func (l *coreLogger) SetMinLevel(level Level) error {
	if bitIndex(level) < 0 {
		return errors.New("logit: invalid minimum level")
	}
	l.minLevel.Store(uint32(level))
	return nil
}

func (l *coreLogger) MinLevel() Level { return Level(l.minLevel.Load()) }

func (l *coreLogger) Debug(ctx context.Context, msg string, fields ...Field) {
	l.Output(ctx, DebugLevel, 1, msg, fields...)
}

func (l *coreLogger) Info(ctx context.Context, msg string, fields ...Field) {
	l.Output(ctx, InfoLevel, 1, msg, fields...)
}

func (l *coreLogger) Warn(ctx context.Context, msg string, fields ...Field) {
	l.Output(ctx, WarnLevel, 1, msg, fields...)
}

func (l *coreLogger) Error(ctx context.Context, msg string, fields ...Field) {
	l.Output(ctx, ErrorLevel, 1, msg, fields...)
}

func (l *coreLogger) Fatal(ctx context.Context, msg string, fields ...Field) {
	l.Output(ctx, FatalLevel, 1, msg, fields...)
}

func (l *coreLogger) With(fields ...Field) Logger {
	if len(fields) == 0 {
		return l
	}
	for _, f := range fields {
		rejectReservedField(f.Key)
	}
	clone := *l
	clone.base = make([]Field, 0, len(l.base)+len(fields))
	clone.base = append(clone.base, l.base...)
	clone.base = append(clone.base, fields...)
	return &clone
}

// Output 是所有日志方法的唯一汇聚点。
//
// 语义:
//   - 调用点保留字段名始终 panic;其余字段在级别未启用或无目标 writer 时直接返回
//   - 字段输出顺序:With 预埋 → meta 字段 → ctx 普通字段 → 调用点字段;
//     同名 key 不合并,按出现顺序逐个输出
//   - Defer 字段此刻才求值;命中 FilterKeys 的字段值打码为 ***
//   - 写入失败不返回给业务调用,但通过 WriteErrorStats/回调保持可观测
func (l *coreLogger) Output(ctx context.Context, level Level, callDepth int, msg string, fields ...Field) {
	for _, f := range fields {
		rejectReservedField(f.Key)
	}
	i := l.enabledIndex(level)
	if i < 0 {
		if level == FatalLevel && l.exit != nil {
			l.exit(1)
		}
		return
	}
	w := l.levelWriter[i]
	ls, _ := l.pool.Get().(*lineState)
	if ls == nil {
		ls = &lineState{buf: make([]byte, 0, 256), order: make([]Field, 0, 12)}
	}
	ls.buf = ls.buf[:0]
	ls.order = ls.order[:0]
	defer l.pool.Put(ls)

	if l.caller {
		// 只对内置具体类型走快速路径。嵌入内置编码器的自定义类型可能重写
		// AppendPrefix,仍须通过公开接口传入完整 caller 字符串。
		switch l.encoder.(type) {
		case TextEncoder, *TextEncoder, JSONEncoder, *JSONEncoder:
			enc := l.encoder.(callerPrefixEncoder)
			_, file, line, found := runtime.Caller(callDepth + 1)
			if found {
				file = trimCallerPath(file)
			} else {
				file, line = "unknown", -1
			}
			ls.buf = enc.appendPrefixCaller(ls.buf, level, time.Now(), file, line)
		default:
			ls.buf = l.encoder.AppendPrefix(ls.buf, level, time.Now(), CallerPath(callDepth+1))
		}
	} else {
		ls.buf = l.encoder.AppendPrefix(ls.buf, level, time.Now(), "")
	}

	ls.order = append(ls.order, l.base...)
	eachVisible(ctx, level, func(f Field) {
		ls.order = append(ls.order, f)
	})
	ls.order = append(ls.order, fields...)
	for _, f := range ls.order {
		if f.typ == deferType || l.filterKeys != nil {
			f = l.prepareField(f)
		}
		ls.buf = l.encoder.AppendField(ls.buf, f)
	}

	ls.buf = l.encoder.AppendMessage(ls.buf, msg)
	ls.buf = l.encoder.Finish(ls.buf)

	n, err := w.Write(ls.buf)
	l.recordWriteResult(n, len(ls.buf), err)
	if level == FatalLevel {
		// 普通日志只保证 Write 完成;Fatal 退出前还要尽力持久化。
		if syncer, ok := w.(interface{ Sync() error }); ok {
			if err := syncer.Sync(); err != nil {
				l.writeState.record(err)
				if l.onWriteError != nil {
					l.onWriteError(err)
				}
			}
		}
		if l.exit != nil {
			l.exit(1)
		}
	}
}

func (l *coreLogger) recordWriteResult(n, want int, err error) {
	err = normalizeWriteError(n, want, err)
	if err == nil {
		return
	}
	l.writeState.record(err)
	if l.onWriteError != nil {
		l.onWriteError(err)
	}
}

func (l *coreLogger) WriteErrors() int64 { return l.writeState.countValue() }

func (l *coreLogger) LastWriteError() error { return l.writeState.lastValue() }

// prepareField 在字段收集完成后执行脱敏与 Defer 求值,
// 避免 context 字段的回调发生在 fieldStore 的读锁内。
func (l *coreLogger) prepareField(f Field) Field {
	if l.filterKeys != nil {
		if _, ok := l.filterKeys[f.Key]; ok {
			f.typ, f.str, f.num, f.val = strType, "***", 0, nil
		}
	}
	if f.typ == deferType {
		if fn, ok := f.val.(func() Field); ok {
			resolved := fn()
			resolved.Key = f.Key // Defer 构造时的 key 优先
			f = resolved
		} else {
			f = Field{Key: f.Key, typ: strType, str: "<bad defer>"}
		}
	}
	return f
}
