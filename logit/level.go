package logit

import (
	"strconv"
	"strings"
)

// Level 是位掩码日志级别。单个级别只占一个比特,数值大小与严重程度一致,
// 因此可以直接用数值比较做"最低级别"过滤;字段可见性用位与判断。
type Level uint16

const (
	// UnknownLevel 未设置级别。
	UnknownLevel Level = 0

	// DebugLevel 调试信息。
	DebugLevel Level = 1 << 0

	// InfoLevel 常规运行信息。
	InfoLevel Level = 1 << 1

	// WarnLevel 需要关注的异常。
	WarnLevel Level = 1 << 2

	// ErrorLevel 出错,但程序可继续运行。
	ErrorLevel Level = 1 << 3

	// FatalLevel 致命错误,程序无法继续运行。
	FatalLevel Level = 1 << 4

	// AllLevels 所有比特位置位,用于字段可见性:任意级别的日志行都输出该字段。
	AllLevels Level = 1<<8 - 1
)

// String 返回级别名,如 "INFO"。
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	case FatalLevel:
		return "FATAL"
	case AllLevels:
		return "ALL"
	}
	return "UNKNOWN"
}

// Is 报告字段可见性掩码 l 是否在 lineLevel 级别的日志行中可见。
// 例如 l=DebugLevel 只在 DebugLevel 日志行可见;l=AllLevels 在任何级别可见。
func (l Level) Is(lineLevel Level) bool {
	return l&lineLevel == lineLevel
}

// ParseLevel 解析级别字符串,大小写不敏感,如 "info"、"ERROR"、"Warn"。
// 仅在加载配置时调用,不在热路径。
func ParseLevel(s string) (Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return DebugLevel, nil
	case "info":
		return InfoLevel, nil
	case "warn", "warning":
		return WarnLevel, nil
	case "error":
		return ErrorLevel, nil
	case "fatal":
		return FatalLevel, nil
	}
	return UnknownLevel, &levelParseError{s: s}
}

type levelParseError struct{ s string }

func (e *levelParseError) Error() string {
	return "log: unknown level name " + strconv.Quote(e.s)
}
