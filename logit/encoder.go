package logit

import (
	"encoding/json"
	"math"
	"strconv"
	"time"
)

// timestampLayout 统一日志前缀与 Time 字段的固定毫秒 ISO 8601/RFC 3339 格式。
const timestampLayout = "2006-01-02T15:04:05.000Z07:00"

// Encoder 负责把一条日志记录的各部分按顺序追加进字节缓冲。
// 全部方法以 append 风格工作,编码过程零中间对象分配。
type Encoder interface {
	// AppendPrefix 追加行前缀(级别、时间,以及调用方已解析好的 caller)。
	AppendPrefix(buf []byte, level Level, now time.Time, caller string) []byte
	// AppendField 追加一个字段(f 已完成 Defer 求值与脱敏)。
	AppendField(buf []byte, f Field) []byte
	// AppendMessage 追加消息体。
	AppendMessage(buf []byte, msg string) []byte
	// Finish 追加行尾(如换行符)。
	Finish(buf []byte) []byte
}

// callerPrefixEncoder 仅供内置编码器使用:把已裁剪的 file 与 line 直接写入行缓冲,
// 避免为 caller 额外拼接字符串。line < 0 表示定位失败,只写 file("unknown")。
// 自定义 Encoder 仍走公开的 AppendPrefix(caller string) 契约。
type callerPrefixEncoder interface {
	appendPrefixCaller(buf []byte, level Level, now time.Time, file string, line int) []byte
}

// TextEncoder 输出以下格式的文本行:
//
//	INFO: 2026-09-14T18:00:00.000+08:00 main.go:42 uid=[42] msg=[user login]
type TextEncoder struct{}

// DefaultTextEncoder 是共享的文本编码器实例(无状态,可复用)。
var DefaultTextEncoder = TextEncoder{}

// AppendPrefix 实现 Encoder。
func (TextEncoder) AppendPrefix(buf []byte, level Level, now time.Time, caller string) []byte {
	buf = append(buf, level.String()...)
	buf = append(buf, ": "...)
	buf = now.AppendFormat(buf, timestampLayout)
	if caller != "" {
		buf = append(buf, ' ')
		buf = append(buf, caller...)
	}
	return append(buf, ' ')
}

func (e TextEncoder) appendPrefixCaller(buf []byte, level Level, now time.Time, file string, line int) []byte {
	buf = e.AppendPrefix(buf, level, now, "")
	if file == "" && line < 0 {
		return buf
	}
	buf = append(buf, file...)
	if line >= 0 {
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, int64(line), 10)
	}
	return append(buf, ' ')
}

// AppendField 实现 Encoder。
func (TextEncoder) AppendField(buf []byte, f Field) []byte {
	buf = appendTextString(buf, f.Key)
	buf = append(buf, "=["...)
	buf = appendFieldValue(buf, f)
	return append(buf, "] "...)
}

// AppendMessage 实现 Encoder。
func (TextEncoder) AppendMessage(buf []byte, msg string) []byte {
	buf = append(buf, msgKey+"=["...)
	buf = appendTextString(buf, msg)
	return append(buf, ']')
}

// Finish 实现 Encoder:裁掉行尾多余分隔空格后换行。
func (TextEncoder) Finish(buf []byte) []byte {
	if n := len(buf); n > 0 && buf[n-1] == ' ' {
		buf = buf[:n-1]
	}
	return append(buf, '\n')
}

// appendFieldValue 按字段类型选择零反射的序列化路径。
func appendFieldValue(buf []byte, f Field) []byte {
	switch f.typ {
	case boolType:
		return strconv.AppendBool(buf, f.num == 1)
	case int64Type:
		return strconv.AppendInt(buf, int64(f.num), 10)
	case uint64Type:
		return strconv.AppendUint(buf, f.num, 10)
	case float64Type:
		return strconv.AppendFloat(buf, math.Float64frombits(f.num), 'f', -1, 64)
	case strType, errType:
		return appendTextString(buf, f.str)
	case durationType:
		// 时长以毫秒浮点数输出,固定保留三位小数。
		return strconv.AppendFloat(buf, float64(int64(f.num))/float64(time.Millisecond), 'f', 3, 64)
	case timeType:
		return f.val.(time.Time).AppendFormat(buf, timestampLayout)
	case reflectType:
		b, err := json.Marshal(f.val)
		if err != nil {
			return appendTextString(buf, err.Error())
		}
		return appendTextString(buf, string(b))
	}
	return append(buf, "<?>"...)
}

const textHexDigits = "0123456789abcdef"

// appendTextString 转义会破坏单行或方括号边界的字节。非 ASCII 内容原样保留;
// 控制字符使用可读短转义或 \xNN,保证普通字段不额外产生物理换行。
func appendTextString(dst []byte, s string) []byte {
	start := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b >= 0x20 && b != 0x7f && b != '\\' && b != ']' {
			continue
		}
		dst = append(dst, s[start:i]...)
		switch b {
		case '\\', ']':
			dst = append(dst, '\\', b)
		case '\n':
			dst = append(dst, '\\', 'n')
		case '\r':
			dst = append(dst, '\\', 'r')
		case '\t':
			dst = append(dst, '\\', 't')
		case '\b':
			dst = append(dst, '\\', 'b')
		case '\f':
			dst = append(dst, '\\', 'f')
		default:
			dst = append(dst, '\\', 'x', textHexDigits[b>>4], textHexDigits[b&0x0f])
		}
		start = i + 1
	}
	dst = append(dst, s[start:]...)
	return dst
}
