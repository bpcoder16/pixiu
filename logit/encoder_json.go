package logit

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

// JSONEncoder 输出有序 JSON 行:键顺序 = 编码顺序(前缀 → 字段 → msg),
// 直接追加进调用方缓冲,无中间 map、无排序。
//
// 字符串转义使用表驱动扫描和干净段整段拷贝，并转义 U+2028/U+2029；
// 输出与 encoding/json(SetEscapeHTML(false)) 逐字节一致，不转义 HTML 字符。
//
// 行形如:
//
//	{"level":"INFO","ts":"2026-09-15T10:00:00.123+08:00","logId":["..."],
//	 "uid":42,"msg":"user login"}
type JSONEncoder struct{}

// DefaultJSONEncoder 是共享的 JSON 编码器实例(无状态,可复用)。
var DefaultJSONEncoder = JSONEncoder{}

// AppendPrefix 实现 Encoder:写入 {"level":..,"ts":..(,"caller":..) 与尾随逗号。
func (JSONEncoder) AppendPrefix(buf []byte, level Level, now time.Time, caller string) []byte {
	buf = append(buf, `{"`+levelKey+`":`...)
	buf = appendJSONString(buf, level.String())
	buf = append(buf, `,"`+tsKey+`":"`...)
	buf = now.AppendFormat(buf, timestampLayout)
	buf = append(buf, '"')
	if caller != "" {
		buf = append(buf, `,"`+callerKey+`":`...)
		buf = appendJSONString(buf, caller)
	}
	return append(buf, ',')
}

func (e JSONEncoder) appendPrefixCaller(buf []byte, level Level, now time.Time, file string, line int) []byte {
	buf = e.AppendPrefix(buf, level, now, "")
	if file == "" && line < 0 {
		return buf
	}
	buf = append(buf, `"`+callerKey+`":`...)
	buf = appendJSONString(buf, file)
	if line >= 0 {
		buf = buf[:len(buf)-1] // 把 :line 追加在 JSON 字符串的闭合引号之前。
		buf = append(buf, ':')
		buf = strconv.AppendInt(buf, int64(line), 10)
		buf = append(buf, '"')
	}
	return append(buf, ',')
}

// AppendField 实现 Encoder:写入 "key":value 与尾随逗号。
func (JSONEncoder) AppendField(buf []byte, f Field) []byte {
	buf = appendJSONString(buf, f.Key)
	buf = append(buf, ':')
	buf = appendJSONValue(buf, f)
	return append(buf, ',')
}

// AppendMessage 实现 Encoder:写入 "msg":... 与尾随逗号。
func (JSONEncoder) AppendMessage(buf []byte, msg string) []byte {
	buf = append(buf, `"`+msgKey+`":`...)
	buf = appendJSONString(buf, msg)
	return append(buf, ',')
}

// Finish 实现 Encoder:去掉最后一个尾随逗号,闭合对象并换行。
func (JSONEncoder) Finish(buf []byte) []byte {
	if n := len(buf); n > 0 && buf[n-1] == ',' {
		buf = buf[:n-1]
	}
	return append(buf, '}', '\n')
}

// appendJSONValue 按字段类型选择零反射序列化路径(Reflect 除外)。
func appendJSONValue(buf []byte, f Field) []byte {
	switch f.typ {
	case boolType:
		return strconv.AppendBool(buf, f.num == 1)
	case int64Type:
		return strconv.AppendInt(buf, int64(f.num), 10)
	case uint64Type:
		return strconv.AppendUint(buf, f.num, 10)
	case float64Type:
		v := math.Float64frombits(f.num)
		switch {
		case math.IsNaN(v):
			return appendJSONString(buf, "NaN")
		case math.IsInf(v, 1):
			return appendJSONString(buf, "+Inf")
		case math.IsInf(v, -1):
			return appendJSONString(buf, "-Inf")
		default:
			return strconv.AppendFloat(buf, v, 'f', -1, 64)
		}
	case strType, errType:
		return appendJSONString(buf, f.str)
	case durationType:
		// 毫秒浮点,与 TextEncoder 口径一致。
		return strconv.AppendFloat(buf, float64(int64(f.num))/float64(time.Millisecond), 'f', 3, 64)
	case timeType:
		buf = append(buf, '"')
		buf = f.val.(time.Time).AppendFormat(buf, timestampLayout)
		return append(buf, '"')
	case reflectType:
		return appendJSONReflectValue(buf, f.val)
	}
	return appendJSONString(buf, "<?>")
}

// appendJSONReflectValue 将 RawMessage 校验并压缩到现有行缓冲，避免格式化 JSON
// 破坏单行日志。无效输入回退 json.Marshal，保持既有的错误字符串语义。
func appendJSONReflectValue(buf []byte, value any) []byte {
	var raw []byte
	switch v := value.(type) {
	case json.RawMessage:
		raw = v
	case *json.RawMessage:
		if v != nil {
			raw = *v
		}
	}
	if raw != nil {
		dst := bytes.NewBuffer(buf)
		if err := json.Compact(dst, raw); err == nil {
			return dst.Bytes()
		}
	}
	b, err := json.Marshal(value)
	if err != nil {
		return appendJSONString(buf, err.Error())
	}
	return append(buf, b...)
}

// jsonNeedsEscape 标记需要转义的 ASCII 字节:控制字符(<0x20)、'"'、'\\'。
var jsonNeedsEscape = [256]bool{}

const jsonHexDigits = "0123456789abcdef"

func init() {
	for i := range 0x20 {
		jsonNeedsEscape[i] = true
	}
	jsonNeedsEscape['"'] = true
	jsonNeedsEscape['\\'] = true
}

// appendJSONString 追加一个转义后的 JSON 字符串(含首尾引号)。
// 快路径:ASCII 干净字节整段拷贝;慢路径:逐个处理需转义字节与多字节 rune
// (非法 UTF-8 → U+FFFD;U+2028/U+2029 → \u202x)。
func appendJSONString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if !jsonNeedsEscape[b] {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '\\', '"':
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
				dst = append(dst, '\\', 'u', '0', '0', jsonHexDigits[b>>4], jsonHexDigits[b&0xF])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, "\ufffd"...)
		} else if r == '\u2028' || r == '\u2029' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', jsonHexDigits[r&0xF])
		} else {
			i += size
			continue
		}
		i += size
		start = i
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
