package logit

import (
	"math"
	"time"
)

// FieldType 标记 Field 携带的数据类型,编码器据此选择零反射的序列化路径。
type FieldType uint8

const (
	invalidType FieldType = iota
	boolType
	int64Type
	uint64Type
	float64Type
	strType
	errType
	durationType
	timeType
	reflectType
	deferType
	panicStackType
)

// Field 是值类型的日志字段:数值直接存进 num,不经过 interface 装箱、不逃逸。
// float64 以位模式存于 num;布局 64B,5 字段变参切片 344B,降低调用点分配。
// 构造函数返回值,调用点以变参切片传给 Logger。
type Field struct {
	Key string

	typ FieldType
	num uint64 // bool: 0/1;int64/uint64: 原值;float64: 位模式;duration: 纳秒
	str string // strType 的值 / errType 的 error.Error()
	val any    // 仅 timeType/reflectType/deferType 使用(冷路径)
}

// Type 返回字段类型。
func (f Field) Type() FieldType { return f.typ }

// Bool 构造 bool 字段。
func Bool(key string, v bool) Field {
	var n uint64
	if v {
		n = 1
	}
	return Field{Key: key, typ: boolType, num: n}
}

// Int 构造 int 字段。
func Int(key string, v int) Field { return Field{Key: key, typ: int64Type, num: uint64(v)} }

// Int64 构造 int64 字段。
func Int64(key string, v int64) Field {
	return Field{Key: key, typ: int64Type, num: uint64(v)}
}

// Uint64 构造 uint64 字段。
func Uint64(key string, v uint64) Field { return Field{Key: key, typ: uint64Type, num: v} }

// Float64 构造 float64 字段。
func Float64(key string, v float64) Field {
	return Field{Key: key, typ: float64Type, num: math.Float64bits(v)}
}

// Str 构造 string 字段。
func Str(key string, v string) Field { return Field{Key: key, typ: strType, str: v} }

// Err 构造 error 字段,构造时求一次 Error(),打印时零额外开销。
func Err(err error) Field {
	if err == nil {
		return Field{Key: "err", typ: strType, str: "nil"}
	}
	return Field{Key: "err", typ: strType, str: err.Error()}
}

// ErrKey 构造自定义 key 的 error 字段。
func ErrKey(key string, err error) Field {
	f := Err(err)
	f.Key = key
	return f
}

// Dur 构造 time.Duration 字段。
func Dur(key string, v time.Duration) Field {
	return Field{Key: key, typ: durationType, num: uint64(v)}
}

// Time 构造 time.Time 字段(装箱存储,属冷路径)。
func Time(key string, v time.Time) Field {
	return Field{Key: key, typ: timeType, val: v}
}

// Any 构造任意类型字段,编码时走反射序列化(冷路径,慎用于热路径)。
func Any(key string, v any) Field {
	return Field{Key: key, typ: reflectType, val: v}
}

// Defer 构造惰性字段:只有当日志行级别检查通过、真正编码时才调用 fn 求值。
// fn 应返回一个不含 deferType 的普通字段。
func Defer(key string, fn func() Field) Field {
	return Field{Key: key, typ: deferType, val: fn}
}

// Auto 依据运行时类型分发给强类型构造器,避免装箱;未识别类型退化为 Any。
func Auto(key string, v any) Field {
	switch val := v.(type) {
	case nil:
		return Str(key, "<nil>")
	case bool:
		return Bool(key, val)
	case int:
		return Int(key, val)
	case int8:
		return Int64(key, int64(val))
	case int16:
		return Int64(key, int64(val))
	case int32:
		return Int64(key, int64(val))
	case int64:
		return Int64(key, val)
	case uint:
		return Uint64(key, uint64(val))
	case uint8:
		return Uint64(key, uint64(val))
	case uint16:
		return Uint64(key, uint64(val))
	case uint32:
		return Uint64(key, uint64(val))
	case uint64:
		return Uint64(key, val)
	case float64:
		return Float64(key, val)
	case float32:
		return Float64(key, float64(val))
	case string:
		return Str(key, val)
	case []byte:
		return Str(key, string(val))
	case time.Duration:
		return Dur(key, val)
	case time.Time:
		return Time(key, val)
	case error:
		return ErrKey(key, val)
	}
	return Any(key, v)
}
