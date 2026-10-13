package logit

import (
	"errors"
	"testing"
	"time"
)

func TestErr(t *testing.T) {
	f := Err(errors.New("boom"))
	if f.Type() != strType || f.str != "boom" {
		t.Errorf("Err = %v %q", f.Type(), f.str)
	}
	if got := Err(nil); got.str != "nil" {
		t.Errorf("Err(nil) = %q, want nil", got.str)
	}
	custom := ErrKey("cause", errors.New("x"))
	if custom.Key != "cause" || custom.str != "x" {
		t.Errorf("ErrKey = %q %q", custom.Key, custom.str)
	}
}

func TestAuto(t *testing.T) {
	tests := []struct {
		v    any
		typ  FieldType
		desc string
	}{
		{true, boolType, "bool"},
		{42, int64Type, "int"},
		{int64(7), int64Type, "int64"},
		{int32(7), int64Type, "int32"},
		{uint16(7), uint64Type, "uint16"},
		{3.14, float64Type, "float64"},
		{float32(1.5), float64Type, "float32"},
		{"txt", strType, "string"},
		{[]byte("bytes"), strType, "[]byte"},
		{time.Minute, durationType, "duration"},
		{time.Unix(0, 0), timeType, "time"},
		{errors.New("e"), strType, "error"},
		{struct{ A int }{1}, reflectType, "struct"},
		{nil, strType, "nil"},
	}
	for _, tt := range tests {
		f := Auto("k", tt.v)
		if f.Type() != tt.typ {
			t.Errorf("Auto(%s): type = %d, want %d", tt.desc, f.Type(), tt.typ)
		}
	}
	if got := Auto("k", nil); got.str != "<nil>" {
		t.Errorf("Auto(nil) str = %q", got.str)
	}
}

func TestDeferRejectsNilCallback(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("Defer(nil) 未在构造时 panic")
		}
	}()
	_ = Defer("lazy", nil)
}
