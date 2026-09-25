package logit

import (
	"errors"
	"testing"
	"time"
)

func TestFieldConstructors(t *testing.T) {
	tests := []struct {
		name  string
		field Field
		typ   FieldType
	}{
		{"bool", Bool("b", true), boolType},
		{"int", Int("i", -3), int64Type},
		{"int64", Int64("i64", -4), int64Type},
		{"uint64", Uint64("u64", 5), uint64Type},
		{"float", Float64("f", 1.5), float64Type},
		{"str", Str("s", "x"), strType},
		{"dur", Dur("d", time.Second), durationType},
		{"time", Time("t", time.Now()), timeType},
		{"any", Any("a", struct{}{}), reflectType},
	}
	for _, tt := range tests {
		if tt.field.Type() != tt.typ {
			t.Errorf("%s: type = %d, want %d", tt.name, tt.field.Type(), tt.typ)
		}
		if tt.field.Key == "" {
			t.Errorf("%s: empty key", tt.name)
		}
	}
}

func TestIntNegativeRoundTrip(t *testing.T) {
	f := Int("v", -3)
	if int64(f.num) != -3 {
		t.Errorf("int round trip = %d, want -3", int64(f.num))
	}
}

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

func TestDefer(t *testing.T) {
	called := false
	f := Defer("lazy", func() Field {
		called = true
		return Int("lazy", 1)
	})
	if f.Type() != deferType {
		t.Errorf("Defer type = %d", f.Type())
	}
	if called {
		t.Error("Defer fn should not run at construction")
	}
	resolved := f.val.(func() Field)()
	if !called || resolved.Key != "lazy" || resolved.Type() != int64Type {
		t.Errorf("Defer resolve = %+v, called=%v", resolved, called)
	}
}
