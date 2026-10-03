package jsonx

import (
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/iotest"
)

func TestDecodeOneSingleValue(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input string
		want  any
	}{
		{"对象", `{"count":1}`, map[string]any{"count": float64(1)}},
		{"首尾空白", " \n{\"count\":1}\t\r\n", map[string]any{"count": float64(1)}},
		{"数组", `[1,true]`, []any{float64(1), true}},
		{"字符串", `"value"`, "value"},
		{"数字", `42`, float64(42)},
		{"布尔值", `true`, true},
		{"空值", `null`, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, split := range []bool{false, true} {
				var reader io.Reader = strings.NewReader(tt.input)
				if split {
					reader = iotest.OneByteReader(reader)
				}
				var got any
				if err := DecodeOne(reader, &got); err != nil || !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("分段=%v: got=%#v err=%v, want=%#v", split, got, err, tt.want)
				}
			}
		})
	}
}

func TestDecodeOneRejectsExtraContent(t *testing.T) {
	for _, input := range []string{
		`{"count":1}{"count":2}`,
		`{"count":1} null`,
		`{"count":1} 2`,
		`{"count":1} []`,
		`{"count":1} trailing`,
		`{"count":1} {`,
	} {
		t.Run(input, func(t *testing.T) {
			for _, split := range []bool{false, true} {
				var reader io.Reader = strings.NewReader(input)
				if split {
					reader = iotest.OneByteReader(reader)
				}
				var got any
				if err := DecodeOne(reader, &got); err == nil {
					t.Fatalf("分段=%v: 未拒绝额外内容", split)
				}
			}
		})
	}
}

func TestDecodeOneStandardSemantics(t *testing.T) {
	for _, input := range []string{"", " \n\t"} {
		var got any
		if err := DecodeOne(strings.NewReader(input), &got); err != io.EOF {
			t.Fatalf("空输入错误=%v, want EOF", err)
		}
	}
	var got struct {
		Count int `json:"count"`
	}
	if err := DecodeOne(strings.NewReader(`{"count":1,"unknown":true}`), &got); err != nil || got.Count != 1 {
		t.Fatalf("未知字段应沿用标准库行为: got=%v err=%v", got, err)
	}
	var typeErr *json.UnmarshalTypeError
	if err := DecodeOne(strings.NewReader(`{"count":"invalid"}`), &got); !errors.As(err, &typeErr) {
		t.Fatalf("目标类型错误未保留: %v", err)
	}
	var invalidTarget *json.InvalidUnmarshalError
	if err := DecodeOne(strings.NewReader(`{}`), got); !errors.As(err, &invalidTarget) {
		t.Fatalf("非法目标错误未保留: %v", err)
	}
}

// 同一次 Read 可以同时返回有效字节与读取错误；后续读取不再重复该错误。
type finalErrorReader struct {
	data string
	err  error
}

func (r *finalErrorReader) Read(p []byte) (int, error) {
	if r.data == "" {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	if r.data == "" {
		return n, r.err
	}
	return n, nil
}

func TestDecodeOnePreservesReadErrors(t *testing.T) {
	readErr := errors.New("读取失败")
	for _, input := range []string{`{"count":1}`, `{"count":`, `{"count":]}`} {
		t.Run(input, func(t *testing.T) {
			var got any
			err := DecodeOne(&finalErrorReader{data: input, err: readErr}, &got)
			if !errors.Is(err, readErr) {
				t.Fatalf("读取错误丢失: %v", err)
			}
			if input == `{"count":]}` {
				var syntaxErr *json.SyntaxError
				if !errors.As(err, &syntaxErr) {
					t.Fatalf("语法错误丢失: %v", err)
				}
			}
		})
	}
	var got any
	reader := io.MultiReader(strings.NewReader(`{"count":1}`), iotest.ErrReader(readErr))
	if err := DecodeOne(reader, &got); !errors.Is(err, readErr) {
		t.Fatalf("确认结束时读取错误丢失: %v", err)
	}
	if err := DecodeOne(&finalErrorReader{data: `{"count":1}`, err: io.EOF}, &got); err != nil {
		t.Fatalf("有效 JSON 随 EOF 返回不应失败: %v", err)
	}
}

type trackedReadCloser struct {
	io.Reader
	closed bool
}

func (r *trackedReadCloser) Close() error {
	r.closed = true
	return nil
}

func TestDecodeOneDoesNotCloseReader(t *testing.T) {
	for _, input := range []string{`{}`, `{`} {
		reader := &trackedReadCloser{Reader: strings.NewReader(input)}
		var got any
		_ = DecodeOne(reader, &got)
		if reader.closed {
			t.Fatalf("不应关闭调用方的 Reader: %q", input)
		}
	}
}
