package elasticsearchx

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestDecodeSourcesPreservesOrderAndIntegerPrecision(t *testing.T) {
	type document struct {
		ID   int64  `json:"id"`
		Name string `json:"name"`
	}
	hits := json.RawMessage(`[
		{"_id":"second","sort":[2],"_source":{"id":9007199254740993,"name":"第二项","extra":true}},
		{"_id":"first","_source":{"id":1,"name":"第一项"}},
		{"_source":{}}
	]`)
	want := []document{
		{ID: 9007199254740993, Name: "第二项"},
		{ID: 1, Name: "第一项"},
		{},
	}
	got, err := DecodeSources[document](hits)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("命中顺序、整数精度或空对象语义有误: got=%+v err=%v", got, err)
	}
}

func TestDecodeSourcesEmptyArray(t *testing.T) {
	got, err := DecodeSources[struct{}](json.RawMessage(" \n [ ] \t"))
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("零命中应返回非 nil 空切片: got=%v err=%v", got, err)
	}
}

func TestDecodeSourcesRejectsInvalidHits(t *testing.T) {
	for _, tt := range []struct {
		name string
		hits string
	}{
		{name: "缺失输入"},
		{name: "null 输入", hits: "null"},
		{name: "非数组输入", hits: "{}"},
		{name: "截断 JSON", hits: `[{"_source":`},
		{name: "额外 JSON", hits: `[] {}`},
		{name: "非对象命中", hits: `[1]`},
		{name: "null 命中", hits: `[null]`},
		{name: "缺失 source", hits: `[{}]`},
		{name: "null source", hits: `[{"_source":null}]`},
		{name: "数组 source", hits: `[{"_source":[]}]`},
		{name: "字符串 source", hits: `[{"_source":"document"}]`},
		{name: "数字 source", hits: `[{"_source":123}]`},
		{name: "布尔 source", hits: `[{"_source":true}]`},
		{name: "后续命中缺失 source", hits: `[{"_source":{}},{}]`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := DecodeSources[struct{}](json.RawMessage(tt.hits))
			if err == nil || got != nil {
				t.Fatalf("无效命中应失败且不返回部分结果: got=%v err=%v", got, err)
			}
		})
	}
}

func TestDecodeSourcesPreservesDecodeError(t *testing.T) {
	type document struct {
		ID int64 `json:"id"`
	}
	got, err := DecodeSources[document](json.RawMessage(`[
		{"_source":{"id":1}},
		{"_source":{"id":"invalid"}}
	]`))
	var typeErr *json.UnmarshalTypeError
	if got != nil || !errors.As(err, &typeErr) ||
		!strings.HasSuffix(typeErr.Field, "_source.id") || typeErr.Offset <= 0 {
		t.Fatalf("字段解码失败应保留错误链且不返回部分结果: got=%v err=%v", got, err)
	}
}

func TestDecodeSourcesReportsMissingSourcePosition(t *testing.T) {
	for _, hit := range []string{`{}`, `{"_source":null}`} {
		got, err := DecodeSources[struct{}](json.RawMessage(`[{"_source":{}},` + hit + `]`))
		if got != nil || err == nil || !strings.Contains(err.Error(), "hit 1") {
			t.Fatalf("缺失或 null source 应标明命中位置: got=%v err=%v", got, err)
		}
	}
}

func TestDecodeSourcesPointerAndMapDocuments(t *testing.T) {
	type document struct {
		ID   int64     `json:"id"`
		Next *document `json:"next"`
	}
	hits := json.RawMessage(`[{"_source":{"id":1,"next":{"id":2}}}]`)
	values, err := DecodeSources[document](hits)
	if err != nil || len(values) != 1 || values[0].ID != 1 || values[0].Next == nil || values[0].Next.ID != 2 {
		t.Fatalf("递归 DTO 应正常解码: got=%+v err=%v", values, err)
	}
	documents, err := DecodeSources[*document](hits)
	if err != nil || len(documents) != 1 || documents[0] == nil || documents[0].ID != 1 ||
		documents[0].Next == nil || documents[0].Next.ID != 2 {
		t.Fatalf("指针与递归结构体应正常解码: got=%+v err=%v", documents, err)
	}
	maps, err := DecodeSources[map[string]int64](json.RawMessage(`[{"_source":{"id":9007199254740993}}]`))
	if err != nil || len(maps) != 1 || maps[0]["id"] != 9007199254740993 {
		t.Fatalf("map 文档应保留整数精度: got=%v err=%v", maps, err)
	}
}

func TestDecodeSourcesUsesTargetTypeSemantics(t *testing.T) {
	for _, source := range []string{`123`, `"text"`, `[]`, `true`, `{"id":"1"}`} {
		t.Run(source, func(t *testing.T) {
			hits := json.RawMessage(`[{"_source":` + source + `}]`)
			if got, err := DecodeSources[any](hits); err != nil || len(got) != 1 {
				t.Fatalf("any 应接受非 null JSON 值: got=%v err=%v", got, err)
			}
			if got, err := DecodeSources[json.RawMessage](hits); err != nil || len(got) != 1 || string(got[0]) != source {
				t.Fatalf("RawMessage 应保留非 null JSON 值: got=%s err=%v", got, err)
			}
		})
	}
	hits := json.RawMessage(`[{"_source":null}]`)
	if got, err := DecodeSources[any](hits); err == nil || got != nil {
		t.Fatalf("any 仍须拒绝 null source: got=%v err=%v", got, err)
	}
	if got, err := DecodeSources[json.RawMessage](hits); err == nil || got != nil {
		t.Fatalf("RawMessage 仍须拒绝 null source: got=%s err=%v", got, err)
	}
}

type sourceDecodeCounter struct {
	Calls int
}

func (value *sourceDecodeCounter) UnmarshalJSON([]byte) error {
	value.Calls++
	return nil
}

func TestDecodeSourcesUsesCustomDecoder(t *testing.T) {
	for _, source := range []string{`{}`, `123`} {
		got, err := DecodeSources[sourceDecodeCounter](json.RawMessage(`[{"_source":` + source + `}]`))
		if err != nil || len(got) != 1 || got[0].Calls != 1 {
			t.Fatalf("source 类型由自定义解码器判断且只调用一次: got=%v err=%v", got, err)
		}
	}
}

type sourceDecodeFailure struct{}

func (*sourceDecodeFailure) UnmarshalJSON([]byte) error {
	sourceDecodeFailureCalls++
	return errSourceDecodeFailure
}

var errSourceDecodeFailure = errors.New("测试字段解码失败")
var sourceDecodeFailureCalls int

func TestDecodeSourcesPreservesNestedCustomError(t *testing.T) {
	sourceDecodeFailureCalls = 0
	type document struct {
		Value sourceDecodeFailure `json:"value"`
	}
	got, err := DecodeSources[document](json.RawMessage(`[{"_source":{}},{"_source":{"value":1}}]`))
	if got != nil || !errors.Is(err, errSourceDecodeFailure) {
		t.Fatalf("嵌套自定义解码失败应保留错误链: got=%v err=%v", got, err)
	}
	if sourceDecodeFailureCalls != 1 {
		t.Fatalf("失败时不得重复调用自定义解码器: calls=%d", sourceDecodeFailureCalls)
	}
}

type sourceTextDecoder struct{}

func (*sourceTextDecoder) UnmarshalText([]byte) error {
	return nil
}

func TestDecodeSourcesUsesTextDecoder(t *testing.T) {
	got, err := DecodeSources[sourceTextDecoder](json.RawMessage(`[{"_source":"text"}]`))
	if err != nil || len(got) != 1 {
		t.Fatalf("字符串 source 应交给文本解码器处理: got=%v err=%v", got, err)
	}
}
