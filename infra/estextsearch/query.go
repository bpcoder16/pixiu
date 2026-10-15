package estextsearch

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

var (
	// ErrInvalidField 表示字段名为空、编码无效，或包含空白、控制字符、通配符。
	ErrInvalidField = errors.New("estextsearch: 字段名无效")
	// ErrInvalidUTF8 表示查询文本不是有效 UTF-8；错误不包含查询原文。
	ErrInvalidUTF8 = errors.New("estextsearch: 查询文本 UTF-8 无效")
)

const maxSegments = 16

// Query 构建文本查询子句，自动使用 field、field.contains 和 field.contains_unigram。
// 字段可为普通对象字段路径；调用方须确保对应 mapping 存在。
// 空白文本返回 nil, nil，应用应省略该条件；输入长度由应用限制。
// 多片段采用整句查询 OR 分段 AND；只有完全相同的片段才去重。
// 分析器、同义词和评分沿用 ES 配置；返回值可直接嵌入请求，每次调用均独立分配。
func Query(field, text string) (map[string]any, error) {
	if field == "" || !utf8.ValidString(field) || strings.ContainsAny(field, "*?") ||
		strings.ContainsFunc(field, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return nil, ErrInvalidField
	}
	if !utf8.ValidString(text) {
		return nil, ErrInvalidUTF8
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, nil
	}
	whole := singleQuery(field, text)
	segments := splitSegments(text)
	if len(segments) < 2 {
		return whole, nil
	}
	must := make([]any, 0, len(segments))
	for _, segment := range segments {
		must = append(must, singleQuery(field, segment))
	}
	// 整句保留跨词同义词语义；分段 AND 补充编号、标点和分词差异下的召回。
	return map[string]any{"bool": map[string]any{
		"should": []any{
			whole,
			map[string]any{"bool": map[string]any{"must": must}},
		},
		"minimum_should_match": 1,
	}}, nil
}

func singleQuery(field, text string) map[string]any {
	count := utf8.RuneCountInString(text)
	containsField := field + ".contains"
	if count < 3 {
		containsField = field + ".contains_unigram"
	}
	contains := map[string]any{"match_phrase": map[string]any{containsField: map[string]any{
		"query": text,
		"slop":  0,
	}}}
	// 单字符不依赖分词；含标点、符号时禁止全文分支忽略这些字符后扩大召回。
	if count == 1 || strings.ContainsFunc(text, func(r rune) bool { return unicode.IsPunct(r) || unicode.IsSymbol(r) }) {
		return contains
	}
	return map[string]any{"bool": map[string]any{
		"should": []any{
			map[string]any{"match": map[string]any{field: map[string]any{
				"query":    text,
				"operator": "and",
			}}},
			contains,
		},
		"minimum_should_match": 1,
	}}
}

func splitSegments(text string) []string {
	var segments []string
	seen := make(map[string]bool)
	for part := range strings.FieldsSeq(text) {
		if seen[part] {
			continue
		}
		if len(segments) == maxSegments {
			// 超出时仅保留完整输入的整句查询，不截断或丢弃后续关键词。
			return nil
		}
		seen[part] = true
		segments = append(segments, part)
	}
	return segments
}
