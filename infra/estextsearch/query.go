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
	return query(field, text, false)
}

// QueryExpanded 在 Query 的基础上扩展混合输入的召回，校验和返回值约定与 Query 相同。
// 以含符号片段分隔普通文本块，保留块内跨词同义词和符号片段的严格字面约束。
// 满足混合规则时采用整句 OR 块组合，省去已被块组合覆盖的外层分段 AND，减少重复评分。
// 不满足规则或超出块数预算时与 Query 一致；扩展召回仍有成本，分数及排名可能改变。
func QueryExpanded(field, text string) (map[string]any, error) {
	return query(field, text, true)
}

func query(field, text string, expanded bool) (map[string]any, error) {
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
	return textQuery(field, text, expanded), nil
}

func textQuery(field, text string, expanded bool) map[string]any {
	whole := singleQuery(field, text)
	segments := splitSegments(text)
	// 仅原输入为单片段或超出分段预算时省略分段路径；重复片段去重后仍须参与召回。
	if len(segments) == 0 || (len(segments) == 1 && segments[0] == text) {
		return whole
	}
	var blocks []string
	if expanded {
		blocks = splitMixedBlocks(text)
	}
	var combined map[string]any
	if len(blocks) == 0 {
		must := make([]any, 0, len(segments))
		for _, segment := range segments {
			must = append(must, singleQuery(field, segment))
		}
		combined = map[string]any{"bool": map[string]any{"must": must}}
	} else {
		var blockMust, literalFilter []any
		for _, block := range blocks {
			if hasSymbol(block) {
				literalFilter = append(literalFilter, singleQuery(field, block))
			} else {
				// 沿用基础规则，保留整块同义词和分段包含能力。
				blockMust = append(blockMust, textQuery(field, block, false))
			}
		}
		// 字面条件独立强制匹配，不让编号再次受全文分词限制，也不重复贡献评分。
		combined = map[string]any{"bool": map[string]any{
			"must":   blockMust,
			"filter": literalFilter,
		}}
	}
	// 混合块内的分段 AND 已覆盖原分段路径，外层只需保留整句和组合两条路径。
	return map[string]any{"bool": map[string]any{
		"should":               []any{whole, combined},
		"minimum_should_match": 1,
	}}
}

// splitMixedBlocks 以含符号的片段为边界，保留普通块的原始空白和重复词上下文。
func splitMixedBlocks(text string) []string {
	if !hasSymbol(text) {
		return nil
	}
	var blocks []string
	start, offset, plainCount := 0, 0, 0
	hasPhrase := false
	for part := range strings.FieldsSeq(text) {
		partStart := offset + strings.Index(text[offset:], part)
		offset = partStart + len(part)
		if !hasSymbol(part) {
			plainCount++
			continue
		}
		if plainCount > 0 {
			blocks = append(blocks, strings.TrimSpace(text[start:partStart]))
			hasPhrase = hasPhrase || plainCount > 1
		}
		blocks = append(blocks, part)
		// 新增路径独立限制块数，重复片段也计数；超限不影响两条原路径。
		if len(blocks) > maxSegments {
			return nil
		}
		start, plainCount = offset, 0
	}
	if plainCount > 0 {
		blocks = append(blocks, strings.TrimSpace(text[start:]))
		hasPhrase = hasPhrase || plainCount > 1
	}
	// 没有连续多词普通块时，现有分段路径已经覆盖，无须重复查询和评分。
	if !hasPhrase || len(blocks) > maxSegments {
		return nil
	}
	return blocks
}

func hasSymbol(text string) bool {
	return strings.ContainsFunc(text, func(r rune) bool { return unicode.IsPunct(r) || unicode.IsSymbol(r) })
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
	if count == 1 || hasSymbol(text) {
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
