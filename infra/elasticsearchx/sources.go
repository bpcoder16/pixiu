package elasticsearchx

import (
	"encoding/json"
	"errors"
	"fmt"
)

// DecodeSources 按命中顺序将 SearchResult.Hits 中每个 _source 解码为 T。
// 输入必须为 JSON 数组，每个 _source 必须存在且非 null；空数组返回非 nil 空切片。
// 类型匹配与自定义解码行为沿用 encoding/json，不额外限制 _source 的 JSON 类型。
// 失败时返回 nil 和错误，不保留部分结果；缺失或 null 的 source 标明从 0 开始的命中位置。
// 解码错误保留原始错误链；保留整数精度时应使用明确的整数类型或 json.Number，
// map[string]any 等动态字段中的数字仍默认解码为 float64。
// 本函数只解码，不判断结果完整性或业务必填字段；调用方须先处理 Search 返回的错误。
func DecodeSources[T any](hits json.RawMessage) ([]T, error) {
	var entries []struct {
		Source *T `json:"_source"`
	}
	if err := json.Unmarshal(hits, &entries); err != nil {
		return nil, fmt.Errorf("elasticsearchx: decode search hits: %w", err)
	}
	if entries == nil {
		return nil, errors.New("elasticsearchx: missing or invalid search hits array")
	}
	sources := make([]T, len(entries))
	for i, entry := range entries {
		if entry.Source == nil {
			return nil, fmt.Errorf("elasticsearchx: hit %d missing or null _source", i)
		}
		sources[i] = *entry.Source
	}
	return sources, nil
}
