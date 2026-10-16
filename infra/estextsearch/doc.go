// Package estextsearch 构建短文本检索的 Elasticsearch 查询子句，仅依赖标准库。
// Query(field, text) 提供基础召回，QueryExpanded(field, text) 扩展混合输入的召回。
// 两个入口均无需初始化或配置对象，不执行网络请求，由调用方选择召回与性能的取舍。
//
// 使用示例：
//
//	clause, err := estextsearch.Query("query_text", "apple PR-PV02")
//	if err != nil {
//		return err
//	}
//	boolQuery := map[string]any{
//		"filter": []any{
//			map[string]any{"term": map[string]any{"merchant_user_id": "10001"}},
//		},
//	}
//	if clause != nil {
//		boolQuery["must"] = []any{clause}
//	}
//	request := map[string]any{
//		"query": map[string]any{"bool": boolQuery},
//		"size":  20,
//	}
//	_ = request // 交给应用使用的 ES 客户端执行。
//
// 需要组合跨词同义词和编号时，可将上述构建调用替换为：
//
//	clause, err := estextsearch.QueryExpanded("query_text", "personal computer PR-PV02")
//	if err != nil {
//		return err
//	}
//	_ = clause // 使用相同的方式嵌入请求。
//
// 两个入口使用主字段及固定后缀 .contains、.contains_unigram，调用方须确保这些字段存在。
// 普通文本采用全文 OR 连续包含；1～2 字符使用 unigram，至少 3 字符使用 trigram。
// 单字符默认支持；单段中的单字符或含标点、符号的文本只走包含，避免分词忽略关键字符。
// 包含使用 match_phrase、slop 0，不设 32 字符的分支切换，也不依赖 edge_ngram。
//
// Query 的多片段采用“整句查询 OR 各片段全部命中”，保留整句全文分支的跨词同义词。
// 按空白分段，仅去除完全相同的片段；超过 16 个不同片段时只保留整句，不截断输入。
// 重复输入去重后仅剩一个片段时，仍保留“整句 OR 单片段”，避免编号或部分词漏召回。
// QueryExpanded 在混合输入含连续多词普通块时，采用“整句查询 OR 块组合”：以含符号片段为边界，
// 普通块沿用“整块 OR 分段 AND”，符号片段作为字面 filter，各块须同时满足。
// 块组合已覆盖外层分段 AND 的命中，省去该冗余路径，减少重复查询与评分。
// 块内保留原始空白与重复词，不跨符号片段拼接同义词；最多 16 块，重复块也计数。
// 超过块数上限或不满足混合规则时与 Query 完全一致。编号仍是保留符号的连续包含，不是完整值相等。
// 不枚举块内的词组切分，同一块内跨词同义词与部分词的任意组合仍不保证召回。
// 全文分支沿用字段的 search_analyzer；同义词规则由索引和词典决定，不另设查询开关。
//
// 去除首尾空白后为空时返回 nil, nil，应用应省略该条件，不能将 nil 作为 query 提交。
// 非法字段或 UTF-8 返回错误；不限制查询长度，应用按业务需求校验输入长度与请求规模。
// 返回值是独立的 map，可直接合并业务请求；两个入口均可并发调用，返回值由各调用方自行管理。
// 查询使用 ES 原生评分，混合路径的字面 filter 不计分；多条路径命中会累加普通文本评分。
// QueryExpanded 保留 Query 的命中并扩展召回，但仍有额外成本，不能保证分数、排名或 Top K 不变。
// 业务负责过滤、排序和分页；把整个子句放入 filter 可只做过滤。
//
// templates/ 中的 JSON 由人工独立维护，用于创建索引；Go API 不生成或读取模板。
// 模板使用说明见 templates/README.md，查询契约见 docs/estextsearch-design.md。
package estextsearch
