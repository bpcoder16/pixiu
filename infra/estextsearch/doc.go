// Package estextsearch 构建短文本检索的 Elasticsearch 查询子句，仅依赖标准库。
// 唯一构建入口为 Query(field, text)，无需初始化或配置对象，不执行网络请求。
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
// Query 使用主字段及固定后缀 .contains、.contains_unigram，调用方须确保这些字段存在。
// 普通文本采用全文 OR 连续包含；1～2 字符使用 unigram，至少 3 字符使用 trigram。
// 单字符默认支持；单字符或含标点、符号的输入只走包含，避免分词忽略关键字符。
// 包含使用 match_phrase、slop 0，不设 32 字符的分支切换，也不依赖 edge_ngram。
//
// 多片段采用“整句查询 OR 各片段全部命中”，整句保留内部空白并支持全文分支的跨词同义词。
// 按空白分段，仅去除完全相同的片段；超过 16 个不同片段时只保留整句，不截断输入。
// 全文分支沿用字段的 search_analyzer；同义词规则由索引和词典决定，不另设查询开关。
//
// 去除首尾空白后为空时返回 nil, nil，应用应省略该条件，不能将 nil 作为 query 提交。
// 非法字段或 UTF-8 返回错误；不限制查询长度，应用按业务需求校验输入长度与请求规模。
// 返回值是独立的 map，可直接合并业务请求；Query 可并发调用，返回值由各调用方自行管理。
// 查询使用 ES 原生评分，不单独给全文或包含设置奖励；业务负责过滤、排序和分页。
//
// templates/ 中的 JSON 由人工独立维护，用于创建索引；Go API 不生成或读取模板。
// 模板使用说明见 templates/README.md，查询契约见 docs/estextsearch-design.md。
package estextsearch
