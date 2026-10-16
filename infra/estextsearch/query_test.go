package estextsearch_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/bpcoder16/pixiu/infra/estextsearch"
)

func assertJSON(t *testing.T, got map[string]any, want string) {
	t.Helper()
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var actual, expected any
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &expected); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("DSL = %s，期望 %s", encoded, want)
	}
}

func mustQuery(t *testing.T, field, text string) map[string]any {
	t.Helper()
	query, err := estextsearch.Query(field, text)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func mustQueryExpanded(t *testing.T, field, text string) map[string]any {
	t.Helper()
	query, err := estextsearch.QueryExpanded(field, text)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func TestQuerySingleInput(t *testing.T) {
	for _, tc := range []struct {
		name string
		text string
		want string
	}{
		{
			name: "单字符默认支持",
			text: " i ",
			want: `{"match_phrase":{"query_text.contains_unigram":{"query":"i","slop":0}}}`,
		},
		{
			name: "双字符全文或包含",
			text: "ip",
			want: `{"bool":{"should":[{"match":{"query_text":{"query":"ip","operator":"and"}}},{"match_phrase":{"query_text.contains_unigram":{"query":"ip","slop":0}}}],"minimum_should_match":1}}`,
		},
		{
			name: "三字符全文或包含",
			text: "iph",
			want: `{"bool":{"should":[{"match":{"query_text":{"query":"iph","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"iph","slop":0}}}],"minimum_should_match":1}}`,
		},
		{
			name: "按字符数处理中文",
			text: "宝可",
			want: `{"bool":{"should":[{"match":{"query_text":{"query":"宝可","operator":"and"}}},{"match_phrase":{"query_text.contains_unigram":{"query":"宝可","slop":0}}}],"minimum_should_match":1}}`,
		},
		{
			name: "短符号只做包含",
			text: "#2",
			want: `{"match_phrase":{"query_text.contains_unigram":{"query":"#2","slop":0}}}`,
		},
		{
			name: "型号保留标点和大小写",
			text: "PR-PV02",
			want: `{"match_phrase":{"query_text.contains":{"query":"PR-PV02","slop":0}}}`,
		},
		{
			name: "emoji 按字符数路由",
			text: "😀😃",
			want: `{"match_phrase":{"query_text.contains_unigram":{"query":"😀😃","slop":0}}}`,
		},
		{
			name: "全角符号不会被忽略",
			text: "A＋2",
			want: `{"match_phrase":{"query_text.contains":{"query":"A＋2","slop":0}}}`,
		},
		{
			name: "不解释通配符",
			text: "a*",
			want: `{"match_phrase":{"query_text.contains_unigram":{"query":"a*","slop":0}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertJSON(t, mustQuery(t, "query_text", tc.text), tc.want)
		})
	}
	for _, field := range []string{"title", "product.query_text", "标题"} {
		got := mustQuery(t, field, "c")
		assertJSON(t, got, fmt.Sprintf(`{"match_phrase":{%q:{"query":"c","slop":0}}}`, field+".contains_unigram"))
	}
}

func TestQueryPreservesWholeAndSegmentedBranches(t *testing.T) {
	got := mustQuery(t, "query_text", " \t#1 PR-PV02 1箱1盒1包1张\u3000")
	assertJSON(t, got, `{"bool":{"should":[
		{"match_phrase":{"query_text.contains":{"query":"#1 PR-PV02 1箱1盒1包1张","slop":0}}},
		{"bool":{"must":[
			{"match_phrase":{"query_text.contains_unigram":{"query":"#1","slop":0}}},
			{"match_phrase":{"query_text.contains":{"query":"PR-PV02","slop":0}}},
			{"bool":{"should":[
				{"match":{"query_text":{"query":"1箱1盒1包1张","operator":"and"}}},
				{"match_phrase":{"query_text.contains":{"query":"1箱1盒1包1张","slop":0}}}
			],"minimum_should_match":1}}
		]}}
	],"minimum_should_match":1}}`)

	// 整句全文分支保留跨词同义词的输入；不在 DSL 中指定或展开分析器。
	got = mustQuery(t, "query_text", "personal computer")
	assertJSON(t, got, `{"bool":{"should":[
		{"bool":{"should":[
			{"match":{"query_text":{"query":"personal computer","operator":"and"}}},
			{"match_phrase":{"query_text.contains":{"query":"personal computer","slop":0}}}
		],"minimum_should_match":1}},
		{"bool":{"must":[
			{"bool":{"should":[{"match":{"query_text":{"query":"personal","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"personal","slop":0}}}],"minimum_should_match":1}},
			{"bool":{"should":[{"match":{"query_text":{"query":"computer","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"computer","slop":0}}}],"minimum_should_match":1}}
		]}}
	],"minimum_should_match":1}}`)
}

func TestQueryDeduplicatesOnlyIdenticalSegments(t *testing.T) {
	for _, tc := range []struct {
		text     string
		segments []string
	}{
		{
			text:     "Prizm Prizm Hobby",
			segments: []string{"Prizm", "Hobby"},
		},
		{
			text:     "Apple apple Apple",
			segments: []string{"Apple", "apple"},
		},
		{
			text:     "Kapple Kapple",
			segments: []string{"Kapple", "Kapple"},
		},
		{
			text:     "apple  \tiph",
			segments: []string{"apple", "iph"},
		},
	} {
		got := mustQuery(t, "query_text", tc.text)
		should := got["bool"].(map[string]any)["should"].([]any)
		whole := should[0].(map[string]any)["bool"].(map[string]any)["should"].([]any)
		if whole[0].(map[string]any)["match"].(map[string]any)["query_text"].(map[string]any)["query"] != tc.text {
			t.Fatal("整句必须保留原始内部空白与重复词")
		}
		must := should[1].(map[string]any)["bool"].(map[string]any)["must"].([]any)
		if len(must) != len(tc.segments) {
			t.Fatalf("分段数量 = %d，期望 %d", len(must), len(tc.segments))
		}
		for i, segment := range tc.segments {
			paths := must[i].(map[string]any)["bool"].(map[string]any)["should"].([]any)
			if paths[0].(map[string]any)["match"].(map[string]any)["query_text"].(map[string]any)["query"] != segment {
				t.Fatalf("分段 %d 丢失原文", i)
			}
		}
	}
}

func TestQueryRepeatedSegmentKeepsWholeAndSingleSegment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		text    string
		whole   string
		segment string
	}{
		{
			name:    "重复编号保留字面包含",
			text:    "PR-PV02 PR-PV02",
			whole:   `{"match_phrase":{"query_text.contains":{"query":"PR-PV02 PR-PV02","slop":0}}}`,
			segment: `{"match_phrase":{"query_text.contains":{"query":"PR-PV02","slop":0}}}`,
		},
		{
			name:    "重复部分词保留全文和包含",
			text:    "iph iph",
			whole:   `{"bool":{"should":[{"match":{"query_text":{"query":"iph iph","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"iph iph","slop":0}}}],"minimum_should_match":1}}`,
			segment: `{"bool":{"should":[{"match":{"query_text":{"query":"iph","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"iph","slop":0}}}],"minimum_should_match":1}}`,
		},
		{
			name:    "重复单字保留整句内部空白",
			text:    " \ti\u3000\ti\n",
			whole:   `{"bool":{"should":[{"match":{"query_text":{"query":"i　\ti","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"i　\ti","slop":0}}}],"minimum_should_match":1}}`,
			segment: `{"match_phrase":{"query_text.contains_unigram":{"query":"i","slop":0}}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertJSON(t, mustQuery(t, "query_text", tc.text), fmt.Sprintf(`{"bool":{"should":[%s,{"bool":{"must":[%s]}}],"minimum_should_match":1}}`, tc.whole, tc.segment))
		})
	}
}

func TestQueryExpandedPreservesContextWithoutRedundantBranch(t *testing.T) {
	for _, tc := range []struct {
		text     string
		ordinary []string
		literal  []string
	}{
		{
			text:     "personal computer PR-PV02",
			ordinary: []string{"personal computer"},
			literal:  []string{"PR-PV02"},
		},
		{
			text:     "PR-PV02 personal computer",
			ordinary: []string{"personal computer"},
			literal:  []string{"PR-PV02"},
		},
		{
			text:     "personal computer PR-PV02 mobile phone #2 iph",
			ordinary: []string{"personal computer", "mobile phone", "iph"},
			literal:  []string{"PR-PV02", "#2"},
		},
		{
			text:     " \t#2 personal\u3000\tcomputer A＋2 ",
			ordinary: []string{"personal\u3000\tcomputer"},
			literal:  []string{"#2", "A＋2"},
		},
		{
			text:     "very very PR-PV02",
			ordinary: []string{"very very"},
			literal:  []string{"PR-PV02"},
		},
	} {
		t.Run(tc.text, func(t *testing.T) {
			text := strings.TrimSpace(tc.text)
			got := mustQuery(t, "query_text", tc.text)
			should := got["bool"].(map[string]any)["should"].([]any)
			if len(should) != 2 {
				t.Fatalf("Query 只应保留整句和分段两条原路径：%v", got)
			}
			assertJSON(t, should[0].(map[string]any), fmt.Sprintf(`{"match_phrase":{"query_text.contains":{"query":%q,"slop":0}}}`, text))
			var original []any
			seen := make(map[string]bool)
			for _, part := range strings.Fields(text) {
				if !seen[part] {
					original = append(original, mustQuery(t, "query_text", part))
					seen[part] = true
				}
			}
			if !reflect.DeepEqual(should[1], map[string]any{"bool": map[string]any{"must": original}}) {
				t.Fatal("原分段路径被改写")
			}
			expanded := mustQueryExpanded(t, "query_text", tc.text)
			expandedShould := expanded["bool"].(map[string]any)["should"].([]any)
			if len(expandedShould) != 2 || !reflect.DeepEqual(expandedShould[0], should[0]) {
				t.Fatalf("QueryExpanded 应保留整句，且不重复保留外层分段路径：%v", expanded)
			}
			var must, filter []any
			for _, part := range tc.ordinary {
				must = append(must, mustQuery(t, "query_text", part))
			}
			for _, part := range tc.literal {
				filter = append(filter, mustQuery(t, "query_text", part))
			}
			if !reflect.DeepEqual(expandedShould[1], map[string]any{"bool": map[string]any{"must": must, "filter": filter}}) {
				t.Fatalf("扩展路径应保留连续文本上下文，且字面条件只参与过滤：%v", expandedShould[1])
			}
		})
	}
}

func TestQueryExpandedFallbackMatchesQuery(t *testing.T) {
	for _, text := range []string{"i", "ip", "iph", "宝可", "😀😃", "A＋2", "a*", "personal computer", "pc PR-PV02", "personal PR-PV02 computer", "#1 PR-PV02", "PR-PV02 PR-PV02", "iph iph", " \ti\u3000\ti\n", strings.Repeat("甲", 512)} {
		if !reflect.DeepEqual(mustQueryExpanded(t, "query_text", text), mustQuery(t, "query_text", text)) {
			t.Fatalf("无需块组合的输入应与 Query 完全一致：%q", text)
		}
	}
	for _, count := range []int{16, 17} {
		// 重复字面片段不消耗原分段预算，但新增块数也必须有界。
		for _, text := range []string{
			"personal computer" + strings.Repeat(" #1", count-1),
			strings.Repeat("#1 ", count-1) + "personal computer",
		} {
			got := mustQueryExpanded(t, "query_text", text)
			should := got["bool"].(map[string]any)["should"].([]any)
			if len(should) != 2 {
				t.Fatalf("%d 块的路径数 = %d，期望 2", count, len(should))
			}
			if count == 16 {
				filter := should[1].(map[string]any)["bool"].(map[string]any)["filter"].([]any)
				if len(filter) != 15 {
					t.Fatal("16 块应保留混合路径的全部字面约束")
				}
			} else if !reflect.DeepEqual(got, mustQuery(t, "query_text", text)) {
				t.Fatal("超出块数预算应回退 Query，不丢弃条件")
			}
		}
	}
	parts := []string{"personal", "computer", "#1"}
	for i := 0; i < 14; i++ {
		parts = append(parts, fmt.Sprintf("word%d", i))
	}
	text := strings.Join(parts, " ")
	assertJSON(t, mustQueryExpanded(t, "query_text", text), fmt.Sprintf(`{"match_phrase":{"query_text.contains":{"query":%q,"slop":0}}}`, text))
	if !reflect.DeepEqual(mustQueryExpanded(t, "query_text", text), mustQuery(t, "query_text", text)) {
		t.Fatal("超出不同片段预算应与 Query 完全一致")
	}
}

func TestQuerySegmentLimitKeepsCompleteInput(t *testing.T) {
	parts := make([]string, 17)
	for i := range parts {
		parts[i] = fmt.Sprintf("part%02d", i)
	}
	for _, count := range []int{16, 17} {
		text := strings.Join(parts[:count], " ")
		got := mustQuery(t, "query_text", text)
		should := got["bool"].(map[string]any)["should"].([]any)
		if count == 16 {
			must := should[1].(map[string]any)["bool"].(map[string]any)["must"].([]any)
			if len(must) != 16 {
				t.Fatalf("16 个不同片段应保留分段路径：%v", got)
			}
		} else {
			assertJSON(t, got, fmt.Sprintf(`{"bool":{"should":[{"match":{"query_text":{"query":%q,"operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":%q,"slop":0}}}],"minimum_should_match":1}}`, text, text))
		}
	}
	text := strings.Join(parts[:16], " ") + strings.Repeat(" part00", 20)
	got := mustQuery(t, "query_text", text)
	should := got["bool"].(map[string]any)["should"].([]any)
	if len(should[1].(map[string]any)["bool"].(map[string]any)["must"].([]any)) != 16 {
		t.Fatal("重复片段不应消耗分段预算")
	}
}

func TestQueryLongInputStillUsesContains(t *testing.T) {
	for _, size := range []int{32, 33, 64, 65, 512} {
		text := strings.Repeat("甲", size)
		got := mustQuery(t, "query_text", text)
		assertJSON(t, got, fmt.Sprintf(`{"bool":{"should":[{"match":{"query_text":{"query":%q,"operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":%q,"slop":0}}}],"minimum_should_match":1}}`, text, text))
	}
	text := strings.Repeat("A-", 40)
	assertJSON(t, mustQuery(t, "query_text", text), fmt.Sprintf(`{"match_phrase":{"query_text.contains":{"query":%q,"slop":0}}}`, text))
}

func TestQueryValidationAndComposition(t *testing.T) {
	for name, build := range map[string]func(string, string) (map[string]any, error){
		"Query":         estextsearch.Query,
		"QueryExpanded": estextsearch.QueryExpanded,
	} {
		t.Run(name, func(t *testing.T) {
			for _, text := range []string{"", " \t\n\u3000"} {
				got, err := build("query_text", text)
				if err != nil || got != nil {
					t.Fatalf("空白查询应返回 nil, nil：%v，%v", got, err)
				}
			}
			for _, field := range []string{"", " ", "query text", "query_text.*", "query?", "title\x00", "\xff"} {
				got, err := build(field, "private search")
				if got != nil || !errors.Is(err, estextsearch.ErrInvalidField) || strings.Contains(err.Error(), "private search") {
					t.Fatalf("非法字段应报错且不泄漏查询：%v，%v", got, err)
				}
			}
			got, err := build("query_text", "private\xff")
			if got != nil || !errors.Is(err, estextsearch.ErrInvalidUTF8) || strings.Contains(err.Error(), "private") {
				t.Fatalf("非法 UTF-8 应报错且不泄漏查询：%v，%v", got, err)
			}
			text := `a"},"match_all":{}\\`
			clause, err := build("query_text", text)
			if err != nil {
				t.Fatal(err)
			}
			body := map[string]any{"query": map[string]any{"bool": map[string]any{"must": []any{clause}}}}
			encoded, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			var decoded map[string]any
			if err := json.Unmarshal(encoded, &decoded); err != nil {
				t.Fatal(err)
			}
			must := decoded["query"].(map[string]any)["bool"].(map[string]any)["must"].([]any)
			literal := must[0].(map[string]any)["match_phrase"].(map[string]any)["query_text.contains"].(map[string]any)["query"]
			if literal != text {
				t.Fatal("用户内容必须作为普通字符串嵌入 DSL")
			}
		})
	}
}

func TestQueryResultsAreIndependentAndConcurrent(t *testing.T) {
	for name, build := range map[string]func(string, string) (map[string]any, error){
		"Query":         estextsearch.Query,
		"QueryExpanded": estextsearch.QueryExpanded,
	} {
		t.Run(name, func(t *testing.T) {
			initial, err := build("query_text", "personal computer PR-PV02")
			if err != nil {
				t.Fatal(err)
			}
			want, err := json.Marshal(initial)
			if err != nil {
				t.Fatal(err)
			}
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 20; j++ {
						got, err := build("query_text", "personal computer PR-PV02")
						if err != nil {
							t.Error(err)
							return
						}
						encoded, err := json.Marshal(got)
						if err != nil || string(encoded) != string(want) {
							t.Errorf("并发查询结果改变：%s，%v", encoded, err)
							return
						}
						should := got["bool"].(map[string]any)["should"].([]any)
						whole := should[0].(map[string]any)["match_phrase"].(map[string]any)["query_text.contains"].(map[string]any)
						whole["query"] = "changed"
					}
				}()
			}
			wg.Wait()
			final, err := build("query_text", "personal computer PR-PV02")
			if err != nil {
				t.Fatal(err)
			}
			got, err := json.Marshal(final)
			if err != nil || string(got) != string(want) {
				t.Fatalf("修改返回值污染了后续查询：%s，%v", got, err)
			}
		})
	}
}
