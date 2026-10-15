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
	got = mustQuery(t, "query_text", "apple apple")
	assertJSON(t, got, `{"bool":{"should":[{"match":{"query_text":{"query":"apple apple","operator":"and"}}},{"match_phrase":{"query_text.contains":{"query":"apple apple","slop":0}}}],"minimum_should_match":1}}`)
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
	for _, text := range []string{"", " \t\n\u3000"} {
		got, err := estextsearch.Query("query_text", text)
		if err != nil || got != nil {
			t.Fatalf("空白查询应返回 nil, nil：%v，%v", got, err)
		}
	}
	for _, field := range []string{"", " ", "query text", "query_text.*", "query?", "title\x00", "\xff"} {
		got, err := estextsearch.Query(field, "private search")
		if got != nil || !errors.Is(err, estextsearch.ErrInvalidField) || strings.Contains(err.Error(), "private search") {
			t.Fatalf("非法字段应报错且不泄漏查询：%v，%v", got, err)
		}
	}
	got, err := estextsearch.Query("query_text", "private\xff")
	if got != nil || !errors.Is(err, estextsearch.ErrInvalidUTF8) || strings.Contains(err.Error(), "private") {
		t.Fatalf("非法 UTF-8 应报错且不泄漏查询：%v，%v", got, err)
	}
	text := `a"},"match_all":{}\\`
	clause := mustQuery(t, "query_text", text)
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
}

func TestQueryResultsAreIndependentAndConcurrent(t *testing.T) {
	want, err := json.Marshal(mustQuery(t, "query_text", "apple iph"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				got, err := estextsearch.Query("query_text", "apple iph")
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
				whole := should[0].(map[string]any)["bool"].(map[string]any)["should"].([]any)
				whole[0].(map[string]any)["match"].(map[string]any)["query_text"].(map[string]any)["query"] = "changed"
			}
		}()
	}
	wg.Wait()
	got, err := json.Marshal(mustQuery(t, "query_text", "apple iph"))
	if err != nil || string(got) != string(want) {
		t.Fatalf("修改返回值污染了后续查询：%s，%v", got, err)
	}
}
