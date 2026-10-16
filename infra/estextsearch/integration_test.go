package estextsearch_test

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// 显式设置 PIXIU_ESTEXTSEARCH_TEST_URL 后才访问 ES；只创建和清理随机命名的测试索引。
// PIXIU_ESTEXTSEARCH_TEST_ANALYZER 选择 standard（默认）或 ik 模板；IK 插件须预先安装。
// 所有数据节点须预先部署空的 config/analysis/search-synonyms.txt，避免词典改变固定样本召回。
func TestElasticsearchIntegration(t *testing.T) {
	fixtures := map[string][]string{
		"iphone":        {"iphone"},
		"hiphone":       {"hiphone"},
		"cherry":        {"cherry"},
		"continuous":    {"abcdef"},
		"grams_apart":   {"bcd x cde"},
		"array":         {"ab", "cd"},
		"single_space":  {"AB CD"},
		"double_space":  {"AB  CD"},
		"code":          {"apple", "PR-PV02"},
		"code_apart":    {"apple PR PV02"},
		"compound":      {"#1", "PR-PV02", "1箱1盒1包1张"},
		"chinese":       {"皮卡丘", "PR-PV02"},
		"emoji":         {"x😀😃😄y"},
		"repeated":      {"xxaaaay"},
		"not_repeated":  {"aaabaa"},
		"long":          {"x" + strings.Repeat("z", 70) + "y"},
		"punctuation":   {"---"},
		"array_symbols": {"AB-", "CD"},
	}
	queries := []integrationQueryCase{
		{
			text: "i",
			want: []string{"iphone", "hiphone"},
		},
		{
			text: "ip",
			want: []string{"iphone", "hiphone"},
		},
		{
			text: "iph",
			want: []string{"iphone", "hiphone"},
		},
		{
			text: "iph iph",
			want: []string{"iphone", "hiphone"},
		},
		{
			text: " \ti\u3000\ti\n",
			want: []string{"iphone", "hiphone"},
		},
		{
			text: "c",
			want: []string{"cherry", "continuous", "grams_apart", "array", "single_space", "double_space", "array_symbols"},
		},
		{
			text: "ch",
			want: []string{"cherry"},
		},
		{
			text: "bcde",
			want: []string{"continuous"},
		},
		{
			text: "AB-CD",
			want: []string{},
		},
		{
			text: "ab cd",
			want: []string{"continuous", "array", "array_symbols", "single_space", "double_space"},
		},
		{
			text: "PR-PV02",
			want: []string{"code", "compound", "chinese"},
		},
		{
			text: "PR-PV02 PR-PV02",
			want: []string{"code", "compound", "chinese"},
		},
		{
			text: "apple PR-PV02",
			want: []string{"code"},
		},
		{
			text: "#1 PR-PV02 1箱1盒1包1张",
			want: []string{"compound"},
		},
		{
			text: "皮卡丘 PR-PV02",
			want: []string{"chinese"},
		},
		{
			text: "😀😃😄",
			want: []string{"emoji"},
		},
		{
			text: "😃😄",
			want: []string{"emoji"},
		},
		{
			text: "aaaa",
			want: []string{"repeated"},
		},
		{
			text: "---",
			want: []string{"punctuation"},
		},
		{
			text: strings.Repeat("z", 33),
			want: []string{"long"},
		},
		{
			text: strings.Repeat("z", 65),
			want: []string{"long"},
		},
	}
	runElasticsearchIntegration(t, nil, fixtures, queries)
}

// 非空词典直接写入临时索引；通过 PIXIU_ESTEXTSEARCH_TEST_ANALYZER 分别运行 standard 和 ik。
// 保留两份模板的分析链及 mapping，不修改节点词典文件，也不验证词典热重载。
func TestElasticsearchSynonymsIntegration(t *testing.T) {
	fixtures := map[string][]string{
		"pc":            {"pc"},
		"phrase":        {"personal computer"},
		"pc_code":       {"pc", "PR-PV02"},
		"phrase_code":   {"personal computer", "PR-PV02"},
		"wrong_code":    {"personal computer", "PR-PV03"},
		"spaced_code":   {"personal computer", "PR PV02"},
		"code_only":     {"PR-PV02"},
		"personal_only": {"personal", "PR-PV02"},
		"computer_only": {"computer", "PR-PV02"},
		"words_apart":   {"personal portable computer"},
		"words_array":   {"personal", "computer"},
		"unrelated":     {"camera", "PR-PV02"},
	}
	queries := []integrationQueryCase{
		{
			// 整句同义词可命中 pc；分段 AND 仍允许原词分散出现。
			text: "personal computer",
			want: []string{"pc", "phrase", "pc_code", "phrase_code", "wrong_code", "spaced_code", "words_apart", "words_array"},
		},
		{
			// 反向展开要求完整短语，不能把非连续词或不同数组元素拼成同义词。
			text: "pc",
			want: []string{"pc", "phrase", "pc_code", "phrase_code", "wrong_code", "spaced_code"},
		},
		{
			// 分段中的 pc 仍可展开同义词，但错误编号、缺少连字符或只有编号均不能命中。
			text: "pc PR-PV02",
			want: []string{"pc_code", "phrase_code"},
		},
		{
			// Query 保留原能力；QueryExpanded 才组合跨词同义词与严格编号。
			text:         "personal computer PR-PV02",
			want:         []string{"phrase_code"},
			wantExpanded: []string{"pc_code", "phrase_code"},
		},
		{
			text: "pc PR-PV99",
			want: []string{},
		},
	}
	runElasticsearchIntegration(t, []string{"pc, personal computer"}, fixtures, queries)
}

func TestElasticsearchMixedIntegration(t *testing.T) {
	fixtures := map[string][]string{
		"alias":         {"pc", "PR-PV02"},
		"literal":       {"personal computer", "PR-PV02"},
		"whole":         {"personal computer PR-PV02"},
		"embedded":      {"pc", "XPR-PV02Y"},
		"wrong":         {"pc", "PR-PV03"},
		"spaced":        {"pc", "PR PV02"},
		"joined":        {"pc", "PRPV02"},
		"split_code":    {"pc", "PR-", "PV02"},
		"code_only":     {"PR-PV02"},
		"personal":      {"personal", "PR-PV02"},
		"computer":      {"computer", "PR-PV02"},
		"unrelated":     {"camera", "PR-PV02"},
		"apart":         {"personal portable computer", "PR-PV02"},
		"array":         {"personal", "computer", "PR-PV02"},
		"multiple":      {"pc", "handset", "PR-PV02", "#2"},
		"partial":       {"pc", "PR-PV02", "iphone"},
		"partial_full":  {"personal computer", "PR-PV02", "iphone"},
		"partial_block": {"pc", "PR-PV02", "mobilephone", "iphone"},
		"repeated":      {"strong", "PR-PV02"},
		"chinese":       {"ai", "A＋2"},
		"ascii_symbol":  {"ai", "A+2"},
	}
	all := []string{"whole", "alias", "literal", "embedded", "apart", "array", "multiple", "partial", "partial_full", "partial_block"}
	base := []string{"whole", "literal", "apart", "array", "partial_full"}
	queries := []integrationQueryCase{
		{
			text:         "personal computer PR-PV02",
			want:         base,
			wantExpanded: all,
			checkScores:  true,
		},
		{
			text:         "PR-PV02 personal computer",
			want:         base,
			wantExpanded: all,
		},
		{
			text:         " \tpersonal\u3000\tcomputer\npr-pv02 ",
			want:         base,
			wantExpanded: all,
		},
		{
			text:         "personal computer PR-PV02 PR-PV02",
			want:         base,
			wantExpanded: all,
		},
		{
			text:         "personal computer PR-PV02 personal computer",
			want:         base,
			wantExpanded: all,
		},
		{
			text:         "personal computer" + strings.Repeat(" PR-PV02", 15),
			want:         base,
			wantExpanded: all,
		},
		{
			text: "personal computer" + strings.Repeat(" PR-PV02", 16),
			want: base,
		},

		// 编号隔开的词不能重组为同义词；原分段 AND 仍可命中分散原词。
		{
			text: "personal PR-PV02 computer",
			want: base,
		},
		{
			text:         "personal computer PR-PV02 mobile phone #2",
			want:         []string{},
			wantExpanded: []string{"multiple"},
		},
		{
			text:         "#2 mobile phone PR-PV02 personal computer",
			want:         []string{},
			wantExpanded: []string{"multiple"},
		},
		{
			text: "personal computer PR-PV02 mobile phone #3",
			want: []string{},
		},
		{
			text: "personal computer PR-PV99",
			want: []string{},
		},
		{
			text:         "personal computer PR-PV02 iph",
			want:         []string{"partial_full"},
			wantExpanded: []string{"partial", "partial_full", "partial_block"},
		},
		// 一个块应用同义词，另一个块仍可通过分段包含满足全部条件。
		{
			text:         "personal computer PR-PV02 mobile iph",
			want:         []string{},
			wantExpanded: []string{"partial_block"},
		},
		// 不扩大为任意词组切分；同一块内同义词与部分词混用仍沿用现有能力边界。
		{
			text: "personal computer iph PR-PV02",
			want: []string{"partial_full"},
		},
		{
			text: "personal computer PR-PV02 camera",
			want: []string{},
		},
		{
			text:         "very very PR-PV02",
			want:         []string{},
			wantExpanded: []string{"repeated"},
		},
		{
			text: "very PR-PV02 very",
			want: []string{},
		},
		{
			text:         "人工 智能 A＋2",
			want:         []string{},
			wantExpanded: []string{"chinese"},
		},
		{
			text:         "A＋2 人工 智能",
			want:         []string{},
			wantExpanded: []string{"chinese"},
		},
	}
	runElasticsearchIntegration(t, []string{"pc, personal computer", "handset, mobile phone", "very very => strong", "ai, 人工 智能"}, fixtures, queries)
}

type integrationQueryCase struct {
	text         string
	want         []string
	wantExpanded []string // nil 表示两个入口的预期命中相同。
	checkScores  bool
}

func runElasticsearchIntegration(t *testing.T, synonyms []string, fixtures map[string][]string, queries []integrationQueryCase) {
	t.Helper()
	endpoint := strings.TrimRight(os.Getenv("PIXIU_ESTEXTSEARCH_TEST_URL"), "/")
	if endpoint == "" {
		t.Skip("未配置 PIXIU_ESTEXTSEARCH_TEST_URL，跳过真实 ES 验证")
	}
	analyzer := os.Getenv("PIXIU_ESTEXTSEARCH_TEST_ANALYZER")
	if analyzer == "" {
		analyzer = "standard"
	}
	if analyzer != "standard" && analyzer != "ik" {
		t.Fatalf("未知模板：%s", analyzer)
	}
	data, err := os.ReadFile(filepath.Join("templates", analyzer+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var template map[string]any
	if err := json.Unmarshal(data, &template); err != nil {
		t.Fatal(err)
	}
	// 使用模板原有分析链和 mapping；仅缩小部署规模，按需注入测试词典。
	settings := template["settings"].(map[string]any)
	settings["number_of_shards"] = 1
	settings["number_of_replicas"] = 0
	if synonyms != nil {
		// 内联词典仅用于临时索引，避免节点文件内容影响非空同义词样本。
		analysis := settings["analysis"].(map[string]any)
		filter := analysis["filter"].(map[string]any)["px_qt_v1_synonyms"].(map[string]any)
		delete(filter, "synonyms_path")
		filter["synonyms"] = synonyms
	}

	client := &http.Client{Timeout: 30 * time.Second}
	t.Cleanup(client.CloseIdleConnections)
	request := func(method, path string, value any) ([]byte, int, error) {
		var body io.Reader
		if value != nil {
			data, err := json.Marshal(value)
			if err != nil {
				return nil, 0, err
			}
			body = bytes.NewReader(data)
		}
		req, err := http.NewRequest(method, endpoint+path, body)
		if err != nil {
			return nil, 0, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, err
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
		return data, resp.StatusCode, err
	}
	mustRequest := func(t *testing.T, method, path string, value any) []byte {
		t.Helper()
		data, status, err := request(method, path, value)
		if err != nil || status < 200 || status >= 300 {
			t.Fatalf("%s %s: status %d, %v, %s", method, path, status, err, data)
		}
		return data
	}
	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	index := "/pixiu-estextsearch-test-" + hex.EncodeToString(random[:])
	// 创建失败时不注册清理，避免碰巧同名时删除其他任务的索引。
	mustRequest(t, "PUT", index, template)
	t.Cleanup(func() {
		data, status, err := request("DELETE", index, nil)
		if err != nil || status != http.StatusOK {
			t.Errorf("清理测试索引: status %d, %v, %s", status, err, data)
		}
	})
	for id, parts := range fixtures {
		mustRequest(t, "PUT", index+"/_doc/"+id, map[string]any{"query_text": parts})
	}
	mustRequest(t, "POST", index+"/_refresh", nil)
	search := func(t *testing.T, clause map[string]any) map[string]float64 {
		t.Helper()
		data := mustRequest(t, "POST", index+"/_search", map[string]any{
			"query": clause,
			"size":  100,
		})
		var result struct {
			TimedOut bool `json:"timed_out"`
			Shards   struct {
				Failed int `json:"failed"`
			} `json:"_shards"`
			Hits struct {
				Hits []struct {
					ID    string  `json:"_id"`
					Score float64 `json:"_score"`
				} `json:"hits"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			t.Fatal(err)
		}
		if result.TimedOut || result.Shards.Failed != 0 {
			t.Fatalf("搜索未完整完成: %s", data)
		}
		hits := make(map[string]float64, len(result.Hits.Hits))
		for _, hit := range result.Hits.Hits {
			hits[hit.ID] = hit.Score
		}
		return hits
	}
	assertHits := func(t *testing.T, hits map[string]float64, want []string) {
		t.Helper()
		ids := make([]string, 0, len(hits))
		for id := range hits {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		expected := append([]string(nil), want...)
		sort.Strings(expected)
		if strings.Join(ids, ",") != strings.Join(expected, ",") {
			t.Fatalf("召回 = %v，期望 %v", ids, expected)
		}
	}
	for _, tc := range queries {
		t.Run(tc.text, func(t *testing.T) {
			base := mustQuery(t, "query_text", tc.text)
			expanded := mustQueryExpanded(t, "query_text", tc.text)
			wantExpanded := tc.wantExpanded
			if wantExpanded == nil {
				wantExpanded = tc.want
			}
			baseHits := search(t, base)
			hits := search(t, expanded)
			t.Run("Query", func(t *testing.T) { assertHits(t, baseHits, tc.want) })
			t.Run("QueryExpanded", func(t *testing.T) { assertHits(t, hits, wantExpanded) })
			if reflect.DeepEqual(base, expanded) {
				return
			}
			// 重建优化前的三路径查询，在同一索引验证去掉外层分段 AND 不丢召回。
			baseShould := base["bool"].(map[string]any)["should"].([]any)
			expandedShould := expanded["bool"].(map[string]any)["should"].([]any)
			unoptimized := map[string]any{"bool": map[string]any{
				"should":               []any{baseShould[0], baseShould[1], expandedShould[1]},
				"minimum_should_match": 1,
			}}
			before := search(t, unoptimized)
			assertHits(t, before, wantExpanded)
			for id := range baseHits {
				if _, ok := hits[id]; !ok {
					t.Fatalf("扩展查询丢失基础命中：%s", id)
				}
			}
			if tc.checkScores {
				if len(baseHits) == 0 || len(baseHits) >= len(hits) {
					t.Fatalf("评分样本应覆盖基础和新增命中：基础 %v，扩展 %v", baseHits, hits)
				}
				for id, score := range hits {
					if !(score > 0) {
						t.Fatalf("扩展路径的普通文本仍应参与评分：%s = %g", id, score)
					}
					if _, ok := baseHits[id]; ok {
						if before[id] <= score {
							t.Fatalf("去重应减少该样本的重复评分：%s，去重前 %g，去重后 %g", id, before[id], score)
						}
					} else if math.Abs(before[id]-score) > 1e-5*math.Max(1, score) {
						t.Fatalf("仅命中混合路径的样本评分应不变：%s，去重前 %g，去重后 %g", id, before[id], score)
					}
				}
				filtered := search(t, map[string]any{"bool": map[string]any{"filter": []any{expanded}}})
				assertHits(t, filtered, wantExpanded)
				for id, score := range filtered {
					if score != 0 {
						t.Fatalf("filter 不应计分：%s = %g", id, score)
					}
				}
				t.Logf("同索引评分：Query %v；扩展去重前 %v；QueryExpanded %v；整体 filter 全部为 0", baseHits, before, hits)
			}
		})
	}
}
