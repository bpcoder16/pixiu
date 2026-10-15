# estextsearch 索引模板

这里保存基于 `Profile.Definition()` 生成的静态 JSON 模板，将主字段搜索链统一为同义词分析器，增加
`es_update_at`，移除 `mappings._meta` 并设置 `dynamic: "strict"`，方便其他项目查看、复制和创建索引。

| 文件 | 配置档 | 依赖 |
| --- | --- | --- |
| [ik.json](ik.json) | `DefaultConfig(AnalyzerIK)` 加模板扩展 | 与 ES 版本匹配的 IK 插件、所有数据节点上的同义词文件 |
| [standard.json](standard.json) | `DefaultConfig(AnalyzerStandard)` 加模板扩展 | ES 内置分词器、所有数据节点上的同义词文件 |

两份文件的默认主字段均为 `query_text`，开启 `contains`、`contains_unigram`，关闭 `prefix`、
`exact`，不包含版本元数据与 schema 指纹。主字段保留 BM25；辅助字段由查询构造器用于召回。
选择与查询 Profile 相同的配置档。Prefix/Exact、自定义字段名等配置需要通过对应 Profile 重新生成。
`Profile.Definition()` 仍返回元数据，当前不生成同义词分析器和 `es_update_at`；
`SchemaFingerprint()` 只覆盖该方法生成的定义，不能代表静态模板的完整 analysis/mapping 或词典内容。

静态模板省略 `text` 字段的默认值 `index_options: "positions"`、`norms: true` 和
`position_increment_gap: 100`，保留 contains 子字段的非默认配置 `norms: false`。
当前 analyzer 未覆盖位置间隔，省略上述参数不改变索引行为；`Profile.Definition()` 仍显式返回这些参数，schema 指纹不变。

模板包含通用短文本定义，并配置 6 个主分片、1 个副本；分片和副本可按部署需要调整。
模板设置 `mappings.dynamic: "strict"`，写入未定义字段时 ES 会拒绝整条文档。
写入商品 ID、价格、状态等业务字段前，必须先在 `mappings.properties` 中显式定义；刷新周期、别名由项目按需补充。
`Profile.Definition()` 本身不指定 `dynamic`，通过 Go 创建索引时需显式设置该策略。
输入限额和单字符开关在 Go 侧执行，JSON mapping 本身不会代替 `Values` / `Query` 的校验。

`es_update_at` 使用 `date`，接受 `yyyy-MM-dd HH:mm:ss`、`strict_date_optional_time` 和
`epoch_millis`，显式设置 `index: false`、`doc_values: true`。该字段可用于排序与聚合；
mapping 不会自动填写或刷新时间，写入方负责赋值，字段也不是必填项。

## 同义词搜索

两份模板仅保留 `px_qt_v1_<配置档>_search_synonyms` 作为主字段的搜索分析器，删除原来的
`px_qt_v1_<配置档>_search`，并同步更新 `query_text.search_analyzer`。写入和 contains 分析链不使用同义词。
当前 Go `Query` 不指定全文 analyzer，会自动使用 mapping 中的搜索分析器，无需新增查询方法。
词典为空且其他分析配置相同时，主字段的分词效果与原搜索链一致；词典有规则时按规则处理查询。

新增分析链为 `ik_smart/standard → lowercase → px_qt_v1_synonyms`，过滤器采用
`synonym_graph`、`updateable: true` 和 `lenient: false`。显式关闭 lenient，避免无效规则被忽略。
`expand` 沿用默认的 `true`。词典使用 Solr 格式，例如：

```text
充电宝, 移动电源
pc, personal computer
```

创建索引前，必须在所有 ES 数据节点准备 `config/analysis/search-synonyms.txt`；
`synonyms_path` 相对于 ES 的 config 目录，不是 Go 程序本机路径。暂不配置规则时准备空词典，
不能省略文件；创建索引时仍需要加载文件。

查询沿用字段 mapping 中的搜索分析器，不传 analyzer 或同义词开关。同一请求适用于两份模板：

```json
{
  "query": {
    "match": {
      "query_text": {
        "query": "充电宝",
        "operator": "and",
        "zero_terms_query": "none"
      }
    }
  }
}
```

`synonym_graph` 支持多词规则，`match` 默认按短语处理多词同义词；同义词规则会经过对应的
分词与 lowercase，Standard 下可加载不代表 IK 下效果相同。参见
[同义词分析器](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/analysis-synonym-graph-tokenfilter.html) 与
[match 查询](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-dsl-match-query.html)。

词典发布前应使用实际分析链验证；更新所有数据节点的文件后，调用
`elasticsearchx.Client.ReloadSearchAnalyzers` 并核对返回的分析器和节点，该方法同时清理 request cache。
仅更新已接入的词典内容无需重建索引；旧索引首次接入这些分析器需要单独更新索引配置，不能仅调用 reload。
参见 [重载要求](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-reload-analyzers.html)。

FullText 将完整输入交给分析器；Hybrid 仍先按空白分段，同义词只作用于各普通片段的全文分支，
不支持跨片段的短语规则。Contains 保持字面匹配。空词典兼容原分词效果不代表任意规则都保留
原召回：`a => b` 会替换原词，需要保留时写 `a => a, b` 或使用双向等价规则。

本轮落地模板调整；Go Definition 的后续对齐与查询整体设计见 `docs/estextsearch-design.md` 第 14 节。

## 创建单个索引

文件顶层是 `settings` / `mappings`，可直接作为 `PUT /<index>` 请求体。以下命令从 Pixiu
仓库根目录执行；先替换 ES 地址和索引名，并为实际环境补充认证参数：

```bash
ES_URL='http://127.0.0.1:9200'
curl --fail-with-body -X PUT "$ES_URL/your-project-search-v1" \
  -H 'Content-Type: application/json' \
  --data-binary @infra/estextsearch/templates/ik.json
```

使用 Standard 时将文件名替换为 `standard.json`；使用 IK 前安装相应插件。两种配置都须先部署上述词典文件。

## 注册为 Elasticsearch 索引模板

`PUT /_index_template/<name>` 需要额外的 `index_patterns` 和外层 `template`，不能直接提交
上述文件。使用 jq 包装后注册；索引匹配规则和模板名称由项目决定，本例不设置 priority：

```bash
jq '{index_patterns: ["your-project-search-*"], version: 1, template: .}' \
  infra/estextsearch/templates/ik.json > /tmp/pixiu-estextsearch-index-template.json

curl --fail-with-body -X PUT "$ES_URL/_index_template/your-project-search-v1?create=true" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/pixiu-estextsearch-index-template.json
```

`create=true` 用于首次注册；更新已有模板时使用项目的模板发布流程。相同优先级的模板不能
匹配重叠的索引模式，应按项目实际情况选择一个配置档并安排优先级。模板变更只影响后续
新建索引；涉及字段索引内容的变更时，已有数据可能需要回填或重建。省略 `_meta` 本身
不要求重建。参见 [ES 索引模板 API](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-put-template.html)。

## 同步与重新生成

生成来源为当前包的 `DefaultConfig` 和 `Definition()`，替换主字段搜索分析器并增加 `es_update_at`，
输出前移除 `mappings._meta` 及上述默认参数，设置 `dynamic: "strict"` 并补充部署参数。
变更 analysis/mapping 后，应重新生成对应文件并按默认值补齐后的语义核对分词器与字段映射；仅变更查询策略版本无需更新这些静态模板。
复制到业务项目后，保持 `query_text` 的 analysis/mapping 与查询 Profile 配套，并另行核对部署参数与 IK 词典。

将以下程序保存到仓库目录外的临时 `.go` 文件，在 Pixiu 根目录运行 `go run <临时文件>`，
会重新生成这两份 JSON：

生成示例仅用于这两种默认配置。若启用 Prefix，须为 prefix 字段另配字面搜索链，避免
删除主字段旧分析器后留下悬空引用；不能直接复用同义词分析器改变前缀语义。

```go
package main

import (
    "encoding/json"
    "os"
    "path/filepath"

    "github.com/bpcoder16/pixiu/infra/estextsearch"
)

func main() {
    for _, analyzer := range []estextsearch.Analyzer{
        estextsearch.AnalyzerIK,
        estextsearch.AnalyzerStandard,
    } {
        profile, err := estextsearch.New(estextsearch.DefaultConfig(analyzer))
        if err != nil {
            panic(err)
        }
        definition := profile.Definition()
        delete(definition.Mappings, "_meta")
        definition.Mappings["dynamic"] = "strict"
        definition.Settings["number_of_shards"] = 6
        definition.Settings["number_of_replicas"] = 1
        analysis := definition.Settings["analysis"].(map[string]any)
        filters, ok := analysis["filter"].(map[string]any)
        if !ok {
            filters = make(map[string]any)
            analysis["filter"] = filters
        }
        filters["px_qt_v1_synonyms"] = map[string]any{
            "type":          "synonym_graph",
            "synonyms_path": "analysis/search-synonyms.txt",
            "updateable":    true,
            "lenient":       false,
        }
        searchTokenizer := "standard"
        if analyzer == estextsearch.AnalyzerIK {
            searchTokenizer = "ik_smart"
        }
        analyzers := analysis["analyzer"].(map[string]any)
        baseSearch := "px_qt_v1_" + string(analyzer) + "_search"
        delete(analyzers, baseSearch)
        analyzers[baseSearch+"_synonyms"] = map[string]any{
            "type":      "custom",
            "tokenizer": searchTokenizer,
            "filter":    []string{"lowercase", "px_qt_v1_synonyms"},
        }
        definition.Mappings["properties"].(map[string]any)["es_update_at"] = map[string]any{
            "type":       "date",
            "format":     "yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis",
            "index":      false,
            "doc_values": true,
        }
        main := definition.Mappings["properties"].(map[string]any)["query_text"].(map[string]any)
        main["search_analyzer"] = baseSearch + "_synonyms"
        fields := []map[string]any{main}
        for _, field := range main["fields"].(map[string]any) {
            fields = append(fields, field.(map[string]any))
        }
        // 默认配置档未自定义 analyzer 的位置间隔，只省略与 ES 默认值一致的参数。
        for _, field := range fields {
            for key, value := range map[string]any{
                "index_options":          "positions",
                "norms":                  true,
                "position_increment_gap": 100,
            } {
                if field[key] == value {
                    delete(field, key)
                }
            }
        }
        data, err := json.MarshalIndent(definition, "", "  ")
        if err != nil {
            panic(err)
        }
        path := filepath.Join("infra", "estextsearch", "templates", string(analyzer)+".json")
        if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
            panic(err)
        }
    }
}
```

基础 Profile schema 曾在 ES 7.17.17 的 Standard 和 IK 配置上通过功能集成用例；
新增的同义词模板、实际词典与重载流程尚未经过真实 ES 集成验证，不沿用该历史结论。
跨版本兼容性、业务质量与容量需由接入项目继续验收。详细设计见仓库本地的 `docs/estextsearch-design.md`。
