# estextsearch 索引模板

`ik.json` 和 `standard.json` 是独立维护的索引定义，直接编辑 JSON 后创建索引。
Go 模块仅构建查询 DSL，不生成、读取或校验这些模板，也不处理数据写入。

| 文件 | 主字段分词方式 | 依赖 |
| --- | --- | --- |
| [ik.json](ik.json) | 索引使用 ik_max_word，搜索使用 ik_smart 与同义词 | 匹配 ES 版本的 IK 插件、所有数据节点上的同义词文件 |
| [standard.json](standard.json) | 索引和搜索使用 standard，搜索增加同义词 | ES 内置分词器、所有数据节点上的同义词文件 |

两份模板默认主字段均为 `query_text`，包含 `contains`、`contains_unigram`，不包含 `prefix`、
`exact` 或 `_meta`。使用 `estextsearch.Query("query_text", text)` 构建查询，无需初始化或选择模式。
全文和包含都使用 ES 原生评分；过滤、排序和分页由业务决定。

模板省略 `text` 字段默认值 `index_options: "positions"`、`norms: true` 和
`position_increment_gap: 100`，保留 contains 子字段的非默认配置 `norms: false`。

模板配置 6 个主分片、1 个副本，可按部署需要修改。`mappings.dynamic` 为 `"strict"`，
业务字段须先在 `mappings.properties` 中显式定义；刷新周期和别名由项目补充。

`es_update_at` 使用 `date`，接受 `yyyy-MM-dd HH:mm:ss`、`strict_date_optional_time` 和
`epoch_millis`，显式设置 `index: false`、`doc_values: true`，可用于排序与聚合。
时间值由应用写入，mapping 不会自动填写或刷新。

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

Query 的全文分支沿用字段 mapping 中的搜索分析器，不传 analyzer 或同义词开关。
以下展示全文分支的请求形式；实际返回的子句还包含字面匹配与适用的分段组合：

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

Query 保留完整普通文本的全文分支，因此可应用跨词同义词规则，同时增加分段 AND 作为召回路径。
单字符或含标点、符号的输入只做字面包含；包含分支不使用同义词。
例如单向规则 `mobile => phone` 会替换全文分支的原词，字面包含分支仍可能命中 mobile；
最终召回由全文与包含组合决定，应使用业务样本验证。查询契约见包文档及 `docs/estextsearch-design.md`。

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

## 手工调整与接入

修改主字段名时，同时修改 JSON 的 `mappings.properties` 和 `Query` 的第一个参数。
Query 固定使用主字段、`.contains` 和 `.contains_unigram`，须保留对应字段及配套的分析链。
同一套查询可用于两份模板，不需要 Go 层的分词器配置或能力开关。

单字符默认支持；`i`、`ip`、`iph` 通过包含字段召回，不需要额外的 prefix 字段。
查询不设置输入长度上限，业务在调用前校验搜索框长度和请求规模。不同片段超过 16 个时只保留整句查询。

修改模板后，按业务需要发布新索引或变更已有索引配置，并在真实 ES 上验证实际召回与评分。
数据清洗、写入和回填均由应用负责；模板由人工维护，不通过 Go 生成。

## 测试范围

Go 单元测试验证 DSL、非法输入、整句/分段组合与并发隔离，不做模板生成一致性比较。
可选 ES 集成测试直接读取模板，覆盖分片为 1、副本为 0 后创建临时索引，验证查询召回并清理索引。
测试环境须提前在所有数据节点准备空的 `config/analysis/search-synonyms.txt`；非空词典会改变固定样本的预期召回。

当前重写后的查询与模板组合、实际同义词及重载流程仍需真实环境验收，不沿用旧方案的历史结论。
