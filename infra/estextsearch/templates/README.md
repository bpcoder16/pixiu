# estextsearch 索引模板

`ik.json` 和 `standard.json` 是独立维护的索引定义，直接编辑 JSON 后创建索引。
Go 模块仅构建查询 DSL，不生成、读取或校验这些模板，也不处理数据写入。

| 文件 | 主字段分词方式 | 依赖 |
| --- | --- | --- |
| [ik.json](ik.json) | 索引使用 ik_max_word，搜索使用 ik_smart 与同义词 | 匹配 ES 版本的 IK 插件、所有数据节点上的同义词文件 |
| [standard.json](standard.json) | 索引和搜索使用 standard，搜索增加同义词 | ES 内置分词器、所有数据节点上的同义词文件 |

两份模板默认主字段均为 `query_text`，包含 `contains`、`contains_unigram`，不包含 `prefix`、
`exact` 或 `_meta`。两个查询入口使用同一套模板，无需初始化或配置对象：

| 入口 | 能力与选择 |
| --- | --- |
| `estextsearch.Query("query_text", text)` | 保留整句 OR 分段 AND 的基础能力，包含重复关键词修复 |
| `estextsearch.QueryExpanded("query_text", text)` | 扩展跨词同义词与编号组合的召回，省去冗余外层分段路径；额外成本由调用方评估 |

全文和包含沿用 ES 原生评分；混合路径中的字面约束使用 filter、不计分。过滤、排序和分页由业务决定。

模板省略 `text` 字段默认值 `index_options: "positions"`、`norms: true` 和
`position_increment_gap: 100`，保留 contains 子字段的非默认配置 `norms: false`。

模板配置 6 个主分片、1 个副本，可按部署需要修改。`mappings.dynamic` 为 `"strict"`，
业务字段须先在 `mappings.properties` 中显式定义；刷新周期和别名由项目补充。

`es_update_at` 使用 `date`，接受 `yyyy-MM-dd HH:mm:ss`、`strict_date_optional_time` 和
`epoch_millis`，显式设置 `index: false`、`doc_values: true`，可用于排序与聚合。
时间值由应用写入，mapping 不会自动填写或刷新。

## 如何选择查询与评分方式

| 使用场景 | 建议 |
| --- | --- |
| 普通关键词、编号、部分词查询为主，或优先控制资源成本 | 选择 `Query`，保留基础召回和重复关键词修复 |
| 已配置跨词同义词，且经常与编号等符号片段混合查询，需要补齐这类漏召回 | 选择 `QueryExpanded`，接受额外 ES 成本并验证排序效果 |
| 需要文本相关性分数参与排序、评分阈值或其他评分逻辑 | 将所选入口的返回子句放入 `query` 或 `bool.must` |
| 只按时间、价格等业务字段排序，不依赖文本分数或评分阈值 | 将所选入口的整个返回子句放入 `bool.filter`，避免该子句参与评分 |

入口选择与是否评分是两个独立决定，`Query` 和 `QueryExpanded` 都可以放入 filter。
例如 `personal computer` 单独查询已可由 Query 应用词典；配置 `pc, personal computer` 后，
需要用 `personal computer PR-PV02` 同时召回 `pc` 和严格编号时，才体现 QueryExpanded 的组合优势。

整体放入 filter 不改变该子句的匹配条件、同义词能力或编号约束，只改变评分上下文；
该子句不贡献分数，只有 filter 的查询 `_score` 为 0。若其他子句参与评分，总分仍可非零；
依赖文本分数的排序、`min_score` 或评分逻辑不能据此视为等价。
返回 nil 时仍省略文本条件。缓存由 ES 根据查询历史、索引分段等条件决定，不保证每次命中；
放入 filter 不等于自动选用基础 Query，也不表示检索没有成本。

## 同义词搜索

两份模板仅保留 `px_qt_v1_<配置档>_search_synonyms` 作为主字段的搜索分析器，删除原来的
`px_qt_v1_<配置档>_search`，并同步更新 `query_text.search_analyzer`。写入和 contains 分析链不使用同义词。
两个 Go 查询方法均不指定全文 analyzer，会自动使用 mapping 中的搜索分析器。
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

两个入口的全文分支沿用字段 mapping 中的搜索分析器，不传 analyzer 或同义词开关。
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
单段中的单字符或含标点、符号的文本只做字面包含；包含分支不使用同义词。
`QueryExpanded` 在混合输入含连续多词普通块时采用整句 OR 块组合：普通块沿用整块查询 OR 分段 AND，
符号片段独立做字面 filter，各块须同时满足。块内保留原始空白和重复词，不跨符号片段拼接。
块组合已经覆盖原分段 AND 的全部命中，因此不再构造外层分段路径，减少重复匹配与评分。
例如扩展入口的 `personal computer PR-PV02` 可以命中 `pc` 与该编号；基础入口仍保留此前的能力边界。
`personal PR-PV02 computer`
不会把编号两侧的词拼成同义词，但仍保留原词分段匹配。编号不必再通过主字段全文分词。
“严格”沿用现有大小写不敏感的连续包含：`PR-PV02` 可匹配 `XPR-PV02Y`，
不能匹配 `PR PV02`、`PRPV02` 或跨数组元素拼出的编号；它不是 keyword 完整值相等。
不识别业务编号格式，纯数字或字母数字片段仍走既有普通文本规则。
例如单向规则 `mobile => phone` 会替换全文分支的原词，字面包含分支仍可能命中 mobile；
最终召回由全文与包含组合决定，应使用业务样本验证。查询契约见[包文档](../doc.go)。

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

修改主字段名时，同时修改 JSON 的 `mappings.properties` 和查询方法的第一个参数。
两个入口固定使用主字段、`.contains` 和 `.contains_unigram`，须保留对应字段及配套的分析链。
同一套查询可用于两份模板，不需要 Go 层的分词器配置或能力开关。

单字符默认支持；`i`、`ip`、`iph` 通过包含字段召回，不需要额外的 prefix 字段。
查询不设置输入长度上限，业务在调用前校验搜索框长度和请求规模。不同片段超过 16 个时只保留整句查询。
`QueryExpanded` 的混合路径另限最多 16 块（重复块也计数），超出时回退为 `Query` 的两条原路径；
纯文本、纯符号或没有连续多词普通块的输入与 `Query` 的 DSL 完全一致。
两个入口不自动切换，不增加配置、索引字段或分析器；`Query` 不承担混合块扫描与构造成本。
重复输入去重后即使只有一个片段，也保留整句和该片段两条路径，例如 `iph iph` 仍可通过 `iph` 召回。
该修复不改变模板，无需重建索引；重复输入因新增匹配分支可能改变评分。

扩展入口仍可能因整句、块内多条路径命中而累加评分；去重会移除外层分段 AND 的分数贡献。
完整召回集合保留，不保证与基础入口或优化前三路径方案的 `_score`、排名或 Top K 相同；
整体放入 filter 不改变所选入口的匹配集合，文本子句不计分。同一普通块内跨词同义词与部分词的任意组合
仍不保证召回，例如 `personal computer iph PR-PV02` 不会额外拆出 `personal computer`。

修改模板后，按业务需要发布新索引或变更已有索引配置，并在真实 ES 上验证实际召回与评分。
数据清洗、写入和回填均由应用负责；模板由人工维护，不通过 Go 生成。

## 测试范围

Go 单元测试验证 DSL、非法输入、整句/分段组合与并发隔离，不做模板生成一致性比较。
`TestElasticsearchIntegration` 直接读取模板，覆盖分片为 1、副本为 0 后创建临时索引，验证查询召回并清理索引。
该测试须提前在所有数据节点准备空的 `config/analysis/search-synonyms.txt`；非空词典会改变固定样本的预期召回。

`TestElasticsearchSynonymsIntegration` 复用同一流程，但仅在临时索引中把 `synonyms_path` 替换为
内联规则 `pc, personal computer`，无需准备节点词典文件，也不修改文件或执行热重载。
分词器、过滤器顺序、`lenient`、`updateable` 和 mapping 均沿用所选模板。
它校验完整命中集合，覆盖双向跨词同义词、编号组合、错误编号、缺失连字符、缺失关键词、
非连续短语及跨数组元素的负例。

`TestElasticsearchMixedIntegration` 补充编号前后位置、多词组和多编号、中文、Unicode 空白与全角符号、
重复词同义词、编号作为子串、错误或缺失编号，以及不跨编号拼接词组的验证。
三类集成测试都验证两个入口。混合样本还覆盖整句、重复块及 16/17 块边界；
同一索引对照基础查询与优化前三路径方案，检查基础命中保留、扩展命中、去重前后完整命中相同、
重复评分减少、仅命中混合路径的评分不变，以及整体 filter 的零分语义；
不把 BM25 绝对分数或固定排名写死为契约。

在已安装匹配版本 IK 插件的测试 ES 上，分别运行两份模板：

```bash
for analyzer in standard ik; do
  PIXIU_ESTEXTSEARCH_TEST_URL=http://127.0.0.1:9200 \
    PIXIU_ESTEXTSEARCH_TEST_ANALYZER="$analyzer" \
    go test -race ./infra/estextsearch -run '^TestElasticsearch(Synonyms|Mixed)?Integration$' -count=1 -v
done
```

三类集成测试均未配置地址时跳过；省略 analyzer 时仅运行 Standard，指定 IK 后缺少插件会报错，不自动跳过。

固定样本验证不替代目标 ES 版本、业务词典、重载流程和性能验收。

## 双入口与去重验证结果（2026-10-16）

Elasticsearch 7.17.17 与同版本 IK 下，每种模板的 46 个输入分别验证两个入口，共 92 项入口召回断言通过（含 race）。
基础能力、重复关键词修复与编号约束保留；扩展查询与优化前三路径方案的完整命中集合相同，
重复评分减少。全量 `go test ./...`、`go test -race ./...`、构建、模块 vet 与格式检查通过。

2 万条合成短文本、1 分片、2 CPU/1 GiB 容器、256 MiB JVM 堆，预热后单并发，
每版本每轮 400 次、轮换 3 轮。以下为各轮 P95 的中位数；请求返回 20 条、统计全部命中、关闭 request cache。
另取回全部 ID，验证优化前后集合相同。数据仅用于本次对照，不代表生产容量。
请求耗时为客户端 HTTP 往返，包含网络、ES 请求处理、查询及响应传输；首次三轮统计还包含客户端 JSON 解码。
CPU 是 ES 容器 CPU 时间除以请求数，包含 JVM GC 等后台开销，不是 CPU 使用率。DSL 提前生成，两项指标均不含 Go 构造成本。

以下表格比较三路径扩展方案与当前 QueryExpanded 的优化效果。

| 查询 | 命中数 Query / QueryExpanded | Standard P95 ms 三路径→QueryExpanded | IK P95 ms 三路径→QueryExpanded |
| --- | ---: | ---: | ---: |
| personal computer PR-PV02 | 4000 / 8000 | 6.073 → 4.066 | 5.034 → 3.587 |
| personal computer PR-PV02 mobile phone #2 | 2000 / 4000 | 5.422 → 3.866 | 5.868 → 3.840 |

上述典型混合评分查询 P95 下降约 29%～35%，容器 CPU/请求下降约 30%～37%。
`personal computer PR-PV02` 的 Go 构建从三路径的 4572 ns、21032 B、166 次分配，
降至 QueryExpanded 的 3082 ns、13576 B、109 次分配；
基础 Query 为 1972 ns、9232 B、71 次分配。两个入口保留明确的能力与成本取舍。

首次短采样中 IK 稀有编号 P95 为 0.758 → 1.128 ms，整体 filter 为 0.783 → 0.939 ms。
这些历史数据保留；后续复测未发现稳定退化，见下文。两个入口与其他查询策略保持不变，无需重建索引。

### Query 与 QueryExpanded 的成本和召回

以下取上述同轮 IK 数据的 P50 与 CPU 中位数，比较基础 Query 和当前已去重的 QueryExpanded。
纯文本、简单词与编号等未触发混合规则的输入，两个入口的 DSL 相同，ES 侧没有结构性成本差异。
触发混合规则时，扩展入口用额外执行成本补充召回。

| 查询 | 请求 P50 ms：Query → QueryExpanded | ES CPU ms/请求：Query → QueryExpanded | 命中数：Query → QueryExpanded |
| --- | ---: | ---: | ---: |
| personal computer PR-PV02 | 1.89 → 2.75 | 1.79 → 2.96 | 4000 → 8000 |
| personal computer PR-PV02 iph | 1.83 → 2.81 | 2.02 → 3.48 | 4000 → 6000 |
| personal computer ZX-00040 | 0.43 → 0.50 | 0.45 → 0.63 | 0 → 1 |
| personal computer PR-PV02（整体 filter） | 0.43 → 0.50 | 0.46 → 0.53 | 4000 → 8000 |

命中数来自合成样本，用于展示能力差异，不代表真实业务召回率会按相同比例增长。
扩展入口保留基础匹配集合与编号约束，分数及 Top K 仍可能变化。表中的整体 filter 表示所选查询不参与评分，
适用于业务本来就不依赖相关性分数的请求；缓存未命中时，执行成本也可能更高。

### IK 尾延迟复测：扩展方案去重前后

首次短采样中，稀有编号与整体 filter 的 P95 上升；后续交错复测更支持测试波动，未发现稳定退化。
这里比较的是优化前三路径扩展方案与当前 QueryExpanded，基础 Query 的对照见前表。

复测保持 ES/IK 7.17.17、2 万条数据、1 分片及相同 CPU/内存配额，单并发；每版本预热 1000 次，
每个场景、每种缓存状态测量 6 轮，每轮每版本 1000 次，随机交错且平衡执行顺序。
另做相同 DSL 的 A/A 对照，并独立统计 CPU、服务端查询阶段耗时和 Profile，Profile 不计入正常请求延迟。
分别开启、关闭测试索引的 query cache；request cache 在两组中均关闭。

| 场景 | query cache | 请求 P95 ms：去重前 → 后 | ES CPU/请求变化 |
| --- | --- | ---: | ---: |
| 稀有编号 | 开启并预热 | 0.835 → 0.745 | 降低 19% |
| 整体 filter | 开启并预热 | 0.630 → 0.616 | 降低 11% |
| 稀有编号 | 关闭 | 0.890 → 0.791 | 降低 15% |
| 整体 filter | 关闭 | 3.973 → 2.874 | 降低 34% |

24 组逐轮对照中 23 组的 P95 更低，CPU 与服务端查询阶段平均耗时均下降；完整命中集合相同
（稀有编号 1 条、整体 filter 8000 条）。相同 DSL 的对照也有波动，采样期间记录到 GC 和 CPU 配额节流，
这些现象支持波动判断，但不能追溯确定首次采样每次慢请求的具体原因。
P99 仍有小幅波动；热缓存下 filter 执行成本已低，关闭缓存后去重收益更明显。

因此保留两个入口及现有去重方案，不为 IK、稀有编号或 filter 增加专用分支。
后续验收采用充分预热、交错采样，同时核对 CPU、缓存和 GC/节流状态；生产表现仍需实际数据与并发压测。
节点 query cache 的适用条件见 [ES 缓存说明](https://www.elastic.co/guide/en/elasticsearch/reference/7.17/query-cache.html)。

主要验证条件、结果与适用边界见上文[双入口与去重验证结果](#双入口与去重验证结果2026-10-16)。
