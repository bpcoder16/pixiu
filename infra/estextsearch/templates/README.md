# estextsearch 默认索引模板

这里保存基于 `Profile.Definition()` 生成、移除 `mappings._meta` 并设置 `dynamic: "strict"` 的静态 JSON 模板，方便其他项目查看、复制和创建索引。

| 文件 | 配置档 | 依赖 |
| --- | --- | --- |
| [ik.json](ik.json) | `DefaultConfig(AnalyzerIK)` | 与 ES 版本匹配的 IK 插件 |
| [standard.json](standard.json) | `DefaultConfig(AnalyzerStandard)` | ES 内置分词器 |

两份文件的默认主字段均为 `query_text`，开启 `contains`、`contains_unigram`，关闭 `prefix`、
`exact`，不包含版本元数据与 schema 指纹。主字段保留 BM25；辅助字段由查询构造器用于召回。
选择与查询 Profile 相同的配置档。Prefix/Exact、自定义字段名等配置需要通过对应 Profile 重新生成。
`Profile.Definition()` 仍返回元数据，`SchemaFingerprint()` 仍可用于本地定义核对；静态模板省略这些记录，不影响分词或查询。

静态模板省略 `text` 字段的默认值 `index_options: "positions"`、`norms: true` 和
`position_increment_gap: 100`，保留 contains 子字段的非默认配置 `norms: false`。
当前 analyzer 未覆盖位置间隔，省略上述参数不改变索引行为；`Profile.Definition()` 仍显式返回这些参数，schema 指纹不变。

模板包含通用短文本定义，并配置 6 个主分片、1 个副本；分片和副本可按部署需要调整。
模板设置 `mappings.dynamic: "strict"`，写入未定义字段时 ES 会拒绝整条文档。
写入商品 ID、价格、状态等业务字段前，必须先在 `mappings.properties` 中显式定义；刷新周期、别名由项目按需补充。
`Profile.Definition()` 本身不指定 `dynamic`，通过 Go 创建索引时需显式设置该策略。
输入限额和单字符开关在 Go 侧执行，JSON mapping 本身不会代替 `Values` / `Query` 的校验。

## 创建单个索引

文件顶层是 `settings` / `mappings`，可直接作为 `PUT /<index>` 请求体。以下命令从 Pixiu
仓库根目录执行；先替换 ES 地址和索引名，并为实际环境补充认证参数：

```bash
ES_URL='http://127.0.0.1:9200'
curl --fail-with-body -X PUT "$ES_URL/your-project-search-v1" \
  -H 'Content-Type: application/json' \
  --data-binary @infra/estextsearch/templates/ik.json
```

使用 Standard 时将文件名替换为 `standard.json`；使用 IK 前安装相应插件。

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

生成来源为当前包的 `DefaultConfig` 和 `Definition()`，输出前移除 `mappings._meta` 及上述默认参数，设置 `dynamic: "strict"` 并补充部署参数。
变更 analysis/mapping 后，应重新生成对应文件并按默认值补齐后的语义核对分词器与字段映射；仅变更查询策略版本无需更新这些静态模板。
复制到业务项目后，保持 `query_text` 的 analysis/mapping 与查询 Profile 配套，并另行核对部署参数与 IK 词典。

将以下程序保存到仓库目录外的临时 `.go` 文件，在 Pixiu 根目录运行 `go run <临时文件>`，
会重新生成这两份 JSON：

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
        main := definition.Mappings["properties"].(map[string]any)["query_text"].(map[string]any)
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

对应 schema 已在 ES 7.17.17 的 Standard 和 IK 配置上通过功能集成用例；跨版本兼容性、
业务质量与容量需由接入项目继续验收。详细设计见仓库本地的 `docs/estextsearch-design.md`。
