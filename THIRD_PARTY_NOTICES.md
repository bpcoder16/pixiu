# Pixiu 第三方软件声明

Pixiu 自身采用 Apache License 2.0。顶层基础包的生产代码只使用 Go 标准库。`infra/` 按包引入第三方 Go 模块，其中 `infra/ginx` 还随仓库分发 OpenAPI 文档页的 JavaScript 和 CSS。发布应用时，应按实际编入程序或随程序分发的组件核对许可和署名。[Pixiu 许可证](LICENSE) · [Go 模块定义](go.mod)

[内嵌前端资源](#内嵌前端资源) · [Go 直接依赖](#go-直接依赖) · [组件附带的其他声明](#组件附带的其他声明) · [核对具体发行物](#核对具体发行物)

## 内嵌前端资源

`infra/ginx` 的文档页通过 `go:embed` 加载以下三个文件。前两个取自 npm 发布包 **`@stoplight/elements@9.0.28`**。Stoplight Elements 采用 Apache-2.0，打包进其 JavaScript 的组件保留各自的许可和署名。[上游发布包](https://registry.npmjs.org/@stoplight/elements/-/elements-9.0.28.tgz) · [随附许可证](infra/ginx/elements/LICENSE)

| 文件 | 来源与修改 |
| --- | --- |
| `elements/web-components.min.js` | **Pixiu 修改了上游 JavaScript。** 对标有 `x-pixiu-bearer` 的 Swagger 2.0 认证方案，在线调试和生成的请求示例会为 `Authorization` 补齐一次 `Bearer` 前缀。[补丁脚本](infra/ginx/elements/patch-bearer.mjs) |
| `elements/styles.min.css` | Stoplight Elements 原版样式表。[资源来源及 SHA-256](infra/ginx/elements/README.md) |
| `openapi.js` | Pixiu 编写的页面加载脚本，负责适配文档地址和文件上传参数。[加载脚本](infra/ginx/openapi.js) |

在仓库根目录运行 `node infra/ginx/elements/patch-bearer.mjs --check` 可校验 JavaScript 补丁。分发带有这些资源的程序时，应随发行材料保留适用的许可证和组件声明。下表摘列部分署名，完整内容见发布包随附的声明文件。[上游组件声明原文](infra/ginx/elements/web-components.min.js.LICENSE.txt)

| 组件 | 上游署名与许可 |
| --- | --- |
| Font Awesome Free 6.1.1 | Copyright 2022 Fonticons, Inc. 图标采用 CC BY 4.0，字体采用 SIL OFL 1.1，代码采用 MIT。[Font Awesome 许可说明](https://fontawesome.com/license/free) |
| EventEmitter 5.2.9 | Oliver Caldwell，Unlicense |
| ieee754 | Feross Aboukhadijeh，BSD-3-Clause |
| React、React DOM 16.14.0 | Facebook, Inc. and its affiliates，MIT |
| React Router、React Router DOM 6.28.0 | Remix Software Inc.，MIT |

## Go 直接依赖

下表列出 `go.mod` 中的直接依赖，许可证取自对应版本的模块根目录。Go 会按应用导入的包选择参与构建的代码。测试直接导入的模块也在表中，例如 `infra/natsx` 使用 `nats.go` 连接 NATS，测试使用 `nats-server/v2` 启动本地服务。

| Go 模块 | 版本 | 许可证 | 在 Pixiu 中的用途 |
| --- | --- | --- | --- |
| `github.com/ClickHouse/ch-go` | `v0.75.0` | Apache-2.0 | `infra/clickhousex` 测试直接使用原生协议类型，生产驱动 `clickhouse-go/v2` 也使用该模块 |
| `github.com/ClickHouse/clickhouse-go/v2` | `v2.50.0` | Apache-2.0 | ClickHouse Go 驱动 |
| `github.com/alibabacloud-go/darabonba-openapi/v2` | `v2.2.4` | Apache-2.0 | 阿里云 OpenAPI 客户端 |
| `github.com/alibabacloud-go/elasticsearch-20170613/v6` | `v6.4.1` | Apache-2.0 | 阿里云 Elasticsearch 管理 API |
| `github.com/alibabacloud-go/sts-20150401/v2` | `v2.1.0` | Apache-2.0 | 阿里云 STS API |
| `github.com/alibabacloud-go/tea` | `v1.5.3` | Apache-2.0 | 阿里云 Tea SDK 运行库 |
| `github.com/aliyun/alibabacloud-oss-go-sdk-v2` | `v1.6.1` | Apache-2.0 | 阿里云 OSS SDK |
| `github.com/aliyun/credentials-go` | `v1.4.14` | Apache-2.0 | 阿里云凭据提供器 |
| `github.com/elastic/elastic-transport-go/v8` | `v8.11.0` | Apache-2.0 | Elasticsearch HTTP 传输层 |
| `github.com/elastic/go-elasticsearch/v7` | `v7.17.10` | Apache-2.0 | Elasticsearch 7 客户端 |
| `github.com/elastic/go-elasticsearch/v8` | `v8.19.7` | Apache-2.0 | Elasticsearch 8 客户端 |
| `github.com/elastic/go-elasticsearch/v9` | `v9.5.2` | Apache-2.0 | Elasticsearch 9 客户端 |
| `github.com/gin-gonic/gin` | `v1.12.0` | MIT | Gin 路由与中间件 |
| `github.com/go-co-op/gocron/v2` | `v2.23.0` | MIT | 任务调度 |
| `github.com/go-resty/resty/v2` | `v2.17.2` | MIT | Resty HTTP 客户端 |
| `github.com/go-sql-driver/mysql` | `v1.10.1` | MPL-2.0 | MySQL 驱动 |
| `github.com/go-viper/mapstructure/v2` | `v2.5.0` | MIT | 配置到结构体的映射 |
| `github.com/golang-jwt/jwt/v5` | `v5.3.1` | MIT | JWT 签发与验签 |
| `github.com/gorilla/websocket` | `v1.5.3` | BSD-2-Clause | WebSocket 握手与读写 |
| `github.com/jackc/pgx/v5` | `v5.12.0` | MIT | PostgreSQL 驱动 |
| `github.com/jellydator/ttlcache/v3` | `v3.4.1` | MIT | 带 TTL 的进程内缓存 |
| `github.com/mattn/go-sqlite3` | `v1.14.52` | MIT | SQLite 的 Go 驱动 |
| `github.com/nats-io/nats-server/v2` | `v2.15.2` | Apache-2.0 | NATS 测试用内嵌服务端 |
| `github.com/nats-io/nats.go` | `v1.55.0` | Apache-2.0 | NATS Core 与 JetStream 客户端 |
| `github.com/redis/go-redis/v9` | `v9.23.0` | BSD-2-Clause | Redis 客户端 |
| `github.com/robfig/cron/v3` | `v3.0.1` | MIT | Cron 表达式解析 |
| `github.com/spf13/viper` | `v1.21.0` | MIT | 配置文件读取 |
| `github.com/swaggo/swag` | `v1.16.6` | MIT | OpenAPI 文档注册与读取 |
| `golang.org/x/net` | `v0.62.0` | BSD-3-Clause | HTTP 头字段校验 |
| `gorm.io/driver/clickhouse` | `v0.7.0` | MIT | GORM ClickHouse 方言 |
| `gorm.io/driver/mysql` | `v1.6.0` | MIT | GORM MySQL 方言 |
| `gorm.io/driver/postgres` | `v1.6.3` | MIT | GORM PostgreSQL 方言 |
| `gorm.io/driver/sqlite` | `v1.6.0` | MIT | GORM SQLite 方言 |
| `gorm.io/gorm` | `v1.31.2` | MIT | 数据库会话与模型操作 |

## 组件附带的其他声明

个别 Go 模块还附有 `NOTICE`、多份许可文本，或包含按其他条款授权的源码。下列条目涉及直接依赖及其依赖链。`go.mod` 中的 `// indirect` 标记可用于核对间接依赖的版本。

- **SQLite**。`github.com/mattn/go-sqlite3` 的 Go 驱动采用 MIT，其包含的 SQLite 核心由上游贡献至公有领域。[SQLite 官方声明](https://sqlite.org/copyright.html)
- **Elasticsearch**。`elastic-transport-go/v8` 的 `NOTICE` 含 Elasticsearch BV 版权与 Apache Software Foundation 归属。`go-elasticsearch/v8` 和 `go-elasticsearch/v9` 各自附有 Elasticsearch B.V. 的 `NOTICE`。分发对应组件时保留适用的原文。
- **GORM ClickHouse 驱动**。`gorm.io/driver/clickhouse` 随附 `LICENCE` 和 `License` 两份 MIT 文本，保留了不同的版权署名。
- **YAML**。间接模块 `go.yaml.in/yaml/v3 v3.0.6` 中从 libyaml 移植的文件采用 MIT，其余文件采用 Apache-2.0。上游根目录的 `LICENSE` 列出了文件范围，`NOTICE` 保留 Canonical Ltd. 的归属。

### MPL-2.0 对应源码

以下两个模块分别经 MySQL 驱动和 ClickHouse 驱动引入。对外分发含 MPL 受覆盖代码的可执行文件时，应按 MPL 第 3.2 节向接收者说明如何获取相应受覆盖源码。使用未修改版本时可从下列标签取得源码，发布者应确保获取方式可用且与实际分发版本一致。修改过受覆盖文件时，应提供包含这些修改的对应源码。[MPL-2.0 原文](https://www.mozilla.org/en-US/MPL/2.0/)

| 模块 | 引入方式 | 对应版本源码 |
| --- | --- | --- |
| `github.com/go-sql-driver/mysql` | 直接 | [v1.10.1 源码](https://codeload.github.com/go-sql-driver/mysql/zip/refs/tags/v1.10.1) |
| `github.com/hashicorp/go-version` | 间接 | [v1.9.0 源码](https://codeload.github.com/hashicorp/go-version/zip/refs/tags/v1.9.0) |

## 核对具体发行物

1. 对最终的 Go 可执行文件运行 `go version -m <可执行文件路径>`，查看构建时记录的模块版本和替换项。若程序提供内嵌 OpenAPI 文档页，还要核对上面的前端资源及随附声明。
2. 用 `go mod download -json 模块路径@版本` 取得对应源码目录（`Dir`）。检查其中的 `LICENSE`、`LICENCE`、`COPYING`、`NOTICE` 及具体文件的版权头。使用本地替换或补丁时，以实际参与构建的源码为准。
3. 按发行物包含的代码和资源附上相应材料。MIT 和 BSD 组件须保留适用的版权、许可条件和免责声明。Apache-2.0 组件需附许可证、保留适用的归属声明，并标明对文件的修改。MPL 组件还需按上节提供对应源码的获取方式。[Apache-2.0 第 4 节](https://www.apache.org/licenses/LICENSE-2.0#redistribution)
