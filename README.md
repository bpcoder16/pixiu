<p align="center">
  <img src="assets/pixiu-logo.png" width="160" alt="貔貅 Pixiu 吉祥物：青玉色瑞兽，暖金瑞角与卷尾">
</p>

<h1 align="center">貔貅 · Pixiu</h1>

<p align="center">
  面向 Go 服务的分层复用库<br>
  独立原语 · 技术能力 · 通用业务流程
</p>

<p align="center">
  <a href="go.mod"><img src="https://img.shields.io/badge/Go-1.27.3%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.27.3+"></a>
  <a href="https://pkg.go.dev/github.com/bpcoder16/pixiu"><img src="https://pkg.go.dev/badge/github.com/bpcoder16/pixiu.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache--2.0-blue.svg" alt="许可证 Apache-2.0"></a>
</p>

貔貅将 Go 服务中反复出现的工程能力沉淀为职责清楚、可按需引入的包：从结构化日志、文件轮转和并发任务，到 HTTP、数据库、缓存、消息通信和 JWT 签发验签。

你可以只引入一个基础模块，也可以在应用中组合多种能力。资源的创建、业务配置与退出顺序由应用明确掌握。

[项目理念](#项目理念) · [安装](#安装) · [快速开始](#快速开始) · [模块导航](#模块导航) · [分层架构](#分层架构) · [使用示例](#使用示例) · [使用约定](#使用约定) · [开发与贡献](#开发与贡献) · [许可证](#许可证)

## 项目理念

### 为什么叫貔貅

“貔貅”是中国传统文化中的祥瑞之兽。这个名字寄托了项目的愿望：把值得复用的工程能力积累下来，为不同的 Go 服务提供可靠支撑。

项目使用 **Pixiu** 作为英文名与 Go module 名。吉祥物以青玉色兽身、暖金瑞角和圆润卷尾呈现貔貅的形象，见 [Logo 图片](assets/pixiu-logo.png)。

### 设计重点

- **职责分层**：基础模块提供独立原语，`infra/` 组合通用技术能力，`biz/` 承载跨项目复用的业务流程。
- **基础模块只使用标准库**：顶层基础模块的生产代码彼此独立，不引入第三方包。
- **性能与正确性一起验证**：日志采用同步写入、字段直接编码与缓冲复用；优化保留字段顺序、JSON 转义和完整记录写入等语义。
- **统一可观测性**：围绕 `logit` 关联请求、下游调用与后台任务的日志，按需汇总调用耗时。
- **显式生命周期**：连接、服务和任务池由调用方创建与关闭；`lifecycle` 帮助按依赖顺序释放资源。
- **按需组合**：沿用 `context.Context`、`io.Writer`、`http.Handler` 和所封装 SDK 的公开能力，便于接入现有应用。

适用于需要统一日志和下游访问方式的 Go 服务，也适用于只想复用文件轮转、并发编排或资源关闭等单项能力的项目。

## 安装

当前 [go.mod](go.mod) 要求 **Go 1.27.3 或更高版本**。

在已有 Go module 的项目中安装需要的包：

```bash
go get github.com/bpcoder16/pixiu/logit
```

需要其他能力时，使用对应路径：

```bash
go get github.com/bpcoder16/pixiu/rotatefile
go get github.com/bpcoder16/pixiu/infra/httpcall
go get github.com/bpcoder16/pixiu/infra/jwtx
```

各包共享根 Go module，导入路径都以 `github.com/bpcoder16/pixiu` 开头。只使用基础模块时不会编译 `infra/` 的代码，但根 `go.mod` 仍记录基础功能与业务功能使用的第三方依赖。

使用 `infra/sqlitex` 时还需要启用 CGO，并准备可用的 C 编译器；该包采用 `go-sqlite3` 驱动。

## 快速开始

下面是一个完整的结构化日志示例。保存为 `main.go`，安装 `logit` 后执行 `go run .`：

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	logger := logit.MustNew(
		logit.OptWriter(logit.Stdout()),
		logit.OptEncoder(logit.DefaultJSONEncoder),
		logit.OptMinLevel(logit.InfoLevel),
	)
	logit.SetDefault(logger)
	defer func() {
		if err := logit.Close(logger); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	ctx := logit.WithContextLogID(context.Background())
	ctx = logit.WithStart(ctx)
	logit.AddField(ctx, logit.Str("service", "demo"))

	logit.Info(ctx, "开始处理", logit.Str("action", "hello"))
	logit.InfoDuration(ctx, "处理完成")
}
```

这个例子会向标准输出写入 JSON 日志，在同一请求中沿用 `logId` 和 `service` 字段，并在结束日志中追加自身与总耗时。

默认全局 Logger 为空操作 Logger：未调用 `SetDefault` 时，日志不输出到 stdout/stderr，所有级别（包括 Fatal）直接返回，`Enabled` 为 false，`Close` 返回 nil。仅使用 `infra/elasticsearchx` 等能力时，无需初始化 logit。需要日志时，在启动阶段通过 `New` / `MustNew` 构造实例并调用 `SetDefault`；也可直接使用独立实例，或通过 `SetNamed` 注册命名 Logger。`WithContext` 的前置约束和 panic 专用接口语义保持不变。

需要缓存模块 Logger 时，应在启动阶段完成 `logit.SetDefault` 或 `logit.SetNamed` 后再获取实例。初始化前缓存的空操作 Logger 会继续保持静默。包级 `logit.Info(ctx, ...)` 等方法在每次调用时选择当前 Logger。

## 模块导航

以下均为当前仓库已实现的公开包。链接指向各包的 `doc.go`，其中包含用途、调用样例和具体边界。

### 基础模块

| 包 | 能力 | 典型用途 |
| --- | --- | --- |
| [`logit`](logit/doc.go) | 文本 / 有序 JSON 日志、类型化字段、context 字段、命名 Logger、耗时与 panic 记录 | 服务日志与调用链关联 |
| [`rotatefile`](rotatefile/doc.go) | 按本地小时或天轮转、稳定软链、旧文件清理 | 日志或其他持续追加文件 |
| [`lifecycle`](lifecycle/doc.go) | 实例式资源关闭栈，逆序执行并汇总错误 | 按依赖顺序关闭资源 |
| [`conc`](conc/doc.go) | 单次调用的命名并发任务、结果汇总、限流或整体超时 | 并行查询与结果聚合 |
| [`jsonx`](jsonx/doc.go) | JSON 首值解码，保留解码期间观察到的读取错误 | 解析流中的一个 JSON 值 |
| [`cryptox`](cryptox/doc.go) | HMAC-SHA256、AES-GCM、用途绑定的分享 token、安全随机数 | 内部接口参数签名、分享凭证加解密；[设计与安全约束](cryptox/doc.go) |
| [`netx`](netx/doc.go) | 本机 IPv4 / IPv6 地址查询 | 获取符合筛选规则的网卡地址 |
| [`validx`](validx/doc.go) | 手机号、邮箱、HTTP URL、IP、数字串与 rune 长度验证 | 请求参数的无状态格式检查 |
| [`moneyx`](moneyx/doc.go) | 元字符串与 int64 分金额精确转换，支持负数、格式及溢出错误 | 金额输入解析与固定两位小数输出 |
| [`percentx`](percentx/doc.go) | 万分之一整数与百分比文本精确互转，支持整个 int64 范围 | 费率输入与百分比展示 |

以上基础模块的生产代码仅依赖 Go 标准库，彼此不互相导入。

### 基础功能 · infra

导入时在下表路径前加上 `github.com/bpcoder16/pixiu/`。

| 包 | 能力 | 主要依赖 |
| --- | --- | --- |
| [`infra/apprun`](infra/apprun/doc.go) | 常驻服务监督、统一停收和限时排空、显式 SIGINT/SIGTERM 接入 | 标准库、logit |
| [`infra/cronx`](infra/cronx/doc.go) | 六字段 Cron、固定/随机间隔、防重入、任务日志与两阶段关闭 | gocron v2、robfig/cron v3、logit；可注入 gocron.Locker |
| [`infra/configx`](infra/configx/doc.go) | YAML / TOML / JSON 结构体配置加载、全局命名获取与零值兜底 | Viper |
| [`infra/env`](infra/env/doc.go) | 通用应用环境、IP 校验与补齐、一次初始化与全局读取 | 标准库、netx |
| [`infra/httpcall`](infra/httpcall/doc.go) | 可复用 HTTP 下游客户端、结果日志、请求级耗时 | Resty v2、logit |
| [`infra/httpserver`](infra/httpserver/doc.go) | 标准 `http.Handler` 服务端、监听、限时关闭 | 标准库 |
| [`infra/websocketx`](infra/websocketx/doc.go) | 标准 Handler、认证与心跳、有界收发、不可变预制帧、独立连接生命周期 | Gorilla WebSocket、logit |
| [`infra/ginx`](infra/ginx/doc.go) | 独立 Gin Engine、请求日志作用域、访问日志、Recovery 与 OpenAPI / Elements 文档 | Gin、Swag、logit |
| [`infra/jwtx`](infra/jwtx/doc.go) | HS256 / RS256 / ES256 / PS256 JWT 签发、验签与声明校验 | golang-jwt/jwt v5 |
| [`infra/taskpool`](infra/taskpool/doc.go) | 本地异步任务、有界队列、worker 扩缩容、重试与排空 | logit |
| [`infra/lrucache`](infra/lrucache/doc.go) | 泛型有界 LRU、默认 / 单条目 TTL、独立及命名实例 | ttlcache v3 |
| [`infra/mysqlx`](infra/mysqlx/doc.go) | MySQL 连接池、显式主从选择、查询日志 | GORM、MySQL 驱动、logit |
| [`infra/gormx`](infra/gormx/doc.go) | Repository 主、从库会话依赖 Sessions，按关键词原文进行 LIKE 包含匹配 | GORM |
| [`infra/schemamigrate`](infra/schemamigrate/doc.go) | AutoMigrate、物理分表、MySQL / PostgreSQL 表注释及按数量校准 SQL 种子 | GORM |
| [`infra/postgresqlx`](infra/postgresqlx/doc.go) | PostgreSQL 连接池、显式主从选择、查询日志 | GORM、pgx、logit |
| [`infra/clickhousex`](infra/clickhousex/doc.go) | ClickHouse 连接池、显式主从选择、查询日志 | GORM、ClickHouse 驱动、logit |
| [`infra/sqlitex`](infra/sqlitex/doc.go) | SQLite 单库连接池、常用参数配置、查询日志 | GORM、go-sqlite3、logit |
| [`infra/redisx`](infra/redisx/doc.go) | 单机 Redis 客户端、初始化验活、命令结果日志 | go-redis v9、logit |
| [`infra/natsx`](infra/natsx/doc.go) | Core / JetStream 消息收发、启动期批量消费注册、Handler 注册表、命名空间与统一日志 | nats.go、logit |
| [`infra/elasticsearchx`](infra/elasticsearchx/doc.go) | Elasticsearch 通用操作、Bulk、默认及命名实例、结果日志与版本适配 | 官方 Elasticsearch SDK、logit |
| [`infra/elasticsearchx/textsearch`](infra/elasticsearchx/textsearch/doc.go) | Elasticsearch 短文本查询 DSL、基础召回与混合输入扩展召回 | 标准库 |
| [`infra/aliyun/ossx`](infra/aliyun/ossx/doc.go) | OSS 固定 Bucket 的上传、对象查询、下载签名与生命周期 | 阿里云 OSS SDK V2、credentials-go、logit |
| [`infra/aliyun/stsx`](infra/aliyun/stsx/doc.go) | STS AssumeRole、临时凭据与独立生命周期 | 阿里云 STS SDK、credentials-go、logit |
| [`infra/aliyun/esx`](infra/aliyun/esx/doc.go) | 阿里云 ES 固定 InstanceID 的同义词清单、完整清单更新、实例查询 | 阿里云 Elasticsearch 管理 SDK、credentials-go、logit |

Elasticsearch 连接按服务端主版本选择 [`v7`](infra/elasticsearchx/v7/doc.go)、[`v8`](infra/elasticsearchx/v8/doc.go) 或 [`v9`](infra/elasticsearchx/v9/doc.go) 适配包；共用操作位于 `infra/elasticsearchx`。各版本的 `NewDefault` / `NewNamed` 共用注册表，通过 `elasticsearchx.Default()` / `Named(name)` 获取实例，应用统一调用 `CloseAll()` 关闭；独立 `New` 创建的实例仍需自行关闭。

独立 Elasticsearch 客户端默认关闭请求结果日志，通过 `OptLogRequests(true)` 开启；请求体和响应详情还需开启 `OptLogDetails(true)`。context 已调用 `logit.WithStart` 时，关闭结果日志仍会记录请求级下游耗时。`Search` 超时或分片失败会保留已有结果并返回 `PartialSearchError`；`Bulk` 动作失败返回 `BulkError` 并保留成功数与失败项，调用方应检查错误后再决定是否接受结果或重试。

原 `biz/elasticsearchservice` 的词库验证已合入 `infra/elasticsearchx`。已有客户端可直接调用 `client.ValidateSynonyms(ctx, elasticsearchx.SynonymValidationOptions{SynonymsPath: "analysis/synonyms.txt"})`。结果分别报告验证与清理状态，`MayExist` 为真时按 `Index` 核查临时索引。

原 `infra/estextsearch` 已迁至 `infra/elasticsearchx/textsearch`，包名改为 `textsearch`。该包独立构建查询子句，不执行网络请求：`Query(field, text)` 提供基础召回，`QueryExpanded(field, text)` 扩展跨词同义词与编号等符号片段的组合召回，可能增加查询成本并改变评分或排名。调用方须准备主字段及 `.contains`、`.contains_unigram` 子字段，索引配置见 [模板说明](infra/elasticsearchx/textsearch/templates/README.md)；空输入返回 `nil, nil`，合并请求时应省略该条件。

`infra/aliyun/ossx`、`infra/aliyun/stsx` 和 `infra/aliyun/esx` 独立持有配置、凭据、连接池和命名注册表，可使用不同账号。三者均支持成对配置 `AccessKeyID` / `AccessKeySecret`，或注入与其互斥的 `Credential`；`New` 只构造，不联网验活，结果日志默认关闭。共享实例使用 `NewNamed` / `NewDefault` 创建并统一 `CloseAll`；独立 `New` 创建的实例自行关闭。

OSS 每个实例固定一个 `Config.Bucket`，阿里云 ES 管理端每个实例固定一个 `Config.InstanceID`。`UpdateSynonymsDicts` 提交完整目标清单，遗漏已有文件会删除它；API 成功不代表分发或索引加载完成。凭据、上传及词典更新的完整边界见对应包文档。

### 通用业务功能 · biz

| 包 | 能力 | 典型用途 |
| --- | --- | --- |
| [`biz/httpauth`](biz/httpauth/doc.go) / [`ginauth`](biz/httpauth/ginauth/doc.go) | 命名认证方案、统一身份、标准 Context 与 Gin 中间件 | 支持多平台认证，按平台注册认证实现，由路由显式选择认证方案 |
| [`biz/ossservice`](biz/ossservice/doc.go) | multipart 上传、指定对象的 STS 凭据签发、URL 内容 MD5 | 上传需要 OSS，签发另需 STS；MD5FromURL 为独立包函数，无需客户端 |

新宿主使用 Baize 加载应用配置、初始化日志、装配资源和执行数据库迁移，通过 Baize 的 `httpboot.Gin`、`cronboot.Service`、`natsboot.ConsumerService` 和 `realtimeboot.Service` 装配常驻服务。事务发件箱使用 Baize 的 `service/outbox`，JetStream 投递通过 `outboxboot.PrepareJetStream` 装配。[Baize v0.1.0](https://github.com/bpcoder16/baize/tree/v0.1.0)

原 `biz/common` 已拆分。百分比转换改用 Pixiu 的 `percentx.Format` 和 `percentx.Parse`，两个百分比错误也随包迁移。业务编号改用 Baize 的 `bizid.Generate`，时间展示与名称脱敏改用 Baize 的 `display.FormatTime` 和 `display.MaskName`。

分布式业务锁已从 `github.com/bpcoder16/pixiu/biz/lockx` 迁至 `github.com/bpcoder16/baize/service/lockx`，Redis 后端随之迁至新路径的 `redislock` 子包。升级 Pixiu 前，将宿主的 `redisboot.PrepareLock` 配置、`lockx.Named` 调用和测试后端一起切换到新包。两个包各有独立的命名注册表。已迁移的宿主使用 Baize `v0.1.2-0.20261123080219-0af689c5bc07`。[Baize 锁包文档](https://github.com/bpcoder16/baize/blob/0af689c5bc072c1424da58ebb4dcf7969d2d5e7f/service/lockx/doc.go)

## 分层架构

项目按基础模块、基础功能（`infra/`）和通用业务功能（`biz/`）分层：

1. 顶层基础模块的生产代码只使用标准库，不依赖其他基础模块、`infra/` 或 `biz/`。
2. `infra/` 可依赖基础模块、第三方包和 `infra/internal/`，不依赖其他公开 `infra` 包或 `biz/`。`infra/internal/` 不反向依赖公开包。既有 Elasticsearch 版本适配子包对父包的依赖保留为迁移边界。
3. `biz/` 可依赖两个下层，同层保持无环依赖。项目特有业务逻辑由业务仓库承载。

应用可以直接使用任意一层。例如，`ginx` 创建的路由器实现 `http.Handler`，应用可把它传给 `httpserver.New`；日志轮转则由应用把 `rotatefile` 接入 `logit.NewWriter`。

## 使用示例

下面的完整示例可分别保存为独立程序，安装相应包后运行。完整配置与边界请阅读对应包文档；新宿主接入示例见 [baize-demo](https://github.com/bpcoder16/baize-demo)。

### 最小 HTTP 应用

示例读取 HTTP 监听配置，提供健康检查，并在收到停机信号后等待请求排空。将模板复制为 `conf/http.yaml`，保存以下代码为 `main.go` 后运行 `go run .`。访问 `http://127.0.0.1:8888/healthz` 应返回 HTTP 204，按 Ctrl+C 关闭服务。[HTTP 配置模板](infra/httpserver/conf.example/http.yaml)

<details>
<summary>展开完整示例（无需外部服务）</summary>

```go
package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/infra/configx"
	"github.com/bpcoder16/pixiu/infra/httpserver"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	cfg, err := configx.Parse[httpserver.Config]("./conf/http.yaml")
	if err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	server, err := httpserver.New(*cfg, mux)
	if err != nil {
		return err
	}
	var manager apprun.Manager
	if err := manager.Register(apprun.Service{
		Name: "http",
		Run: func() error {
			err := server.Run()
			if errors.Is(err, httpserver.ErrClosed) {
				return nil
			}
			return err
		},
		Stop:   server.Stop,
		Wait:   server.Wait,
		Cancel: server.Cancel,
	}); err != nil {
		return err
	}
	return manager.RunSignals()
}
```

</details>

### Gin 与 OpenAPI 文档

使用 Baize 的宿主通过 `httpboot.Gin` 注册业务路由。直接使用 Pixiu 时，通过 `ginx.New` 创建 Engine，将它传给上例中的 `httpserver.New`。需要文档页面时，先导入 Swag 生成的文档包，再在选定的路由组调用 `ginx.RegisterOpenAPI`。多模块应用可以传入 `ginx.OpenAPIModule` 指定文档名称和导航标题。[Gin 与 OpenAPI 包文档](infra/ginx/doc.go)

### MySQL / PostgreSQL 表结构迁移与种子

使用 Pixiu 直接执行迁移时，将上下文、获取主库连接的函数和模型清单传给 `schemamigrate.Apply`。使用 Baize 的宿主可将迁移声明为启动计划中的准备步骤。

每张表通过 `Model` 登记，可选 `Name` 指定物理表名、`Comment` 管理 MySQL / PostgreSQL 表注释。只迁移显式登记的表，关联表和多对多中间表须单独登记，不自动创建关联外键。没有跨实例迁移锁，发布时应避免多实例同时执行；DDL 失败或超时可能已部分生效，不能保证整批回滚。完整样例与报告语义见 [`schemamigrate` 包文档](infra/schemamigrate/doc.go)。

表结构就绪后，可通过 `schemamigrate.ApplySeeds` 执行项目内嵌的种子 SQL。

种子 SQL 每个有效行必须是一条以分号结束、插入一行的 `INSERT INTO`；关键字不区分大小写，空行与 `--` 整行注释不计数。空文件、跨行 SQL 及其他语句报错。有效行数与 `Where` 范围内的物理记录数（含软删除记录）相同则保留内容；不同则事务 DELETE 该范围并重建，空 `Where` 明确作用于整表。每条 INSERT 必须影响一行，完成后再次核对范围内数量；失败回滚当前项的删除与插入并停止，之前完成的项保留。

SQL 是可信部署代码，调用方负责其目标表及条件正确，并使用支持事务的表；本组件不解析 SQL 或隔离权限。不重置自增序列，重建可能改变 ID 和覆盖自定义数据，只适用于可整体重建的受管数据。数量相同不会发现内容差异。底层可直接调用 `schemamigrate.ApplySeeds(ctx, masterDB, files, seeds)`，由调用方提供带期限的 Context；没有跨实例锁，须由单个实例在业务流量进入前执行。

PostgreSQL 的 `serial` / `bigserial` 或允许显式值的 identity 列插入固定 ID 时，不会自动推进对应序列，后续默认生成 ID 可能发生主键冲突。优先省略自增 ID 或填写 `DEFAULT`；必须混用固定 ID 和自动生成 ID 时，由项目在单实例初始化期间显式校准对应序列，组件不自动处理。序列取号或 `setval` 不随事务回滚，不属于种子删除与插入的回滚保证。参见 [PostgreSQL 18 serial 说明](https://www.postgresql.org/docs/18/datatype-numeric.html#DATATYPE-SERIAL)及[序列函数](https://www.postgresql.org/docs/18/functions-sequence.html)。

### 日志写入轮转文件

按小时轮转，当前轮转维度的保留上限设为 48 个实际文件（包含当前时段）；每小时整点清理，清理完成前可能暂时超过上限。通过稳定的 `app.log` 软链访问当前文件。示例使用临时目录，结束后保留文件以便查看。

<details>
<summary>展开完整示例</summary>

```go
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bpcoder16/pixiu/logit"
	"github.com/bpcoder16/pixiu/rotatefile"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "pixiu-logs-")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, "app.log")
	file, err := rotatefile.New(path,
		rotatefile.OptEvery(time.Hour),
		rotatefile.OptMaxFiles(48),
	)
	if err != nil {
		return err
	}
	logger := logit.MustNew(logit.OptWriter(logit.NewWriter(file)))
	ctx := logit.WithContextLogID(context.Background())
	logger.Info(ctx, "日志已写入")
	fmt.Println("日志路径:", path)
	return logit.Close(logger)
}
```

</details>

生产环境请使用应用有权限写入的绝对路径，并在其他资源收尾之后关闭 Logger。按天轮转使用 `rotatefile.OptEvery(24 * time.Hour)`。

### 一次请求中的并发任务

`conc` 汇总命名任务的结果、错误和耗时。下面同时执行两个任务，并设置整组超时。

<details>
<summary>展开完整示例</summary>

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/bpcoder16/pixiu/conc"
)

func main() {
	tasks := map[string]conc.Task{
		"profile": func(ctx context.Context) (any, error) {
			return "alice", ctx.Err()
		},
		"orders": func(ctx context.Context) (any, error) {
			return 3, ctx.Err()
		},
	}
	report, err := conc.RunNamed(context.Background(), tasks,
		conc.WithTimeout(500*time.Millisecond),
		conc.WithCancelOnError(),
	)
	for name, result := range report.Results {
		fmt.Printf("%s: value=%v err=%v timedOut=%t\n",
			name, result.Value, result.Err, result.TimedOut,
		)
	}
	fmt.Println("整组耗时:", report.Duration)
	if err != nil {
		fmt.Println("整组错误:", err)
	}
}
```

</details>

需要限制并发数时使用 `WithLimit(n)`；它不能与 `WithTimeout` 同时使用。超时返回结果快照，忽略 context 取消的已启动任务仍可能继续运行。

### HTTP 下游调用与耗时汇总

`httpcall` 复用客户端与连接池，每次调用创建新 Request。这个例子在本地启动一个演示下游，无需外部服务；生产环境将 BaseURL 换成实际下游地址。

<details>
<summary>展开完整示例</summary>

```go
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

func main() {
	logger := logit.MustNew(
		logit.OptWriter(logit.Stdout()),
		logit.OptEncoder(logit.DefaultJSONEncoder),
	)
	logit.SetDefault(logger)
	defer func() {
		if err := logit.Close(logger); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()

	downstream := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		},
	))
	defer downstream.Close()

	client := httpcall.New("demo",
		httpcall.OptResty(func(r *resty.Client) {
			r.SetBaseURL(downstream.URL)
			r.SetTimeout(3 * time.Second)
		}),
	)
	ctx := logit.WithContextLogID(context.Background())
	ctx = logit.WithStart(ctx)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	resp, err := client.Request(ctx).Get("/health")
	if err != nil {
		logit.Error(ctx, "下游调用失败", logit.Err(err))
	} else if resp.IsError() {
		logit.Error(ctx, "下游返回错误状态", logit.Int("status", resp.StatusCode()))
	} else {
		fmt.Println("HTTP 状态:", resp.StatusCode())
	}
	logit.InfoDuration(ctx, "请求结束")
}
```

</details>

HTTP 4xx / 5xx 不会自动成为 Go error，需要检查响应状态。默认不自动重试；通过 `OptResty` 配置重试时，应结合下游的幂等语义决定哪些请求允许重试。

### 请求结束后的本地异步任务

`taskpool` 为每个任务创建独立日志作用域，并沿用入口的 `logId`。任务 context 不继承入口的取消和截止时间，执行期限在回调内设置。

<details>
<summary>展开完整示例</summary>

```go
package main

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/infra/taskpool"
	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() (runErr error) {
	logger := logit.MustNew(
		logit.OptWriter(logit.Stdout()),
		logit.OptEncoder(logit.DefaultJSONEncoder),
	)
	logit.SetDefault(logger)
	defer func() {
		runErr = errors.Join(runErr, logit.Close(logger))
	}()

	pool, err := taskpool.New(taskpool.Config{
		MinWorkers:   1,
		MaxWorkers:   4,
		QueueSize:    64,
		DrainTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	requestCtx := logit.WithContextLogID(context.Background())
	submitErr := pool.Submit(requestCtx, "send-notice",
		func(taskCtx context.Context) error {
			workCtx, cancel := context.WithTimeout(taskCtx, time.Second)
			defer cancel()
			logit.Info(workCtx, "后台任务执行")
			return workCtx.Err()
		},
	)

	shutdownErr := pool.Shutdown()
	waitErr := pool.Wait()
	return errors.Join(submitErr, shutdownErr, waitErr)
}
```

</details>

`Submit` 成功只表示任务已接收。任务保存在内存中；需要进程异常退出后的可靠恢复时，应用应选择持久化消息队列。示例显式启用标准输出 JSON 日志，通过 `Shutdown` 发起排空，再用 `Wait` 确认 worker 退出，最后关闭 Logger；`Wait` 没有等待上限。

### JWT 签发与验签

`jwtx` 提供独立的签发器与验签器，可在初始化后并发复用。下面用 HS256 演示签发与验证，并校验签发者 `iss`、接收方 `aud` 和签发时间 `iat`。示例临时生成 32 字节随机密钥；生产环境应从安全配置读取持久密钥。

<details>
<summary>展开完整示例</summary>

```go
package main

import (
	"crypto/rand"
	"fmt"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/infra/jwtx"
	"github.com/golang-jwt/jwt/v5"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() error {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return err
	}
	signer, err := jwtx.NewSigner(jwtx.SignConfig{
		Algorithm: jwtx.HS256,
		Secret:    secret,
	})
	if err != nil {
		return err
	}
	verifier, err := jwtx.NewVerifier(jwtx.VerifyConfig{
		Algorithm:     jwtx.HS256,
		Secret:        secret,
		Issuer:        "example",
		Audience:      []string{"api"},
		CheckIssuedAt: true,
	})
	if err != nil {
		return err
	}
	now := time.Now()
	raw, err := signer.Sign(jwt.RegisteredClaims{
		Issuer:    "example",
		Subject:   "user-42",
		Audience:  jwt.ClaimStrings{"api"},
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(15 * time.Minute)),
	})
	if err != nil {
		return err
	}
	var claims jwt.RegisteredClaims
	if err := verifier.Verify(raw, &claims); err != nil {
		return err
	}
	fmt.Println("已验证用户:", claims.Subject)
	return nil
}
```

</details>

`Sign` 要求提供 `exp`，原样签名，不补写声明或检查时间；`Verify` 验证签名并要求 `exp` 存在、校验过期时间，存在 `nbf` 时校验生效时间；时间校验可通过 `Leeway` 配置容差。RS256 / PS256 使用至少 2048 位的 RSA 密钥，ES256 使用 P-256；非对称签发通过 `PrivateKeyPEM` 配置私钥，验签通过 `PublicKeyPEM` 配置公钥，不传 `Secret`。自定义 claims 与完整密钥格式见 [`jwtx` 包文档](infra/jwtx/doc.go)。

接入 HTTP 认证时，应用实现 [`httpauth.Authenticator`](biz/httpauth/doc.go)，通过 `httpauth.BearerToken` 提取凭证，再调用 jwtx 验签器的 `Verify`，成功后返回 `Principal`。按名称注册认证方案，再用 [`ginauth`](biz/httpauth/ginauth/doc.go) 的 `Require(name)` 绑定路由；业务从请求 Context 读取身份。刷新、撤销和授权规则由应用实现，认证成功不自动代表具备业务权限。

更多示例：[`ginx` 请求处理](infra/ginx/doc.go)、[`httpserver` 启停](infra/httpserver/doc.go)、[`lrucache` 缓存](infra/lrucache/doc.go)、[`mysqlx` 主从查询](infra/mysqlx/doc.go)、[`natsx` 消息通信](infra/natsx/doc.go)。

## 使用约定

这些约定决定了模块组合后的行为，接入时建议先阅读：

| 场景 | 约定 |
| --- | --- |
| 请求日志字段 | 先调用 `WithContext` 或 `WithContextLogID`，再使用 `AddField` / `AddMeta`；未初始化时会 panic。入口初始化一次，后续传递同一个 context。 |
| 字段输出 | 顺序为 With 预埋字段 → meta 字段 → ctx 普通字段 → 调用点字段。同名 key 保留全部出现顺序，不去重；`logId` 是可选关联字段，不承诺唯一性。 |
| 并行日志作用域 | 派生 context 默认共享已初始化的字段存储；需要隔离字段时使用 `NewContextScope`，需要独立耗时表时使用 `NewDurationScope`。 |
| 日志写入 | 日志调用返回时，选中的 Writer 已完成 Write；不代表每条日志都已 fsync。写入错误通过统计和可选回调观察，业务日志调用不返回该错误。 |
| 文件轮转 | 路径必须为绝对路径，同一路径由一个 Writer 管理。进入新时段后的首次 Write 触发轮转，每小时整点清理旧文件，清理完成前文件数可能暂时超过保留上限；旧文件异步 Sync / Close，Writer 的 Sync / Close 等待收尾。 |
| 日志详情 | 各适配包的日志开关与默认值见包文档。开启 Header / Body / SQL / 命令参数等详情时，按包契约保留完整内容，应用需决定记录范围及敏感数据策略。 |
| 数据库读写 | 事务与要求读己之写的查询显式使用主库；可接受延迟的读取可选择从库。模型迁移由应用决定。 |
| JWT 验签 | 实例固定算法与密钥。每次验签使用新的空 claims 对象，仅在 `Verify` 返回 nil 后使用身份字段；失败时丢弃可能已解码的不可信内容。HTTP 认证可组合 httpauth / ginauth，会话刷新、撤销与授权规则由应用处理。 |
| 应用退出 | 先停止入口与生产者，等待任务结束，再关闭下游资源，最后关闭日志。`lifecycle.Stack` 逆序关闭资源，所以日志关闭函数应先登记。 |

文件轮转当前不提供 gzip 压缩；系统时间变化和目录中的未来时段文件不在轮转正确性保证范围。

## 开发与贡献

欢迎通过 [Issues](https://github.com/bpcoder16/pixiu/issues) 报告问题或讨论新的通用能力，也欢迎提交 [Pull Request](https://github.com/bpcoder16/pixiu/pulls)。

克隆仓库并验证：

```bash
git clone https://github.com/bpcoder16/pixiu.git
cd pixiu
go build ./...
make test
make test-race
```

`make test` 执行 `go test ./...`，`make test-race` 执行 `go test -race ./...`。

开发时遵循以下约定：

1. 先确认能力所属层次和职责边界，复用既有实现，保持变更聚焦。
2. 功能变更先用测试定义预期行为；修复问题时提供可复现的输入或场景。
3. 新增公开包时补充中文 `doc.go` 和调用示例；公开 API 变化同步更新文档。
4. 提交前运行 `make test` 与 `make test-race`，并使用 `gofmt` 格式化本次修改的 Go 文件。
5. 日志或轮转热路径变更需运行与功能等价的性能基准，保留正确性门禁和原始结果。

数据库、Redis 和 Elasticsearch 的真实服务验证需要相应环境；具体条件与跳过行为以各包测试为准。全量构建包含 SQLite 驱动，需要 CGO 与 C 编译器。

### 文档入口

- **使用与 API**：从上方模块导航进入各包的 `doc.go`，或查看 [Go Reference](https://pkg.go.dev/github.com/bpcoder16/pixiu)。
- **品牌资源**：[`assets/pixiu-logo.png`](assets/pixiu-logo.png)。
- **本地设计约定**：维护工作区的 `docs/` 保存设计与规划，作为项目约定的权威来源；该目录当前被 Git 忽略，不作为 GitHub 阅读入口。

## 许可证

貔貅采用 [Apache License 2.0](LICENSE)，允许商业使用、修改和分发，也允许集成到闭源项目。分发时须遵守许可证中的版权、修改声明及适用的 NOTICE 要求。

我们欢迎使用者将通用改进和问题修复贡献回社区；公开修改和提交贡献是自愿的，不是本许可证的强制要求。

第三方依赖保留各自的许可证，貔貅的授权不替代它们的许可条件。依赖版本见 [go.mod](go.mod)，使用与分发说明见 [第三方声明](THIRD_PARTY_NOTICES.md)；具体许可条款以各上游项目的许可证和相关声明为准。
