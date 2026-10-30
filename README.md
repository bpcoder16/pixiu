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

貔貅将 Go 服务中反复出现的工程能力沉淀为职责清楚、可按需引入的包：从结构化日志、文件轮转和并发任务，到 HTTP、数据库、缓存、消息通信、JWT 签发验签，以及可复用的分布式锁流程。

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
go get github.com/bpcoder16/pixiu/biz/lockx/redislock
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

默认全局 Logger 为空操作 Logger：未调用 `SetDefault` 时，日志不输出到 stdout/stderr，所有级别（包括 Fatal）直接返回，`Enabled` 为 false，`Close` 返回 nil。仅使用 `biz/lockx`、`infra/elasticsearchx` 等能力时，无需初始化 logit。需要日志时，在启动阶段通过 `New` / `MustNew` 构造实例并调用 `SetDefault`；也可直接使用独立实例，或通过 `SetNamed` 注册命名 Logger。`WithContext` 的前置约束和 panic 专用接口语义保持不变。

新项目应先完成 `bootstrap.MustBaseInit` 初始化，再通过 `logit.Default()`、`logit.With(...)` 或 `logit.Named(...)` 创建和保存模块 Logger。不要在先于 bootstrap 执行的包变量初始化或 Go 的 `init()` 中缓存这些 Logger：初始化前取到的空操作实例不会随后续 `SetDefault` 或命名注册自动更新。包级 `logit.Info(ctx, ...)` 等方法每次调用时选择当前 Logger，可在初始化完成后直接使用。默认静默便于老项目引入其他 Pixiu 能力，不影响已完成日志初始化的新项目。

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
| [`infra/gormx`](infra/gormx/doc.go) | Repository 主、从库会话依赖 Sessions | GORM |
| [`infra/schemamigrate`](infra/schemamigrate/doc.go) | AutoMigrate、物理分表、MySQL 表注释及按数量校准 SQL 种子 | GORM |
| [`infra/pgsqlx`](infra/pgsqlx/doc.go) | PostgreSQL 连接池、显式主从选择、查询日志 | GORM、pgx、logit |
| [`infra/clickhousex`](infra/clickhousex/doc.go) | ClickHouse 连接池、显式主从选择、查询日志 | GORM、ClickHouse 驱动、logit |
| [`infra/sqlitex`](infra/sqlitex/doc.go) | SQLite 单库连接池、常用参数配置、查询日志 | GORM、go-sqlite3、logit |
| [`infra/redisx`](infra/redisx/doc.go) | 单机 Redis 客户端、初始化验活、命令结果日志 | go-redis v9、logit |
| [`infra/natsx`](infra/natsx/doc.go) | Core / JetStream 消息收发、启动期批量消费注册、Handler 注册表、命名空间与统一日志 | nats.go、logit |
| [`infra/elasticsearchx`](infra/elasticsearchx/doc.go) | Elasticsearch 通用操作、Bulk、默认及命名实例、结果日志与版本适配 | 官方 Elasticsearch SDK、logit |
| [`infra/estextsearch`](infra/estextsearch/doc.go) | Elasticsearch 短文本查询 DSL、基础召回与混合输入扩展召回 | 标准库 |
| [`infra/aliyun/ossx`](infra/aliyun/ossx/doc.go) | OSS 固定 Bucket 的上传、对象查询、下载签名与生命周期 | 阿里云 OSS SDK V2、credentials-go、logit |
| [`infra/aliyun/stsx`](infra/aliyun/stsx/doc.go) | STS AssumeRole、临时凭据与独立生命周期 | 阿里云 STS SDK、credentials-go、logit |
| [`infra/aliyun/esx`](infra/aliyun/esx/doc.go) | 阿里云 ES 固定 InstanceID 的同义词清单、完整清单更新、实例查询 | 阿里云 Elasticsearch 管理 SDK、credentials-go、logit |

Elasticsearch 连接按服务端主版本选择 [`v7`](infra/elasticsearchx/v7/doc.go)、[`v8`](infra/elasticsearchx/v8/doc.go) 或 [`v9`](infra/elasticsearchx/v9/doc.go) 适配包；共用操作位于 `infra/elasticsearchx`。各版本的 `NewDefault` / `NewNamed` 共用注册表，通过 `elasticsearchx.Default()` / `Named(name)` 获取实例，应用统一调用 `CloseAll()` 关闭；独立 `New` 创建的实例仍需自行关闭。

独立 Elasticsearch 客户端默认关闭请求结果日志，通过 `OptLogRequests(true)` 开启；请求体和响应详情还需开启 `OptLogDetails(true)`。context 已调用 `logit.WithStart` 时，关闭结果日志仍会记录请求级下游耗时。`Search` 超时或分片失败会保留已有结果并返回 `PartialSearchError`；`Bulk` 动作失败返回 `BulkError` 并保留成功数与失败项，调用方应检查错误后再决定是否接受结果或重试。

`infra/estextsearch` 独立构建查询子句，不执行网络请求：`Query(field, text)` 提供基础召回，`QueryExpanded(field, text)` 扩展跨词同义词与编号等符号片段的组合召回，可能增加查询成本并改变评分或排名。调用方须准备主字段及 `.contains`、`.contains_unigram` 子字段，索引配置见 [模板说明](infra/estextsearch/templates/README.md)；空输入返回 `nil, nil`，合并请求时应省略该条件。

`infra/aliyun/ossx`、`infra/aliyun/stsx` 和 `infra/aliyun/esx` 独立持有配置、凭据、连接池和命名注册表，可使用不同账号。三者均支持成对配置 `AccessKeyID` / `AccessKeySecret`，或注入与其互斥的 `Credential`；`New` 只构造，不联网验活，结果日志默认关闭。共享实例使用 `NewNamed` / `NewDefault` 创建并统一 `CloseAll`；独立 `New` 创建的实例自行关闭。

OSS 每个实例固定一个 `Config.Bucket`，阿里云 ES 管理端每个实例固定一个 `Config.InstanceID`。`UpdateSynonymsDicts` 提交完整目标清单，遗漏已有文件会删除它；API 成功不代表分发或索引加载完成。凭据、上传及词典更新的完整边界见对应包文档。

### 通用业务功能 · biz

| 包 | 能力 | 典型用途 |
| --- | --- | --- |
| [`biz/common`](biz/common/doc.go) | 按应用时区生成 ID、格式化时间，int64 万分之一整数与百分比精确互转 | 常规业务工具；ID 与时间依赖 env，百分比支持负数 |
| [`biz/httpauth`](biz/httpauth/doc.go) / [`ginauth`](biz/httpauth/ginauth/doc.go) | 命名认证方案、统一身份、标准 Context 与 Gin 中间件 | 支持多平台认证，按平台注册认证实现，由路由显式选择认证方案 |
| [`biz/bootstrap`](biz/bootstrap/doc.go) | 应用时区、日志及多实例初始化，Gin / HTTP / Cron 服务构造、NATS 消费配置装配与显式 MySQL 迁移入口 | 声明实例、登记关闭栈；通过 Start 或自持 Manager 运行服务，日志最后关闭 |
| [`biz/ossservice`](biz/ossservice/doc.go) | multipart 上传、指定对象的 STS 凭据签发、URL 内容 MD5 | 上传需要 OSS，签发另需 STS；MD5FromURL 为独立包函数，无需客户端 |
| [`biz/elasticsearchservice`](biz/elasticsearchservice/doc.go) | 固定 standard → lowercase → synonym_graph，创建临时索引严格验证并清理 | 通过 `ValidateSynonyms` 验证 `synonyms_path`，保留 ES 错误原因与清理状态 |
| [`biz/appconfig`](biz/appconfig/doc.go) | 直接定义 env/log，严格解析应用配置、返回配置指针并初始化环境 | 从 [`app.yaml` 模板](biz/appconfig/conf.example/app.yaml) 加载配置，不在 configx 注册全局配置 |
| [`biz/lockx`](biz/lockx/doc.go) | 阻塞 / 非阻塞锁契约，默认及命名执行器，`Do` / `TryDo` 协调获取、执行与释放 | 受锁保护的业务流程 |
| [`biz/lockx/redislock`](biz/lockx/redislock/doc.go) | 基于单机 Redis 的固定租期锁，原子校验身份并释放 | 同一协议下的多实例协调 |

`biz/appconfig` 直接定义应用共用的环境与日志配置；HTTP、Cron 各自独立配置，由 bootstrap 显式装配。原 httpconfig 和 baseconfig 已删除，不保留兼容入口。

`appconfig.MustLoadAppConfig` 使用 `configx.Parse`，支持 YAML / TOML / JSON，拒绝未知字段和弱类型转换；配置只返回给调用方，环境由 `env.Init` 一次性发布。应用配置文件所在目录作为 `env.ConfigDirPath()`，供 bootstrap 查找实例配置。加载配置不会初始化日志或修改 `time.Local`；`bootstrap.MustBaseInit` 在日志创建前应用环境时区，再依次初始化日志、OSS、STS、阿里云 ES、MySQL、Redis、Elasticsearch 和 NATS，共享客户端仅初始化已声明的实例。

HTTP、Cron 和 NATS 统一停收，共享默认 30 秒并行排空；超时取消剩余任务。正常和超时路径都执行资源回收，lifecycle.Stack 默认整体 20 秒，可用 `lifecycle.New(timeout)` 设置；到期停止发起后续关闭，入口退出。资源逆序回收，日志最后关闭；同一模块的多命名实例不保证关闭顺序。初始化前登记收尾 defer，初始化失败同样有界回收。

共享业务锁通过 `bootstrap.MustRegisterRedisLock(name, isDefault, redisName, redislock.Config{...})` 在代码中声明，无需锁配置文件。`MustBaseInit` 在 Redis 就绪后装配执行器，业务使用 `lockx.Default().Do/TryDo` 或 `lockx.Named(name).Do/TryDo`。多个锁实例可共享同一个 Redis，分别设置租期与等待参数；默认执行器也是对应的命名实例。Redis 须设置 `maxRetries: -1`，执行器不创建或关闭连接池，实例名不改变业务 key。独立装配可使用 `lockx.New/NewNamed/NewDefault`，具体示例见 [bootstrap](biz/bootstrap/doc.go) 和 [lockx](biz/lockx/doc.go)。

初始化完成后可调用 bootstrap.NewAppManager() 创建默认 30 秒管理器，也可传入不小于 5 秒的自定义时长。NewHTTPService(handler) 严格读取 http.yaml，并适配 Run/Stop/Wait/Cancel。登记后调用 RunSignals，返回后关闭资源栈。

常见启动流程直接使用 bootstrap.Start(factories...)，按顺序构造和登记，再接管 SIGINT/SIGTERM。构造失败逆序清理已构造服务；运行后统一停收并共享默认 30 秒。StartWithConfig 的 ShutdownTimeout 自定义服务预算，资源栈由应用单独持有。

`bootstrap.NewCronService(handlers...)` 从独立的 [cron.yaml](biz/bootstrap/conf.example/cron.yaml) 读取调度，按名称绑定 `cronx.NamedHandler`，返回供 Manager 使用的服务和构造错误。也可使用 `cronx.MustNewHandlerRegistry(handlers...).Handlers()...` 提供声明。启用 lock 段时直接组合命名 Redis 与 gocron-redis-lock，设置租期和自动续租，不扩展 lockx。任务超时只取消 context，实际返回后才解锁；失租不会自动取消业务，不保证集群每周期只执行一次。

HTTP、Cron 不再保留独立关闭计时器；http.yaml 的 shutdownTimeout 以及 cron.yaml 的 shutdownTimeout、drainTimeout 须删除，业务任务自身 timeout 保留。服务登记顺序不决定停收顺序。示例见 [bootstrap 文档](biz/bootstrap/doc.go) 与 [pixiu-demo](https://github.com/bpcoder16/pixiu-demo)。

### 规划中的能力

本地设计文档还规划了 `websocketx` 等模块；尚未实现的包不属于当前可用 API。实际可用能力以上述模块导航为准。

## 分层架构

```text
pixiu/
├── logit/                 结构化日志与请求上下文
├── rotatefile/            通用时间轮转文件
├── lifecycle/             资源关闭栈
├── conc/                  单次调用的并发任务编排
├── jsonx/                 JSON 首值解码
├── cryptox/               HMAC-SHA256、AES-GCM 与分享 token
├── netx/                  网络基础能力
├── validx/                参数格式验证
├── moneyx/                元字符串与分金额转换，支持负数
├── infra/                 通用技术能力与 SDK 适配
│   ├── apprun/             常驻服务运行监督与关闭
│   ├── configx/            结构体配置解析与命名获取
│   ├── env/                通用应用运行环境
│   ├── cronx/              Cron、固定/随机间隔与限时排空
│   ├── httpcall/           HTTP 下游调用
│   ├── httpserver/         HTTP 服务端
│   ├── websocketx/         原始长连接、心跳、有界收发与排空
│   ├── ginx/               Gin 请求处理
│   ├── jwtx/               JWT 签发、验签与声明校验
│   ├── taskpool/           本地异步任务池
│   ├── lrucache/           LRU / TTL 缓存
│   ├── mysqlx/             MySQL
│   ├── gormx/              Repository 主、从库会话依赖
│   ├── schemamigrate/      显式表结构迁移、MySQL 表注释与 SQL 种子校准
│   ├── pgsqlx/             PostgreSQL
│   ├── clickhousex/        ClickHouse
│   ├── sqlitex/            SQLite
│   ├── redisx/             Redis
│   ├── natsx/              NATS
│   ├── elasticsearchx/     Elasticsearch 与 v7 / v8 / v9 适配
│   ├── estextsearch/       Elasticsearch 短文本查询 DSL
│   ├── aliyun/             阿里云 SDK 适配
│   │   ├── ossx/           阿里云 OSS 上传
│   │   ├── stsx/           阿里云 STS 临时凭据
│   │   └── esx/            阿里云 ES 管理 API
│   └── internal/           infra 内部共用实现
├── biz/
│   ├── common/             应用时区 ID、时间格式化与百分比转换
│   ├── bootstrap/          组件初始化、运行管理器与 HTTP/Cron 服务构造
│   ├── elasticsearchservice/ 同义词库索引验证与临时资源清理
│   ├── ossservice/         multipart 上传、OSS 授权与 URL 内容 MD5
│   ├── appconfig/          应用 env/log 配置加载与环境初始化
│   ├── httpauth/           命名认证方案与统一身份
│   │   └── ginauth/        Gin 认证中间件
│   └── lockx/              锁契约与业务执行流程
│       └── redislock/      Redis 锁实现
└── assets/                吉祥物 Logo
```

依赖规则：

1. 顶层基础模块的生产代码只使用标准库，不依赖其他基础模块、`infra/` 或 `biz/`。
2. `infra/` 可依赖基础模块、第三方包和 `infra/internal/`，不依赖其他公开 `infra` 包或 `biz/`。`infra/internal/` 不反向依赖公开包。既有 Elasticsearch 版本适配子包对父包的依赖保留为迁移边界。
3. `biz/` 可依赖两个下层，同层保持无环依赖。项目特有业务逻辑由业务仓库承载。

应用可以直接使用任意一层。例如，`ginx` 创建的路由器实现 `http.Handler`，应用可把它传给 `httpserver.New`；日志轮转则由应用把 `rotatefile` 接入 `logit.NewWriter`。

## 使用示例

下面的示例可分别保存为独立程序，安装相应包后运行。每个示例展示一种组合方式；完整配置与边界请阅读对应包文档。

### 最小 HTTP 应用

这个示例无需数据库等外部服务，使用标准 `http.Handler` 提供健康检查，并通过管理器接收停机信号、等待 HTTP 排空，最后关闭共享资源与日志。

将 [`app.yaml`](biz/appconfig/conf.example/app.yaml) 和 [`http.yaml`](biz/bootstrap/conf.example/http.yaml) 复制到应用的 `conf/` 目录，修改应用名称。保存以下代码为 `main.go`，安装 `biz/bootstrap` 后执行 `go run .`，再访问 `http://127.0.0.1:8888/healthz`，应返回 HTTP 204；在终端按 Ctrl+C 关闭。

<details>
<summary>展开完整示例（无需外部服务）</summary>

```go
package main

import (
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/infra/apprun"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func main() {
	if run() != nil {
		os.Exit(1)
	}
}

func run() (runErr error) {
	cfg := appconfig.MustLoadAppConfig("./conf/app.yaml")
	var resources lifecycle.Stack
	defer func() {
		if runErr != nil {
			failure := runErr
			_ = resources.Register(func() error {
				log.Printf("server exit: %v", failure)
				return nil
			})
		}
		runErr = errors.Join(runErr, resources.Close())
	}()
	bootstrap.MustBaseInit(cfg, &resources)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return bootstrap.Start(func() (apprun.Service, error) {
		return bootstrap.NewHTTPService(mux)
	})
}
```

</details>

NewHTTPService 构造不监听，Start 接管服务与信号；Stop 统一封闭入口，Wait 共享 30 秒，超时 Cancel。返回后无论是否排空超时都执行资源栈 Close，默认整体最多等待 20 秒。库不退出进程，应用 main 在有界收尾后退出。

通过 bootstrap 初始化后，业务日志默认写入 `<dir>/<appName>/biz/` 下的 `debug.log`、`info.log`、`wf.log`，分别对应 Debug、Info 和 Warn / Error / Fatal；命名 Logger 增加 `<name>.` 文件前缀。`dir` 默认 `./log`，相对应用启动目录解析；release 输出 Info 及以上，debug/test 输出 Debug 及以上。

`log.hookStd` 省略时默认开启，模板也显式开启：stdout / stderr 原始输出分别追加到 `<dir>/<appName>/std/stdout.log` 和 `stderr.log`，文件不轮转，重定向持续至进程退出。该能力仅支持 Linux/macOS；其他平台或需要保留终端标准输出时，在 `app.yaml` 的 `log` 段显式设置 `hookStd: false`。业务日志仍按上述文件分流。

### Gin 与 OpenAPI 文档

使用 Gin 时，在初始化环境后显式调用 `gin.SetMode(env.RunMode())`，通过 `bootstrap.NewGinEngine()` 构造 Engine，注册业务路由，再将 Engine 传给 `bootstrap.NewHTTPService`。需额外导入 Gin 和 `infra/env`；将 [`gin.yaml`](biz/bootstrap/conf.example/gin.yaml) 放在同一配置目录。文件必须存在，内容为 `{}` 时沿用默认值。

也可在 `Start` 的服务构造函数中调用 `bootstrap.NewGinHTTPService(registerRoutes, modules...)`：它设置 Gin 模式、读取 `gin.yaml` 和 `http.yaml`，在 Engine 与文档路由创建成功后调用业务路由注册回调，并返回 `apprun.Service`。构造仍不监听端口，须在启动阶段串行调用；完整装配示例见 [bootstrap 文档](biz/bootstrap/doc.go)。

`NewGinEngine` 根据应用环境决定文档路由：

- **debug/test**：自动注册 OpenAPI 与内嵌的 Stoplight Elements。应用须先使用 Swag CLI 生成文档（例如 `swag init -o ./openapi`），并在入口 blank import 生成包；未注册文档时构造返回错误。默认页面为 `/docs`，文档为 `/openapi/doc.json`。
- **release**：不自动注册文档路由，也不要求生成文档。

多模块应用可向 `NewGinEngine` 传入 `ginx.OpenAPIModule`，以 `Name` 对应 Swag 的命名文档实例，`Title` 作为导航标题。每个模块提供 `/openapi/<Name>.json` 和 `/docs/<Name>`，`/docs` 展示首个模块；静态资源位于 `/docs/assets/`。旧 `/elements` 及其子路径不注册，也不重定向。生成参数、Bearer 前缀补齐等用法见 [`ginx` 包文档](infra/ginx/doc.go)。

文档路由在 Engine 构造时注册，之后添加的业务中间件不会作用于它们，也不会自动增加认证。需要控制文档访问时，可使用 `ginx.New`，在带认证中间件的路由组调用 `ginx.RegisterOpenAPI`。

文档页面、JSON 端点和内嵌静态资源的 GET 路由会自动跳过 ginx 访问日志、请求和响应详情采集及 `Observe`，仍保留请求作用域、组中间件与 Recovery。`gin.yaml` 模板对业务路由开启完整请求和响应详情，可按需调整对应开关；其他需要跳过观察的路由可通过 `ginx.RegisterIgnoredRoute` 显式登记。

### 应用配置与多实例初始化

将 [`app.yaml`](biz/appconfig/conf.example/app.yaml) 复制到应用的 `conf/app.yaml`。本例声明六个组件实例，需将对应模板复制到同一配置目录，并填写实际连接信息：

| 声明 | 配置文件 | 模板 |
| --- | --- | --- |
| `MustRegisterMySQL("orders", true)` | `conf/mysql.orders.yaml` | [MySQL](biz/bootstrap/conf.example/mysql.example.yaml) |
| `MustRegisterRedis("cache", true)` | `conf/redis.cache.yaml` | [Redis](biz/bootstrap/conf.example/redis.example.yaml) |
| `MustRegisterElasticsearch("search", true)` | `conf/elasticsearch.search.yaml` | [Elasticsearch](biz/bootstrap/conf.example/elasticsearch.example.yaml) |
| `MustRegisterAliyunOSS("assets", true)` | `conf/aliyunossx.assets.yaml` | [阿里云 OSS](biz/bootstrap/conf.example/aliyunossx.example.yaml) |
| `MustRegisterAliyunSTS("upload", true)` | `conf/aliyunstsx.upload.yaml` | [阿里云 STS](biz/bootstrap/conf.example/aliyunstsx.example.yaml) |
| `MustRegisterAliyunES("search-control", true)` | `conf/aliyunesx.search-control.yaml` | [阿里云 ES 管理端](biz/bootstrap/conf.example/aliyunesx.example.yaml) |
| `MustRegisterNATS("events", true)` | `conf/nats.events.yaml` | [NATS](biz/bootstrap/conf.example/nats.example.yaml) |

实例名与默认标记只在代码中声明，实例文件不接受 `name` / `isDefault`。Elasticsearch 文件的 `version` 必须与部署主版本一致，支持 7 / 8 / 9；`caCertFile` 相对路径基于配置目录。只需某个组件时，保留对应声明与配置文件即可；未声明的实例不会加载。

<details>
<summary>展开完整示例（需要实际下游服务）</summary>

```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/bpcoder16/pixiu/biz/appconfig"
	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	cfg := appconfig.MustLoadAppConfig("./conf/app.yaml")
	var resources lifecycle.Stack
	defer func() {
		if err := resources.Close(); err != nil {
			fmt.Fprintln(os.Stderr, "close resources:", err)
		}
	}()

	bootstrap.MustRegisterMySQL("orders", true)
	bootstrap.MustRegisterRedis("cache", true)
	bootstrap.MustRegisterElasticsearch("search", true)
	bootstrap.MustRegisterAliyunOSS("assets", true)
	bootstrap.MustRegisterAliyunES("search-control", true)
	bootstrap.MustRegisterNATS("events", true)
	bootstrap.MustBaseInit(cfg, &resources)
	logit.Info(context.Background(), "应用初始化完成")
}
```

</details>

各组件独立选择默认实例，`Default()` 与 `Named(默认名称)` 返回同一客户端。共享组件由 bootstrap 各登记一次 `CloseAll`，业务无需重复登记单个客户端关闭。初始化失败会 panic，提前登记的 defer 负责回收已创建资源；初始化只在启动阶段串行执行，失败后不支持重试。

此示例只展示初始化；常驻 HTTP 应用还需接入上面的服务管理器。`MustRegisterNATS` 声明实例，`MustBaseInit` 只完成连接初始化，不会自动准备 Stream 或启动消费。

需要消费时，通过 natsx.MustNewJetStreamHandlerRegistry / MustNewCoreHandlerRegistry 登记 Handler，在 Start 工厂中直接返回 `bootstrap.NewNATSConsumerService(instanceName, jetStreamHandlers, coreHandlers)`。该函数读取消费配置并绑定 Handler，返回服务和构造错误。Run 注册，Stop 停收，Wait 等待回调与确认，Cancel 协作取消。与 HTTP/Cron 共用默认 30 秒，登记顺序不表示启动或停机依赖。排空期间保留连接及发布能力，随后由资源栈关闭。JetStream 消费者放在 `streams[].consumers`，自动继承父级 name；每项 Stream 必须声明 subjects，统一创建或校验。旧顶层 jetStreamConsumers、消费者的 stream 字段及仅引用外部 Stream 的配置方式已移除。可选参数继续以注释展示，随消费者嵌套，按需取消注释。模板见 [NATS 消费配置](biz/bootstrap/conf.example/nats-register.example.yaml)；具名函数默认保留原始大小写，闭包、方法值、泛型函数须显式命名。

注册文件中的 `streamDefaults.replicas` 显式配置时必须为正整数；省略或 `null` 时使用对应实例初始化时 `nats.<name>.yaml` 中 `urls` 的条目数，不去重、不计入自动发现节点。最终全局值作为逐项副本数上限，即使清单为空也校验；仅列部分种子地址、使用代理入口或需要不同副本策略时，应显式配置。每个客户端的三类注册入口各只允许调用一次，通过入口检查后，空清单或失败也占用次数；配置装配不能与手动注册混用，失败后应中止启动并关闭客户端，不在同一实例上重试。JetStream 处理函数完成本条业务后返回，由组件根据返回结果确认或重投，业务不自行 ACK / NAK / Term，仍需保证幂等。

连接文件可配置 `loggerName: "nats"`，并在 app.yaml 的 `log.names` 中声明 nats；连接、异步错误、订阅及消费链路写入 nats 命名日志。Handler 及其下游沿用消息 ctx 即可继承；HTTP/Cron 中发布 NATS 的调用日志继续跟随调用方 ctx。省略或未注册 Logger 名称时回退默认日志。

可选 `namespace` 配置在连接文件 `nats.events.yaml`，注册文件不接受该字段。非空值仅允许 ASCII 字母、数字、`_`、`-`；例如 `test1` 会将主题 `orders.created` 映射为 `test1.orders.created`，将 Stream `ORDERS` 映射为 `test1_ORDERS`，Durable、Core 名称及非空 Queue 也添加 `test1_`。注册与发布 / Request 均填写原始名称或主题，Handler 名称不变；回调主题已含前缀，原生 `Conn()` / `JetStream()` 不自动添加前缀。修改 namespace 不会迁移或删除旧资源，完整边界见 [`natsx` 包文档](infra/natsx/doc.go)。

NATS 消费者可在启动时保存客户端引用，也可在消费排空期间查询 `Default` / `Named` 并继续发布。关闭完成后查询仍按原约定 panic；其他客户端的关闭与查询行为不变。先停止并等待外部生产者，再由资源栈排空 NATS，随后关闭数据库等依赖，日志最后关闭。

OSS 文件必填 `region`、`bucket`、`accessKeyID`、`accessKeySecret`，可选 `endpoint`；`HeadObject` 查询元数据，`PresignGetObject` 签发下载 URL。`requestTimeout` 默认 60s，`slowThreshold` 默认 500ms，`logRequests` 默认关闭。初始化不联网校验 Bucket 或权限，业务通过 `ossx.Default()` / `Named(name)` 取得固定 Bucket 的实例。

STS 文件必填 `region`、`accessKeyID`、`accessKeySecret`，`endpoint` 留空时使用地域公网地址；`requestTimeout` 默认 60s，`slowThreshold` 默认 500ms，`logRequests` 默认关闭。业务通过 `stsx.Default()` / `Named(name)` 调用 `AssumeRole`，在请求中提供角色 ARN、会话名、时长和可选策略。OSS 上传可通过 [`biz/ossservice`](biz/ossservice/doc.go) 组合已有 OSS、STS 客户端，按指定对象构造上传策略并签发凭据；文件校验、Key 生成和记录保存由应用负责，STS 模块不依赖 Bucket。迁移旧配置时，从 OSS 文件移除 `stsRoleARN`、`stsSessionName`、`stsEndpoint`；前两项转入业务配置，`stsEndpoint` 改为 STS 文件的 `endpoint`。

阿里云 ES 管理端文件必填 `region`、`instanceID`、`accessKeyID`、`accessKeySecret`，可选 `endpoint`；`requestTimeout` 默认 60s，`slowThreshold` 默认 200ms，`logRequests` 默认关闭。初始化不联网验证实例或权限，业务通过 `esx.Default()` / `Named(name)` 取得固定 InstanceID 的客户端。

Elasticsearch 经 bootstrap 创建时，省略 `logRequests` 默认开启结果日志，`logDetails` 默认关闭；这与直接调用版本适配包 `New` 默认关闭结果日志不同。Redis 模板显式开启 `logCommands`，省略该字段则关闭正常命令日志，慢调用和错误日志仍会输出。完整参数与边界见 [`bootstrap` 包文档](biz/bootstrap/doc.go)。

### 显式执行 MySQL 表结构迁移

项目维护 `[]schemamigrate.Table` 模型清单，在 `MustBaseInit` 完成后、业务服务启动前调用 `bootstrap.RunMySQLMigration("orders", tables)`，并检查返回的报告与错误。名称对应已声明的 MySQL 实例，迁移在其主库执行；失败时停止启动，不忽略错误后开放业务入口。

将 [`migrate.example.yaml`](biz/bootstrap/conf.example/migrate.example.yaml) 复制为 `conf/migrate.orders.yaml`。调用迁移入口时文件必须存在，`enabled` 默认 false，显式开启后执行；`timeout` 默认 5 分钟。迁移不会自动挂到 `MustBaseInit` 或连接构造中，关闭或空清单不获取数据库。

每张表通过 `Model` 登记，可选 `Name` 指定物理表名、`Comment` 管理 MySQL 表注释。只迁移显式登记的表，关联表和多对多中间表须单独登记，不自动创建关联外键。没有跨实例迁移锁，发布时应避免多实例同时执行；DDL 失败或超时可能已部分生效，不能保证整批回滚。完整样例与报告语义见 [`schemamigrate` 包文档](infra/schemamigrate/doc.go)。

需要同时初始化 SQL 种子时使用组合入口，原 `RunMySQLMigration` 调用保持兼容：

```go
report, err := bootstrap.RunMySQLInitialization("orders", tables, bootstrap.SeedOptions{
    Files: seedFiles, // 项目内嵌的 embed.FS。
    Seeds: []schemamigrate.Seed{
        {Model: &OrderStatus{}, Where: map[string]any{"category": "default"}, File: "seeds/statuses.sql"},
    },
    Timeout: 3 * time.Minute,
})
```

项目保留 Model、表清单、种子声明和 SQL 文件。`enabled` 同时控制迁移与 seeds；关闭时不访问数据库或读取 SQL，迁移失败不会执行 seeds。表清单为空仍可对已有表执行种子。YAML 的 `timeout` 仅限制表结构迁移；`SeedOptions.Timeout` 从种子阶段开始独立计时，限制整批 seeds，零值默认 2 分钟、负值非法。返回的报告仅记录结构迁移结果，调用方必须处理任一阶段的错误。

种子 SQL 每个有效行必须是一条以分号结束、插入一行的 `INSERT INTO`；关键字不区分大小写，空行与 `--` 整行注释不计数。空文件、跨行 SQL 及其他语句报错。有效行数与 `Where` 范围内的物理记录数（含软删除记录）相同则保留内容；不同则事务 DELETE 该范围并重建，空 `Where` 明确作用于整表。每条 INSERT 必须影响一行，完成后再次核对范围内数量；失败回滚当前项的删除与插入并停止，之前完成的项保留。

SQL 是可信部署代码，调用方负责其目标表及条件正确，并使用支持事务的表；本组件不解析 SQL 或隔离权限。不重置自增序列，重建可能改变 ID 和覆盖自定义数据，只适用于可整体重建的受管数据。数量相同不会发现内容差异。底层可直接调用 `schemamigrate.ApplySeeds(ctx, masterDB, files, seeds)`，由调用方提供带期限的 Context；没有跨实例锁，须由单个实例在业务流量进入前执行。

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

更多示例：[`ginx` 请求处理](infra/ginx/doc.go)、[`httpserver` 启停](infra/httpserver/doc.go)、[`lrucache` 缓存](infra/lrucache/doc.go)、[`mysqlx` 主从查询](infra/mysqlx/doc.go)、[`natsx` 消息通信](infra/natsx/doc.go)、[`redislock` 锁流程](biz/lockx/redislock/doc.go)。

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
| Redis 锁 | `biz/lockx/redislock` 使用固定租期；Cron 通过 bootstrap 组合 gocron-redis-lock 的自动续租。同一资源的竞争者使用相同实现与协议，业务自行响应取消，并结合幂等、事务或数据端版本约束保护结果。 |
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

`make test` 和 `make test-race` 是完整测试入口，每个入口顺序执行常规测试与 NATS 配置注册集成测试两轮。等价的 Go 命令如下，CI 也应执行完整两轮：

```bash
go test ./...
go test -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$'

go test -race ./...
go test -race -tags=natsintegration ./biz/bootstrap -run '^TestNATSFromConfig$'
```

NATS 集成测试与被测代码保持同目录，通过 `natsintegration` build tag 编入内嵌服务端，无需预先安装服务端或在测试中另行构建可执行文件。常规测试不编入该组用例；带标签的第二轮仅运行 `TestNATSFromConfig`，其中不调用会修改 `time.Local` 的 `MustBaseInit`。不要使用 `go test -tags=natsintegration ./...` 混跑全部用例，否则 macOS 上服务端初始化的后台采样可能与时区初始化测试发生竞争。直接执行 `go test ./...` 或 `go test -race ./...` 只完成第一轮。

开发时遵循以下约定：

1. 先确认能力所属层次和职责边界，复用既有实现，保持变更聚焦。
2. 功能变更先用测试定义预期行为；修复问题时提供可复现的输入或场景。
3. 新增公开包时补充中文 `doc.go` 和调用示例；公开 API 变化同步更新文档。
4. 提交前运行 `make test` 与 `make test-race`，确保包含两轮测试，并使用 `gofmt` 格式化本次修改的 Go 文件。
5. 日志或轮转热路径变更需运行与功能等价的性能基准，保留正确性门禁和原始结果。

数据库、Redis 和 Elasticsearch 的真实服务验证需要相应环境；具体条件与跳过行为以各包测试为准。全量构建包含 SQLite 驱动，需要 CGO 与 C 编译器。

Cron 的真实 Redis 验收通过 `PIXIU_CRON_REDIS_ADDR` 指定独立测试实例的 `host:port`，然后执行 `go test ./biz/bootstrap -run TestCronRedisLeaseAndDrain`（也支持 `-race`）。测试使用随机 key 前缀，验证两个调度器互斥、跨 TTL 续租及停机解锁；未设置地址时跳过该项。

### 文档入口

- **使用与 API**：从上方模块导航进入各包的 `doc.go`，或查看 [Go Reference](https://pkg.go.dev/github.com/bpcoder16/pixiu)。
- **品牌资源**：[`assets/pixiu-logo.png`](assets/pixiu-logo.png)。
- **本地设计约定**：维护工作区的 `docs/` 保存设计与规划，作为项目约定的权威来源；该目录当前被 Git 忽略，不作为 GitHub 阅读入口。

## 许可证

貔貅采用 [Apache License 2.0](LICENSE)，允许商业使用、修改和分发，也允许集成到闭源项目。分发时须遵守许可证中的版权、修改声明及适用的 NOTICE 要求。

我们欢迎使用者将通用改进和问题修复贡献回社区；公开修改和提交贡献是自愿的，不是本许可证的强制要求。

第三方依赖保留各自的许可证，貔貅的授权不替代它们的许可条件。依赖版本见 [go.mod](go.mod)，使用与分发说明见 [第三方声明](THIRD_PARTY_NOTICES.md)；具体许可条款以各上游项目的许可证和相关声明为准。
