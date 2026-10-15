<p align="center">
  <img src="assets/pixiu-logo.png" width="160" alt="貔貅 Pixiu 吉祥物：青玉色瑞兽，暖金瑞角与卷尾">
</p>

<h1 align="center">貔貅 · Pixiu</h1>

<p align="center">
  面向 Go 服务的分层复用库<br>
  独立原语 · 技术能力 · 通用业务流程
</p>

<p align="center">
  <a href="go.mod"><img src="https://img.shields.io/badge/Go-1.27.2%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.27.2+"></a>
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

当前 [go.mod](go.mod) 要求 **Go 1.27.2 或更高版本**。

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

默认全局 Logger 已可直接使用，输出文本日志到 stdout。需要 JSON、级别过滤或专属写入目标时，在启动阶段通过 `New` / `MustNew` 构造实例；应用可使用 `SetDefault`，也可通过 `Logger` 接口进行依赖注入。

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
| [`netx`](netx/doc.go) | 本机 IPv4 / IPv6 地址查询 | 获取符合筛选规则的网卡地址 |

以上基础模块的生产代码仅依赖 Go 标准库，彼此不互相导入。

### 基础功能 · infra

导入时在下表路径前加上 `github.com/bpcoder16/pixiu/`。

| 包 | 能力 | 主要依赖 |
| --- | --- | --- |
| [`infra/apprun`](infra/apprun/doc.go) | 常驻服务运行监督、逆序关闭、显式 SIGINT/SIGTERM 接入 | 标准库、logit |
| [`infra/configx`](infra/configx/doc.go) | YAML / TOML / JSON 结构体配置加载、全局命名获取与零值兜底 | Viper |
| [`infra/env`](infra/env/doc.go) | 通用应用环境、IP 校验与补齐、一次初始化与全局读取 | 标准库、netx |
| [`infra/httpcall`](infra/httpcall/doc.go) | 可复用 HTTP 下游客户端、结果日志、请求级耗时 | Resty v2、logit |
| [`infra/httpserver`](infra/httpserver/doc.go) | 标准 `http.Handler` 服务端、监听、限时关闭 | 标准库 |
| [`infra/ginx`](infra/ginx/doc.go) | 独立 Gin Engine、请求日志作用域、访问日志、Recovery 与 OpenAPI / Elements 文档 | Gin、Swag、logit |
| [`infra/jwtx`](infra/jwtx/doc.go) | HS256 / RS256 / ES256 / PS256 JWT 签发、验签与声明校验 | golang-jwt/jwt v5 |
| [`infra/taskpool`](infra/taskpool/doc.go) | 本地异步任务、有界队列、worker 扩缩容、重试与排空 | logit |
| [`infra/lrucache`](infra/lrucache/doc.go) | 泛型有界 LRU、默认 / 单条目 TTL、独立及命名实例 | ttlcache v3 |
| [`infra/mysqlx`](infra/mysqlx/doc.go) | MySQL 连接池、显式主从选择、查询日志 | GORM、MySQL 驱动、logit |
| [`infra/gormx`](infra/gormx/doc.go) | Repository 主、从库会话依赖 Sessions | GORM |
| [`infra/schemamigrate`](infra/schemamigrate/doc.go) | 统一 AutoMigrate 入口、物理分表与 MySQL 表注释 | GORM |
| [`infra/pgsqlx`](infra/pgsqlx/doc.go) | PostgreSQL 连接池、显式主从选择、查询日志 | GORM、pgx、logit |
| [`infra/clickhousex`](infra/clickhousex/doc.go) | ClickHouse 连接池、显式主从选择、查询日志 | GORM、ClickHouse 驱动、logit |
| [`infra/sqlitex`](infra/sqlitex/doc.go) | SQLite 单库连接池、常用参数配置、查询日志 | GORM、go-sqlite3、logit |
| [`infra/redisx`](infra/redisx/doc.go) | 单机 Redis 客户端、初始化验活、命令结果日志 | go-redis v9、logit |
| [`infra/natsx`](infra/natsx/doc.go) | NATS 发布 / 请求 / 订阅、JetStream 发布与消费、统一日志 | nats.go、logit |
| [`infra/elasticsearchx`](infra/elasticsearchx/doc.go) | Elasticsearch 通用操作、Bulk、默认及命名实例、结果日志与版本适配 | 官方 Elasticsearch SDK、logit |
| [`infra/aliyun/ossx`](infra/aliyun/ossx/doc.go) | OSS 固定 Bucket 的流与文件上传、独立账号、结果日志与生命周期 | 阿里云 OSS SDK V2、credentials-go、logit |
| [`infra/aliyun/esx`](infra/aliyun/esx/doc.go) | 阿里云 ES 固定 InstanceID 的同义词清单、完整清单更新、实例查询 | 阿里云 Elasticsearch 管理 SDK、credentials-go、logit |

Elasticsearch 连接按服务端主版本选择 [`v7`](infra/elasticsearchx/v7/doc.go)、[`v8`](infra/elasticsearchx/v8/doc.go) 或 [`v9`](infra/elasticsearchx/v9/doc.go) 适配包；共用操作位于 `infra/elasticsearchx`。各版本的 `NewDefault` / `NewNamed` 共用注册表，通过 `elasticsearchx.Default()` / `Named(name)` 获取实例，应用统一调用 `CloseAll()` 关闭；独立 `New` 创建的实例仍需自行关闭。

独立 Elasticsearch 客户端默认关闭请求结果日志，通过 `OptLogRequests(true)` 开启；请求体和响应详情还需开启 `OptLogDetails(true)`。context 已调用 `logit.WithStart` 时，关闭结果日志仍会记录请求级下游耗时。`Search` 超时或分片失败会保留已有结果并返回 `PartialSearchError`；`Bulk` 动作失败返回 `BulkError` 并保留成功数与失败项，调用方应检查错误后再决定是否接受结果或重试。

`infra/aliyun/ossx` 和 `infra/aliyun/esx` 独立持有配置、凭据、连接池和命名注册表，可使用不同账号。两者均支持成对配置 `AccessKeyID` / `AccessKeySecret`，或注入与其互斥的 `Credential`；`New` 只构造，不联网验活，结果日志默认关闭。共享实例使用 `NewNamed` / `NewDefault` 创建并统一 `CloseAll`；独立 `New` 创建的实例自行关闭。

OSS 每个实例固定一个 `Config.Bucket`，阿里云 ES 管理端每个实例固定一个 `Config.InstanceID`。`UpdateSynonymsDicts` 提交完整目标清单，遗漏已有文件会删除它；API 成功不代表分发或索引加载完成。凭据、上传及词典更新的完整边界见对应包文档。

### 通用业务功能 · biz

| 包 | 能力 | 典型用途 |
| --- | --- | --- |
| [`biz/httpauth`](biz/httpauth/doc.go) / [`ginauth`](biz/httpauth/ginauth/doc.go) | 命名认证方案、统一身份、标准 Context 与 Gin 中间件 | 支持多平台认证，按平台注册认证实现，由路由显式选择认证方案 |
| [`biz/bootstrap`](biz/bootstrap/doc.go) | 应用时区、日志及多实例初始化，运行管理器、Gin / HTTP 服务构造与显式 MySQL 迁移入口 | 声明实例、登记关闭栈；应用持有 Manager 并注册 HTTP 服务，日志最后关闭 |
| [`biz/elasticsearchservice`](biz/elasticsearchservice/doc.go) | 固定 standard → lowercase → synonym_graph，创建临时索引严格验证并清理 | 通过 `ValidateSynonyms` 验证 `synonyms_path`，保留 ES 错误原因与清理状态 |
| [`biz/httpconfig`](biz/httpconfig/doc.go) | 嵌入 baseconfig，严格解析 HTTP 应用配置、返回配置指针并初始化环境 | 从 [`app.yaml` 模板](biz/httpconfig/conf.example/app.yaml) 加载配置，不在 configx 注册全局配置 |
| [`biz/lockx`](biz/lockx/doc.go) | 阻塞 / 非阻塞锁契约，`Do` / `TryDo` 协调获取、执行与释放 | 受锁保护的业务流程 |
| [`biz/lockx/redislock`](biz/lockx/redislock/doc.go) | 基于单机 Redis 的固定租期锁，原子校验身份并释放 | 同一协议下的多实例协调 |

`biz/internal/baseconfig` 供 biz 内部共用环境与日志配置；业务项目通过 `httpconfig.AppConfig` 的嵌入字段使用，无需直接导入内部包。

`httpconfig.MustLoadAppConfig` 使用 `configx.Parse`，支持 YAML / TOML / JSON，拒绝未知字段和弱类型转换；配置只返回给调用方，环境由 `env.Init` 一次性发布。应用配置文件所在目录作为 `env.ConfigDirPath()`，供 bootstrap 查找实例配置。加载配置不会初始化日志或修改 `time.Local`；`bootstrap.MustBaseInit` 在日志创建前应用环境时区，再依次初始化日志、MySQL、Redis、Elasticsearch、OSS、阿里云 ES 和 NATS，仅初始化已声明的组件实例。

初始化完成后，应用可调用 `bootstrap.NewAppManager()` 创建默认总关闭期限为 20 秒的管理器，也可传入不小于 5 秒的自定义时长，传入 0 仍使用默认值。`bootstrap.NewHTTPService(handler)` 从 `env.ConfigDirPath()` 下严格加载 `http.yaml` 并返回可注册的 `apprun.Service`，HTTP 关闭期限默认 15 秒。应用显式调用 `manager.Register(service)` 和 `manager.RunSignals()`，管理器返回后再关闭共享资源栈。

### 规划中的能力

本地设计文档还规划了 `cronx`、`websocketx` 等模块；这些包目前尚未实现，不属于当前可用 API。实际可用能力以上述模块导航为准。

## 分层架构

```text
pixiu/
├── logit/                 结构化日志与请求上下文
├── rotatefile/            通用时间轮转文件
├── lifecycle/             资源关闭栈
├── conc/                  单次调用的并发任务编排
├── jsonx/                 JSON 首值解码
├── netx/                  网络基础能力
├── infra/                 通用技术能力与 SDK 适配
│   ├── apprun/             常驻服务运行监督与关闭
│   ├── configx/            结构体配置解析与命名获取
│   ├── env/                通用应用运行环境
│   ├── httpcall/           HTTP 下游调用
│   ├── httpserver/         HTTP 服务端
│   ├── ginx/               Gin 请求处理
│   ├── jwtx/               JWT 签发、验签与声明校验
│   ├── taskpool/           本地异步任务池
│   ├── lrucache/           LRU / TTL 缓存
│   ├── mysqlx/             MySQL
│   ├── gormx/              Repository 主、从库会话依赖
│   ├── schemamigrate/      显式表结构迁移与 MySQL 表注释
│   ├── pgsqlx/             PostgreSQL
│   ├── clickhousex/        ClickHouse
│   ├── sqlitex/            SQLite
│   ├── redisx/             Redis
│   ├── natsx/              NATS
│   ├── elasticsearchx/     Elasticsearch 与 v7 / v8 / v9 适配
│   ├── aliyun/             阿里云 SDK 适配
│   │   ├── ossx/           阿里云 OSS 上传
│   │   └── esx/            阿里云 ES 管理 API
│   └── internal/           infra 内部共用实现
├── biz/
│   ├── bootstrap/          组件初始化、运行管理器与 HTTP 服务构造
│   ├── elasticsearchservice/ 同义词库索引验证与临时资源清理
│   ├── httpconfig/         HTTP 应用配置加载与环境初始化
│   ├── httpauth/           命名认证方案与统一身份
│   │   └── ginauth/        Gin 认证中间件
│   ├── internal/           biz 内部共用实现
│   │   └── baseconfig/     各类应用共用的环境与日志配置
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

将 [`app.yaml`](biz/httpconfig/conf.example/app.yaml) 和 [`http.yaml`](biz/bootstrap/conf.example/http.yaml) 复制到应用的 `conf/` 目录，修改应用名称。保存以下代码为 `main.go`，安装 `biz/bootstrap` 后执行 `go run .`，再访问 `http://127.0.0.1:8888/healthz`，应返回 HTTP 204；在终端按 Ctrl+C 关闭。

<details>
<summary>展开完整示例（无需外部服务）</summary>

```go
package main

import (
	"errors"
	"fmt"
	"net/http"
	"os"

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	cfg := httpconfig.MustLoadAppConfig("./conf/app.yaml")
	var resources lifecycle.Stack
	defer func() {
		runErr = errors.Join(runErr, resources.Close())
	}()
	bootstrap.MustBaseInit(&cfg.AppConfig, &resources)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	manager, err := bootstrap.NewAppManager()
	if err != nil {
		return err
	}
	service, err := bootstrap.NewHTTPService(mux)
	if err != nil {
		return err
	}
	if err := manager.Register(service); err != nil {
		return errors.Join(err, service.Shutdown())
	}
	return manager.RunSignals()
}
```

</details>

`NewHTTPService` 构造时不监听端口；`RunSignals` 启动服务，并在收到 SIGINT / SIGTERM 时关闭。注册成功的服务由管理器关闭，无需重复登记到资源栈。HTTP 排空默认 15 秒，管理器总关闭期限默认 20 秒；超时后可能仍有未退出的 handler，应用需处理返回错误并尽力清理后退出。共享资源清理时间不计入管理器总期限。

通过 bootstrap 初始化后，业务日志默认写入 `<dir>/<appName>/biz/` 下的 `debug.log`、`info.log`、`wf.log`，分别对应 Debug、Info 和 Warn / Error / Fatal；命名 Logger 增加 `<name>.` 文件前缀。`dir` 默认 `./log`，相对应用启动目录解析；release 输出 Info 及以上，debug/test 输出 Debug 及以上。

`log.hookStd` 省略时默认开启，模板也显式开启：stdout / stderr 原始输出分别追加到 `<dir>/<appName>/std/stdout.log` 和 `stderr.log`，文件不轮转，重定向持续至进程退出。该能力仅支持 Linux/macOS；其他平台或需要保留终端标准输出时，在 `app.yaml` 的 `log` 段显式设置 `hookStd: false`。业务日志仍按上述文件分流。

### Gin 与 OpenAPI 文档

使用 Gin 时，在初始化环境后显式调用 `gin.SetMode(env.RunMode())`，通过 `bootstrap.NewGinEngine()` 构造 Engine，注册业务路由，再将 Engine 传给 `bootstrap.NewHTTPService`。需额外导入 Gin 和 `infra/env`；将 [`gin.yaml`](biz/bootstrap/conf.example/gin.yaml) 放在同一配置目录。文件必须存在，内容为 `{}` 时沿用默认值。

`NewGinEngine` 根据应用环境决定文档路由：

- **debug/test**：自动注册 OpenAPI 与内嵌的 Stoplight Elements。应用须先使用 Swag CLI 生成文档（例如 `swag init -o ./openapi`），并在入口 blank import 生成包；未注册文档时构造返回错误。默认页面为 `/docs`，文档为 `/openapi/doc.json`。
- **release**：不自动注册文档路由，也不要求生成文档。

多模块应用可向 `NewGinEngine` 传入 `ginx.OpenAPIModule`，以 `Name` 对应 Swag 的命名文档实例，`Title` 作为导航标题。每个模块提供 `/openapi/<Name>.json` 和 `/docs/<Name>`，`/docs` 展示首个模块；静态资源位于 `/docs/assets/`。旧 `/elements` 及其子路径不注册，也不重定向。生成参数、Bearer 前缀补齐等用法见 [`ginx` 包文档](infra/ginx/doc.go)。

文档路由在 Engine 构造时注册，之后添加的业务中间件不会作用于它们，也不会自动增加认证。需要控制文档访问时，可使用 `ginx.New`，在带认证中间件的路由组调用 `ginx.RegisterOpenAPI`。`gin.yaml` 模板开启完整请求和响应详情；使用文档 UI 时建议关闭 `logResponseInfo`，避免记录完整静态资源。

### 应用配置与多实例初始化

将 [`app.yaml`](biz/httpconfig/conf.example/app.yaml) 复制到应用的 `conf/app.yaml`。本例声明六个组件实例，需将对应模板复制到同一配置目录，并填写实际连接信息：

| 声明 | 配置文件 | 模板 |
| --- | --- | --- |
| `MustRegisterMySQL("orders", true)` | `conf/mysql.orders.yaml` | [MySQL](biz/bootstrap/conf.example/mysql.example.yaml) |
| `MustRegisterRedis("cache", true)` | `conf/redis.cache.yaml` | [Redis](biz/bootstrap/conf.example/redis.example.yaml) |
| `MustRegisterElasticsearch("search", true)` | `conf/elasticsearch.search.yaml` | [Elasticsearch](biz/bootstrap/conf.example/elasticsearch.example.yaml) |
| `MustRegisterAliyunOSS("assets", true)` | `conf/aliyunossx.assets.yaml` | [阿里云 OSS](biz/bootstrap/conf.example/aliyunossx.example.yaml) |
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

	"github.com/bpcoder16/pixiu/biz/bootstrap"
	"github.com/bpcoder16/pixiu/biz/httpconfig"
	"github.com/bpcoder16/pixiu/lifecycle"
	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	cfg := httpconfig.MustLoadAppConfig("./conf/app.yaml")
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
	bootstrap.MustBaseInit(&cfg.AppConfig, &resources)
	logit.Info(context.Background(), "应用初始化完成")
}
```

</details>

各组件独立选择默认实例，`Default()` 与 `Named(默认名称)` 返回同一客户端。共享组件由 bootstrap 各登记一次 `CloseAll`，业务无需重复登记单个客户端关闭；应用退出前先停止并等待业务任务，再关闭资源栈。配置或初始化失败会 panic，提前登记的 defer 负责回收已创建资源；初始化只在启动阶段串行执行，失败后不支持重试。

此示例只展示初始化；常驻 HTTP 应用还需接入上面的服务管理器。使用 NATS 消费时，在回调依赖就绪后注册订阅，消费者在启动时保存客户端引用，排空期间不再查询 `Default` / `Named`。先停止并等待外部生产者，再由资源栈排空 NATS，随后关闭数据库等依赖，日志最后关闭。

OSS 文件必填 `region`、`bucket`、`accessKeyID`、`accessKeySecret`，可选 `endpoint`；`requestTimeout` 默认 60s，`slowThreshold` 默认 500ms，`logRequests` 默认关闭。初始化不联网校验 Bucket 或权限，业务通过 `ossx.Default()` / `Named(name)` 取得固定 Bucket 的实例。

阿里云 ES 管理端文件必填 `region`、`instanceID`、`accessKeyID`、`accessKeySecret`，可选 `endpoint`；`requestTimeout` 默认 60s，`slowThreshold` 默认 200ms，`logRequests` 默认关闭。初始化不联网验证实例或权限，业务通过 `esx.Default()` / `Named(name)` 取得固定 InstanceID 的客户端。

Elasticsearch 经 bootstrap 创建时，省略 `logRequests` 默认开启结果日志，`logDetails` 默认关闭；这与直接调用版本适配包 `New` 默认关闭结果日志不同。Redis 模板显式开启 `logCommands`，省略该字段则关闭正常命令日志，慢调用和错误日志仍会输出。完整参数与边界见 [`bootstrap` 包文档](biz/bootstrap/doc.go)。

### 显式执行 MySQL 表结构迁移

项目维护 `[]schemamigrate.Table` 模型清单，在 `MustBaseInit` 完成后、业务服务启动前调用 `bootstrap.RunMySQLMigration("orders", tables)`，并检查返回的报告与错误。名称对应已声明的 MySQL 实例，迁移在其主库执行；失败时停止启动，不忽略错误后开放业务入口。

将 [`migrate.example.yaml`](biz/bootstrap/conf.example/migrate.example.yaml) 复制为 `conf/migrate.orders.yaml`。调用迁移入口时文件必须存在，`enabled` 默认 false，显式开启后执行；`timeout` 默认 5 分钟。迁移不会自动挂到 `MustBaseInit` 或连接构造中，关闭或空清单不获取数据库。

每张表通过 `Model` 登记，可选 `Name` 指定物理表名、`Comment` 管理 MySQL 表注释。只迁移显式登记的表，关联表和多对多中间表须单独登记，不自动创建关联外键。没有跨实例迁移锁，发布时应避免多实例同时执行；DDL 失败或超时可能已部分生效，不能保证整批回滚。完整样例与报告语义见 [`schemamigrate` 包文档](infra/schemamigrate/doc.go)。

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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
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
	"time"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
	"github.com/go-resty/resty/v2"
)

func main() {
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
	"fmt"
	"os"
	"time"

	"github.com/bpcoder16/pixiu/infra/taskpool"
	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
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

`Submit` 成功只表示任务已接收。任务保存在内存中；需要进程异常退出后的可靠恢复时，应用应选择持久化消息队列。示例通过 `Shutdown` 发起排空，再用 `Wait` 确认 worker 退出；`Wait` 没有等待上限。

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
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
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
| Redis 锁 | 内置锁使用固定租期，不自动续租。同一资源的竞争者使用相同实现与协议，业务自行响应取消，并结合幂等、事务或数据端版本约束保护结果。 |
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
go test ./...
go test -race ./...
```

开发时遵循以下约定：

1. 先确认能力所属层次和职责边界，复用既有实现，保持变更聚焦。
2. 功能变更先用测试定义预期行为；修复问题时提供可复现的输入或场景。
3. 新增公开包时补充中文 `doc.go` 和调用示例；公开 API 变化同步更新文档。
4. 提交前运行全量测试与 race 检查，并使用 `gofmt` 格式化本次修改的 Go 文件。
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
