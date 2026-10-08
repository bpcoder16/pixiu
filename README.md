<p align="center">
  <img src="assets/pixiu-logo.png" width="160" alt="貔貅 Pixiu 吉祥物：青玉色瑞兽，暖金瑞角与卷尾">
</p>

<h1 align="center">貔貅 · Pixiu</h1>

<p align="center">
  面向 Go 服务的分层复用库<br>
  独立原语 · 技术能力 · 通用业务流程
</p>

<p align="center">
  <a href="go.mod"><img src="https://img.shields.io/badge/Go-1.27.1%2B-00ADD8?logo=go&logoColor=white" alt="Go 1.27.1+"></a>
  <a href="https://pkg.go.dev/github.com/bpcoder16/pixiu"><img src="https://pkg.go.dev/badge/github.com/bpcoder16/pixiu.svg" alt="Go Reference"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache--2.0-blue.svg" alt="许可证 Apache-2.0"></a>
</p>

貔貅将 Go 服务中反复出现的工程能力沉淀为职责清楚、可按需引入的包：从结构化日志、文件轮转和并发任务，到 HTTP、数据库、缓存、消息通信，以及可复用的分布式锁流程。

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

当前 [go.mod](go.mod) 要求 **Go 1.27.1 或更高版本**。

在已有 Go module 的项目中安装需要的包：

```bash
go get github.com/bpcoder16/pixiu/logit
```

需要其他能力时，使用对应路径：

```bash
go get github.com/bpcoder16/pixiu/rotatefile
go get github.com/bpcoder16/pixiu/infra/httpcall
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
| [`infra/httpcall`](infra/httpcall/doc.go) | 可复用 HTTP 下游客户端、结果日志、请求级耗时 | Resty v2、logit |
| [`infra/httpserver`](infra/httpserver/doc.go) | 标准 `http.Handler` 服务端、监听、限时关闭 | 标准库 |
| [`infra/ginx`](infra/ginx/doc.go) | 独立 Gin Engine、请求日志作用域、访问日志与 Recovery | Gin、logit |
| [`infra/taskpool`](infra/taskpool/doc.go) | 本地异步任务、有界队列、worker 扩缩容、重试与排空 | logit |
| [`infra/lrucache`](infra/lrucache/doc.go) | 泛型有界 LRU、默认 / 单条目 TTL、独立及命名实例 | ttlcache v3 |
| [`infra/mysqlx`](infra/mysqlx/doc.go) | MySQL 连接池、显式主从选择、查询日志 | GORM、MySQL 驱动、logit |
| [`infra/pgsqlx`](infra/pgsqlx/doc.go) | PostgreSQL 连接池、显式主从选择、查询日志 | GORM、pgx、logit |
| [`infra/clickhousex`](infra/clickhousex/doc.go) | ClickHouse 连接池、显式主从选择、查询日志 | GORM、ClickHouse 驱动、logit |
| [`infra/sqlitex`](infra/sqlitex/doc.go) | SQLite 单库连接池、常用参数配置、查询日志 | GORM、go-sqlite3、logit |
| [`infra/redisx`](infra/redisx/doc.go) | 单机 Redis 客户端、初始化验活、命令结果日志 | go-redis v9、logit |
| [`infra/natsx`](infra/natsx/doc.go) | NATS 发布 / 请求 / 订阅、JetStream 发布与消费、统一日志 | nats.go、logit |
| [`infra/elasticSearchx`](infra/elasticSearchx/doc.go) | Elasticsearch 通用操作、Bulk、结果日志与版本适配 | 官方 Elasticsearch SDK、logit |

Elasticsearch 连接按服务端主版本选择 [`v7`](infra/elasticSearchx/v7/doc.go)、[`v8`](infra/elasticSearchx/v8/doc.go) 或 [`v9`](infra/elasticSearchx/v9/doc.go) 适配包；共用操作位于 `infra/elasticSearchx`。

### 通用业务功能 · biz

| 包 | 能力 | 典型用途 |
| --- | --- | --- |
| [`biz/lockx`](biz/lockx/doc.go) | 阻塞 / 非阻塞锁契约，`Do` / `TryDo` 协调获取、执行与释放 | 受锁保护的业务流程 |
| [`biz/lockx/redislock`](biz/lockx/redislock/doc.go) | 基于单机 Redis 的固定租期锁，原子校验身份并释放 | 同一协议下的多实例协调 |

### 规划中的能力

本地设计文档还规划了 `cronx`、`jwtx`、`websocketx` 等模块；这些包目前尚未实现，不属于当前可用 API。实际可用能力以上述模块导航为准。

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
│   ├── httpcall/           HTTP 下游调用
│   ├── httpserver/         HTTP 服务端
│   ├── ginx/               Gin 请求处理
│   ├── taskpool/           本地异步任务池
│   ├── lrucache/           LRU / TTL 缓存
│   ├── mysqlx/             MySQL
│   ├── pgsqlx/             PostgreSQL
│   ├── clickhousex/        ClickHouse
│   ├── sqlitex/            SQLite
│   ├── redisx/             Redis
│   ├── natsx/              NATS
│   ├── elasticSearchx/     Elasticsearch 与 v7 / v8 / v9 适配
│   └── internal/           infra 内部共用实现
├── biz/
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
