<p align="center">
  <img src="assets/pixiu-logo.png" width="160" alt="貔貅 Pixiu 吉祥物">
</p>

# 貔貅 · Pixiu

Pixiu 是一个可以按包使用的 Go 基础库。只需精确换算金额时，导入 `moneyx` 即可。编写 HTTP 服务时，可以分别选择日志、路由、客户端和服务退出管理。应用决定启用哪些组件、如何配置，以及何时释放资源。

## 安装

Go 版本要求为 **1.27.3 或更高版本**。所有包共用 `github.com/bpcoder16/pixiu` 这一个 Go module，在已有 Go 项目中按导入路径添加需要的包。

```sh
go get github.com/bpcoder16/pixiu/moneyx
```

顶层基础包的生产代码仅使用标准库，彼此独立。单独使用 `moneyx` 时，Pixiu 不会为程序引入第三方依赖。`infra/` 的依赖由实际导入的包决定。使用 SQLite 驱动时，需要启用 cgo 并准备 C 编译器。[模块定义](go.mod)

## 快速开始

### 把元转换为分

`moneyx` 用整数字段保存分，用十进制字符串表示元，避免金额经过浮点数。输入超过两位小数会返回错误。将下面的程序保存为 `main.go`，在模块目录执行 `go run .`，会输出 `1990 19.90`。

```go
package main

import (
	"fmt"
	"log"

	"github.com/bpcoder16/pixiu/moneyx"
)

func main() {
	cent, err := moneyx.YuanToCent("19.90")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(cent, moneyx.CentToYuan(cent))
}
```

### 在一次 HTTP 调用中关联日志与耗时

这个示例会启动一个临时本地 HTTP 服务并发送一次请求。在模块目录执行 `go get github.com/bpcoder16/pixiu/infra/httpcall`，再将下面的程序保存为 `main.go` 并运行。标准输出会出现两条 JSON 日志，共用同一个 `logId`。第二条的 `HttpCall_health_1_duration_ms` 是本次下游调用耗时，`total_duration_ms` 是从 `WithStart` 到汇总时的总耗时。

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/bpcoder16/pixiu/infra/httpcall"
	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	logger := logit.MustNew(
		logit.OptWriter(logit.Stdout()),
		logit.OptEncoder(logit.DefaultJSONEncoder),
	)
	logit.SetDefault(logger)

	err := checkHealth()
	if err := errors.Join(err, logit.Close(logger)); err != nil {
		log.Fatal(err)
	}
}

func checkHealth() error {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = logit.WithStart(logit.WithContextLogID(ctx))

	client := httpcall.New("health")
	resp, err := client.Request(ctx).Get(server.URL)
	if err != nil {
		return err
	}
	if resp.StatusCode() != http.StatusNoContent {
		return fmt.Errorf("健康检查返回 HTTP %d", resp.StatusCode())
	}
	logit.InfoDuration(ctx, "健康检查完成")
	return nil
}
```

在常驻服务中，启动时创建并复用 `httpcall.Client`，每次调用用 `Request(ctx)` 创建新请求。HTTP 4xx 和 5xx 需要从响应状态码判断。需要把已有的 `logId` 传给下游服务时，创建客户端时启用 `OptPropagateLogID(true)`。[HTTP 客户端用法](infra/httpcall/doc.go)

## 包导航

下面的链接指向各包的 `doc.go`，包含调用示例和使用约束。导入路径为 `github.com/bpcoder16/pixiu/` 加上表中的包路径。

### 基础模块

| 需要的能力 | 包 |
| --- | --- |
| 结构化日志、文本或有序 JSON 编码、上下文字段和耗时汇总 | [`logit`](logit/doc.go) |
| 按小时或天轮转文件、维护稳定软链、清理到期文件 | [`rotatefile`](rotatefile/doc.go) |
| 在统一期限内按登记逆序关闭资源 | [`lifecycle`](lifecycle/doc.go) |
| 在单次调用中并发执行任务，收集各任务的结果、错误和耗时 | [`conc`](conc/doc.go) |
| 解码 JSON 流的首个值，保留读取期间观察到的底层错误 | [`jsonx`](jsonx/doc.go) |
| HMAC-SHA256、AES-GCM 和密码学安全随机数 | [`cryptox`](cryptox/doc.go) |
| 查询本机 IPv4、IPv6 地址 | [`netx`](netx/doc.go) |
| 校验手机号、邮箱、HTTP URL、IP 地址等输入格式 | [`validx`](validx/doc.go) |
| 元文本与整数分互转、百分比文本与整数万分比互转 | [`moneyx`](moneyx/doc.go)、[`percentx`](percentx/doc.go) |

### 基础功能

| 需要的能力 | 包 |
| --- | --- |
| 读取应用名、运行模式、时区等运行环境信息 | [`infra/env`](infra/env/doc.go) |
| 将 YAML、TOML、JSON 配置文件严格映射为结构体，按名称读取配置 | [`infra/configx`](infra/configx/doc.go) |
| 监督常驻服务，统一停收、等待排空和取消 | [`infra/apprun`](infra/apprun/doc.go) |
| 向本地任务池提交异步任务，为任务建立独立日志作用域 | [`infra/taskpool`](infra/taskpool/doc.go) |
| 按 Cron 表达式、固定间隔或随机间隔调度任务 | [`infra/cronx`](infra/cronx/doc.go) |
| 托管标准 `http.Handler`，控制监听、停收与排空 | [`infra/httpserver`](infra/httpserver/doc.go) |
| 创建 Gin 路由，接入请求日志、panic 恢复、可选 CORS 和 OpenAPI 文档页 | [`infra/ginx`](infra/ginx/doc.go) |
| 复用 HTTP 客户端和连接池，记录下游请求及耗时 | [`infra/httpcall`](infra/httpcall/doc.go) |
| 通过标准 `http.Handler` 提供 WebSocket 连接、心跳、有界队列和认证回调 | [`infra/websocketx`](infra/websocketx/doc.go) |
| JWT 签发与验签，支持 HS256、RS256、ES256、PS256 | [`infra/jwtx`](infra/jwtx/doc.go) |
| 使用有容量上限、支持 TTL 的进程内 LRU 缓存 | [`infra/lrucache`](infra/lrucache/doc.go) |
| 创建 GORM 数据库连接池，显式选择主从会话或使用 SQLite 单库 | [`infra/mysqlx`](infra/mysqlx/doc.go)、[`infra/postgresqlx`](infra/postgresqlx/doc.go)、[`infra/clickhousex`](infra/clickhousex/doc.go)、[`infra/sqlitex`](infra/sqlitex/doc.go) |
| 为数据访问层注入主从会话，构建按原文匹配的 LIKE 查询 | [`infra/gormx`](infra/gormx/doc.go) |
| 对登记的模型执行 GORM AutoMigrate，校准 SQL 种子数据 | [`infra/schemamigrate`](infra/schemamigrate/doc.go) |
| 管理单机 Redis 客户端、连接验活和命令结果日志 | [`infra/redisx`](infra/redisx/doc.go) |
| NATS Core 发布、订阅和请求应答，JetStream 发布和消费 | [`infra/natsx`](infra/natsx/doc.go) |
| Elasticsearch 通用操作与日志，按服务端主版本选择官方客户端 | [`infra/elasticsearchx`](infra/elasticsearchx/doc.go)、[`infra/elasticsearchx/v7`](infra/elasticsearchx/v7/doc.go)、[`infra/elasticsearchx/v8`](infra/elasticsearchx/v8/doc.go)、[`infra/elasticsearchx/v9`](infra/elasticsearchx/v9/doc.go) |
| 为短文本检索构造 Elasticsearch 查询子句 | [`infra/elasticsearchx/textsearch`](infra/elasticsearchx/textsearch/doc.go) |
| 阿里云 OSS 对象操作、STS AssumeRole、Elasticsearch 实例与词典管理 | [`infra/aliyun/ossx`](infra/aliyun/ossx/doc.go)、[`infra/aliyun/stsx`](infra/aliyun/stsx/doc.go)、[`infra/aliyun/esx`](infra/aliyun/esx/doc.go) |

HTTP 身份认证已迁入 `github.com/bpcoder16/baize/httpauth`。Gin 路由通过 `github.com/bpcoder16/baize/middleware/ginauth` 直接绑定认证器。

## 组合与使用约定

- **请求上下文**。使用 `logit.AddField` 或 `AddMeta` 前，通过 `WithContext` 或 `WithContextLogID` 初始化字段存储。`ginx` 已为每个请求建立独立的耗时统计，在处理函数中向下传递 `c.Request.Context()` 即可。[日志](logit/doc.go) · [Gin](infra/ginx/doc.go)
- **日志内容**。`httpcall` 默认记录请求摘要，开启详情会记录请求头和可读取的正文。`redisx` 默认在失败和慢调用日志中记录完整参数。要省略键和值，设置 `redisx.Config.OmitLogArgs` 为 `true`，对单条命令、Pipeline 和 TxPipeline 均生效。[HTTP 日志](infra/httpcall/doc.go) · [Redis 日志](infra/redisx/doc.go)
- **文件与退出**。`rotatefile.New` 接受绝对路径，创建的轮转文件可以通过 `logit.NewWriter` 接入。使用 `lifecycle.Stack` 时先登记日志关闭函数，其他组件就能在释放资源时继续写日志。[轮转文件](rotatefile/doc.go) · [资源关闭顺序](lifecycle/doc.go)

## 开发与验证

在仓库根目录运行以下命令。全量测试包括 SQLite 驱动测试，并会编译本页的完整 Go 示例。

```sh
go build ./...
make test
make test-race
```

## 许可证

Pixiu 采用 Apache License 2.0。Go 依赖及内嵌 OpenAPI 文档页资源的版本、修改和归属见第三方声明。[项目许可证](LICENSE) · [第三方声明](THIRD_PARTY_NOTICES.md)
