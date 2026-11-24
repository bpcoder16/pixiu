<p align="center">
  <img src="assets/pixiu-logo.png" width="160" alt="貔貅 Pixiu 吉祥物">
</p>

# 貔貅 · Pixiu

**Pixiu 是一个可以按包使用的 Go 后端基础库，涵盖日志、HTTP 服务、消息处理和数据访问。** 你可以只用 `logit` 给现有服务加上结构化日志，也可以把 Gin 入口、下游 HTTP 调用和数据库查询串在同一个请求上下文里，在一条汇总日志中看到时间花在哪里。

顶层包彼此独立，生产代码只依赖 Go 标准库，适合直接用在服务或命令行工具中。例如，`moneyx.YuanToCent("19.90")` 会得到整数 `1990` 分。`infra/` 中的组件封装 Gin、GORM、NATS 等库，应用按需创建客户端、注册路由或消息处理函数，并控制服务的启动和关闭。

[快速开始](#快速开始) · [按场景选包](#按场景选包) · [HTTP 调用示例](#http-调用示例) · [开发与验证](#开发与验证)

## 快速开始

需要 **Go 1.27.3 或更高版本**。下面新建一个 Go module，用同一个 `logId` 关联两条 JSON 日志。已有项目只需在模块目录执行最后一条命令。

```sh
mkdir hello-pixiu
cd hello-pixiu
go mod init example.com/hello-pixiu
go get github.com/bpcoder16/pixiu/logit@v0.2.12
```

把下面的代码保存为 `main.go`，然后运行 `go run .`。

```go
package main

import (
	"context"

	"github.com/bpcoder16/pixiu/logit"
)

func main() {
	logger := logit.MustNew(
		logit.OptWriter(logit.Stdout()),
		logit.OptEncoder(logit.DefaultJSONEncoder),
	)
	defer logit.Close(logger)

	ctx := logit.WithContextLogID(context.Background())
	logit.AddField(ctx, logit.Str("job", "catalog-sync"))
	logger.Info(ctx, "开始同步")
	logger.Info(ctx, "同步完成")
}
```

两条日志会带上相同的 `logId` 和 `job`。一次运行的输出节选如下，省略了级别和时间戳。

```jsonl
{"logId":"6943c048-b4b9-4685-9be8-b05810d34ec3","job":"catalog-sync","msg":"开始同步"}
{"logId":"6943c048-b4b9-4685-9be8-b05810d34ec3","job":"catalog-sync","msg":"同步完成"}
```

这里直接使用创建好的 Logger。希望 Pixiu 的 HTTP、数据库等组件也写入同一目标时，在应用启动时调用 `logit.SetDefault(logger)`。[日志用法](logit/doc.go)

## 按场景选包

所有包都在 `github.com/bpcoder16/pixiu` 这一个 Go module 下。下表中的包名也是相对导入路径，点开可查看该包的调用示例。使用 SQLite 驱动还需启用 cgo 并准备 C 编译器。[模块定义](go.mod)

### 日志、并发与应用退出

| 想做什么 | 使用的包 |
| --- | --- |
| 输出结构化日志，把请求字段、下游耗时和总耗时放进同一条记录 | [`logit`](logit/doc.go) |
| 按小时或天轮转文件，通过稳定路径访问当前文件 | [`rotatefile`](rotatefile/doc.go) |
| 并发执行一组任务，按名称收集结果和错误 | [`conc`](conc/doc.go) |
| 在一个总期限内按登记逆序关闭资源 | [`lifecycle`](lifecycle/doc.go) |
| 让多个常驻服务一起运行，退出时停止接收新工作并等待在途工作结束 | [`infra/apprun`](infra/apprun/doc.go) |
| 提交本地后台任务，或按 Cron 表达式、固定及随机间隔调度 | [`infra/taskpool`](infra/taskpool/doc.go)、[`infra/cronx`](infra/cronx/doc.go) |

使用文件日志时，把绝对路径交给 `rotatefile.New`，再通过 `logit.NewWriter` 接入日志。使用 `lifecycle.Stack` 回收资源时，先登记日志关闭函数，数据库和其他组件在收尾时仍可写日志。

### HTTP、长连接与消息

| 想做什么 | 使用的包 |
| --- | --- |
| 给标准 `http.Handler` 配置监听、超时和优雅关闭 | [`infra/httpserver`](infra/httpserver/doc.go) |
| 创建 Gin 路由，记录访问日志，恢复 panic，按需启用 CORS 和 OpenAPI 文档页 | [`infra/ginx`](infra/ginx/doc.go) |
| 复用 HTTP 连接池，自动记录每次下游调用的状态和耗时 | [`infra/httpcall`](infra/httpcall/doc.go) |
| 接入 WebSocket，管理连接、心跳、收发队列和握手认证回调 | [`infra/websocketx`](infra/websocketx/doc.go) |
| 签发和验证 JWT，在路由中间件中校验签名和声明 | [`infra/jwtx`](infra/jwtx/doc.go) |
| 使用 NATS Core 发布、订阅和请求应答，或用 JetStream 发布、消费并确认消息 | [`infra/natsx`](infra/natsx/doc.go) |

已有 Gin 路由可以直接作为 `http.Handler` 交给 `httpserver`。在 `ginx` 的处理函数中把 `c.Request.Context()` 传给下游客户端，访问日志就可以汇总本次请求的下游耗时。

### 数据、搜索与云服务

| 想做什么 | 使用的包 |
| --- | --- |
| 管理 GORM 连接池，显式选择 MySQL、PostgreSQL 或 ClickHouse 的主从会话 | [`infra/mysqlx`](infra/mysqlx/doc.go)、[`infra/postgresqlx`](infra/postgresqlx/doc.go)、[`infra/clickhousex`](infra/clickhousex/doc.go) |
| 打开 SQLite 文件或内存数据库 | [`infra/sqlitex`](infra/sqlitex/doc.go) |
| 给数据访问层注入主从会话，或对包含通配符的原文做 LIKE 匹配 | [`infra/gormx`](infra/gormx/doc.go) |
| 迁移应用登记的 GORM 模型，按记录数校准 SQL 种子数据 | [`infra/schemamigrate`](infra/schemamigrate/doc.go) |
| 连接单机 Redis，或在进程内使用有容量上限和 TTL 的 LRU 缓存 | [`infra/redisx`](infra/redisx/doc.go)、[`infra/lrucache`](infra/lrucache/doc.go) |
| 按 Elasticsearch 服务端主版本创建客户端，再通过统一接口读写索引和文档 | [`infra/elasticsearchx/v7`](infra/elasticsearchx/v7/doc.go)、[`infra/elasticsearchx/v8`](infra/elasticsearchx/v8/doc.go)、[`infra/elasticsearchx/v9`](infra/elasticsearchx/v9/doc.go)、[`infra/elasticsearchx`](infra/elasticsearchx/doc.go) |
| 为短文本检索构建 Elasticsearch 查询子句 | [`infra/elasticsearchx/textsearch`](infra/elasticsearchx/textsearch/doc.go) |
| 上传 OSS 对象、通过 STS 获取临时凭据、管理阿里云 Elasticsearch 实例和词典 | [`infra/aliyun/ossx`](infra/aliyun/ossx/doc.go)、[`infra/aliyun/stsx`](infra/aliyun/stsx/doc.go)、[`infra/aliyun/esx`](infra/aliyun/esx/doc.go) |

### 配置与基础工具

| 想做什么 | 使用的包 |
| --- | --- |
| 将 YAML、TOML 或 JSON 配置按字段映射到 Go 结构体 | [`infra/configx`](infra/configx/doc.go) |
| 在应用启动时初始化应用名、运行模式和时区，供其他包读取 | [`infra/env`](infra/env/doc.go) |
| 在元与整数分、百分比文本与整数万分比之间精确换算 | [`moneyx`](moneyx/doc.go)、[`percentx`](percentx/doc.go) |
| 读取 JSON 流的首个值，同时保留解码过程中发现的底层读取错误 | [`jsonx`](jsonx/doc.go) |
| 使用 HMAC-SHA256、AES-GCM 或生成密码学安全随机数 | [`cryptox`](cryptox/doc.go) |
| 查询本机 IPv4、IPv6 地址，或校验中国大陆手机号、邮箱、HTTP URL 和 IP 的格式 | [`netx`](netx/doc.go)、[`validx`](validx/doc.go) |

## HTTP 调用示例

一次请求慢在哪里？`httpcall` 把下游调用的耗时写入 `context`，`logit.InfoDuration` 在请求结束时输出汇总。下面的程序会启动临时本地 HTTP 服务并访问一次，只需 Go 环境。

在快速开始的目录运行 `go get github.com/bpcoder16/pixiu/infra/httpcall@v0.2.12`，将 `main.go` 替换为下方程序，再运行 `go run .`。

<details>
<summary>查看完整的 HTTP 示例</summary>

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

</details>

按同一个 `logId` 可以查到两条记录。

| 消息 | 可以看到什么 |
| --- | --- |
| `HttpCall` | 客户端名称 `health`、请求方法、URL、HTTP 状态和调用耗时 |
| `健康检查完成` | `HttpCall_health_1_duration_ms` 是第一次调用的耗时，`total_duration_ms` 是从 `WithStart` 到汇总的总耗时 |

常驻服务应在启动时创建并复用 `httpcall.Client`，每次调用用 `Request(ctx)` 创建请求，并从响应状态码判断 HTTP 4xx 和 5xx。需要向下游服务传递已有 `logId` 时，启用 `OptPropagateLogID(true)`。[HTTP 客户端用法](infra/httpcall/doc.go)

`httpcall` 默认记录请求摘要，开启详情会记录原始请求头和可读取的正文。`redisx` 默认在慢调用和错误日志中记录完整命令参数，设置 `OmitLogArgs: true` 可省略键和值。[HTTP 日志](infra/httpcall/doc.go) · [Redis 日志](infra/redisx/doc.go)

## 开发与验证

在仓库根目录运行以下命令。全量测试会编译本页的完整 Go 程序，覆盖 SQLite 驱动，并使用回环端口启动本地测试服务。

```sh
go build ./...
make test
make test-race
```

## 许可证

Pixiu 使用 Apache License 2.0。第三方声明列出了 Go 依赖的版本和许可，以及内嵌 OpenAPI 文档页资源的来源和修改。[项目许可证](LICENSE) · [第三方声明](THIRD_PARTY_NOTICES.md)
