// Package elasticSearchx 提供 Elasticsearch 7、8、9 共用的结果日志和基础操作。
// 版本适配包负责连接创建；配置、日志与结果约定见 docs/elasticSearchx-design.md。
//
// Config 中只有 Name 和 Addresses 必填，其余字段均可省略。
// 认证参数按服务端要求选择：APIKey 与 Username/Password 不能同时设置，
// Password 非空时必须设置 Username。CACert 在需要信任额外 CA 时提供。
// 未设置的 DialTimeout 默认 5 秒，SlowThreshold 默认 200 毫秒；
// 连接池零值保留默认设置，其中每节点最多保留 10 条空闲连接，其余继承默认 Transport。
// OptLogRequests 默认开启请求结果日志，OptLogDetails 默认关闭详情日志。
//
// 下例还需导入 context、time、github.com/bpcoder16/pixiu/infra/elasticSearchx/v8：
//
//	client, err := v8.New(ctx, elasticSearchx.Config{
//	    Name:                "catalog",                                    // 必填：实例名称
//	    Addresses:           []string{"https://search.example.com:9200"},   // 必填：至少一个节点地址
//	    APIKey:              apiKey,                                       // 可选：按服务端要求配置认证
//	    CACert:              caPEM,                                        // 可选：额外信任的 PEM CA 证书
//	    MaxIdleConns:        100,                                          // 可选：所有节点合计的空闲连接上限
//	    MaxIdleConnsPerHost: 20,                                           // 可选：每节点空闲连接上限
//	    MaxConnsPerHost:     50,                                           // 可选：每节点总连接上限
//	    IdleConnTimeout:     90 * time.Second,                             // 可选：空闲连接保留时间
//	},
//	    elasticSearchx.OptLogDetails(true), // 可选：采集请求体和必要响应信息
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Close(context.Background()) // 应用停止请求后、日志关闭前调用
//	result, err := client.Search(ctx, "products", queryDSL)
//	_ = result
//	return err
//
// 命名与默认实例由版本适配包创建，公共包负责查询和统一关闭：
// v7/v8/v9.NewNamed 按 Config.Name 登记，NewDefault 同时设置默认引用。
// elasticSearchx.Default() 与 elasticSearchx.Named(cfg.Name) 返回同一实例。
// 三个版本共用名称空间和一个默认实例；New 和 NewNamed 不设置默认引用。
// 名称重复或默认实例已存在时返回错误，初始化失败可重试；未知名称、默认未初始化
// 或 CloseAll 开始后查询会 panic。启动阶段须串行完成所有初始化后才开始业务，
// 运行期不再新增共享客户端，初始化不能与 CloseAll 并发，关闭后不得重新初始化。
//
// 以下样例结合 lifecycle 管理默认与命名客户端，另需导入 errors、logit、lifecycle、v8。
// ctx 是启动 context，logger 是已创建并通过 logit.SetDefault 设置的日志实例：
//
//	var stack lifecycle.Stack
//	// Stack 逆序关闭：日志最先登记，最后关闭。
//	if err := stack.Register(func() error {
//	    return logit.Close(logger)
//	}); err != nil {
//	    return err
//	}
//	if err := stack.Register(elasticSearchx.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err := v8.NewDefault(ctx, elasticSearchx.Config{
//	    Name:      "catalog", // 必填：默认实例也需提供名称
//	    Addresses: []string{"https://search.example.com:9200"}, // 必填
//	    APIKey:    apiKey, // 可选：按服务端要求配置
//	})
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err = v8.NewNamed(ctx, elasticSearchx.Config{
//	    Name:      "audit",
//	    Addresses: []string{"https://audit-search.example.com:9200"},
//	    APIKey:    apiKey,
//	})
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err = elasticSearchx.Default().Count(ctx, "products", queryDSL)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err = elasticSearchx.Named("audit").Count(ctx, "events", queryDSL)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	// 应用退出前，先停止并等待所有使用客户端的任务，再关闭 Stack。
//	return stack.Close()
//
// 共享实例只登记一次 CloseAll，不再单独登记默认或命名实例的 Close。
// CloseAll 使用 context.Background()，汇总关闭错误，重复调用复用首次结果。
// 独立 New 创建的实例不归 CloseAll 管理，仍需自行关闭 Client.Close(ctx)。
//
// OptLogRequests(false) 关闭所有请求结果日志，详情日志也随之关闭。
// 详情仅增加 request_body、response_proto、response_body、response_status_text，
// 不记录 Header；Body 不主动脱敏或截断。请求体从 GetBody 副本读取，
// 响应体随业务读取采集，普通日志在响应 Body 关闭时输出；直接使用 Perform 或
// esapi 时，调用方必须关闭 Body，提前关闭只记录已读取的部分。
// 基础操作会自行读取并关闭 Body，Bulk 在逐项解析后统一输出结果。
// ctx 已调用 logit.WithStart 时，每次实际请求还记录 elasticSearch_<Name>_<序号>
// 下游耗时，独立于日志开关和级别过滤；业务可调用 logit.InfoDuration 汇总，
// 例如 Name 为 catalog 时输出 elasticSearch_catalog_1_duration_ms。
package elasticSearchx
