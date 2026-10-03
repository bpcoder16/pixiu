// Package elasticSearchx 提供 Elasticsearch 7、8、9 共用的结果日志和基础操作。
// 版本适配包负责连接创建；配置、日志与结果约定见 docs/elasticSearchx-design.md。
// v8/v9 使用官方 NewBase 和共用的 transport v8；启动验活只核对服务端主版本，
// 不保证所有旧次版本均兼容，真实集群验收版本见设计文档。
//
// Config 中只有 Name 和 Addresses 必填，其余字段均可省略。
// 认证参数按服务端要求选择：APIKey 与 Username/Password 不能同时设置，
// Password 非空时必须设置 Username。CACert 在需要信任额外 CA 时提供。
// 未设置的 DialTimeout 默认 5 秒，SlowThreshold 默认 200 毫秒；
// 连接池零值保留默认设置，其中每节点最多保留 10 条空闲连接，其余继承默认 Transport。
// 地址必须包含非空主机名；Transport 清除继承的 TLS 专用拨号器，统一执行拨号与证书验证。
// TLS 最低版本为 1.2，默认 Transport 已设置更严格的版本下限时保留它。
// 不启用 SDK 自动重试；Go HTTP Transport 自身仍遵循标准库的安全重发规则。
// v7 适配层包装 EOF 以规避 SDK 重试缺陷，调用方可用 errors.Is(err, io.EOF) 判断。
// 不启用节点发现；Close 后拒绝新的操作，在途操作应由应用先行停止。
// v8/v9 的 SDK 后台恢复任务由 Close 收尾；v7 没有对应关闭接口，
// 已安排的恢复定时器仍会触发，但仅更新连接状态，不主动发起探测请求。
// 请求结果日志默认关闭，OptLogRequests(true) 显式开启；OptLogDetails 默认关闭详情日志。
// 启动验活不输出请求结果日志或登记下游耗时，失败时仍返回错误。
//
// Search 返回扁平结果，Total 为 nil 表示未统计总数；非 nil 时 Relation 为 eq 表示
// 准确数量，gte 表示下限。
// 超时或分片失败时，Search 保留结果及状态，不额外返回错误；
// 调用方应检查 TimedOut 和 Shards.Failed，决定是否接受部分结果。
// Count 无法携带分片状态，分片失败时返回零值及错误，避免接受部分计数。
// Took（服务端耗时）和 TerminatedEarly（提前终止标记）暂以注释保留，
// 需要时同步恢复解析、映射和测试；使用 terminate_after 时按需启用 TerminatedEarly。
// Hits、Aggregations 和 Shards.Failures 保留原始 JSON，业务解析时应使用明确的字段类型
// 或 json.Decoder.UseNumber 保持整数精度。
//
// 下例还需导入 context、errors、time、github.com/bpcoder16/pixiu/infra/elasticSearchx/v8：
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
//	    elasticSearchx.OptLogRequests(true), // 可选：显式开启请求结果日志
//	    elasticSearchx.OptLogDetails(true), // 可选：采集请求体和必要响应信息
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Close(context.Background()) // 应用停止请求后、日志关闭前调用
//	result, err := client.Search(ctx, "products", queryDSL)
//	if err != nil {
//	    return err
//	}
//	// 本例拒绝超时或分片失败的结果，业务也可自行处理部分结果。
//	if result.TimedOut || result.Shards.Failed > 0 {
//	    return errors.New("搜索结果不完整")
//	}
//	if result.Total != nil {
//	    _ = result.Total.Value
//	    _ = result.Total.Relation
//	}
//	_ = result.Hits
//	_ = result.Aggregations
//	return nil
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
// OptLogRequests(true) 开启请求结果日志，OptLogRequests(false) 关闭结果及详情日志。
// 单独设置 OptLogDetails(true) 不会开启请求结果日志。
// 详情采集只由这两个开关决定，日志级别在最终输出时判断。
// 详情仅增加 request_body、response_proto、response_body、response_status_text，
// 不记录 Header；Body 不主动脱敏或截断。请求详情复用已编码的请求体，
// 响应详情随操作解析采集；解析失败时可能只包含已读取部分。
// Client 仅公开封装好的基础操作，不直接接入官方 esapi；
// 各操作在完成解析和 Body 关闭后，按最终结果统一输出一次日志。
// 成功及文档不存在为 Info，慢操作和 HTTP 4xx 为 Warn；
// 网络、HTTP 5xx、解析和收尾错误为 Error。error_type 保留服务端错误类型，
// 其余情况使用 document_not_found、bulk_error、transport_error 或 response_error。
// ctx 已调用 logit.WithStart 时，每次实际业务请求还记录 elasticSearch_<Name>_<序号>
// 下游耗时，独立于日志开关和级别过滤；业务可调用 logit.InfoDuration 汇总，
// 例如 Name 为 catalog 时输出 elasticSearch_catalog_1_duration_ms。
// 耗时包含请求执行、响应解析和 Body 关闭，不包含请求编码及日志写入。
// 本地参数错误、空 Bulk 和关闭后拒绝的操作不计时；耗时前缀在客户端创建时计算。
package elasticSearchx
