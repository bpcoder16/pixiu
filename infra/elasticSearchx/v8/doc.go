// Package v8 创建 Elasticsearch 8 官方客户端并接入 elasticSearchx 的统一日志与操作。
//
// 以下示例需导入 github.com/bpcoder16/pixiu/infra/elasticSearchx：
//
//	client, err := v8.New(elasticSearchx.Config{
//	    Name:      "search", // 必填：日志与耗时前缀中的实例名称
//	    Addresses: []string{"https://localhost:9200"}, // 必填：节点地址
//	    APIKey:    apiKey, // 可选：按服务端要求配置认证
//	    CACert:    caPEM,  // 可选：额外信任的 CA 证书
//	},
//	    elasticSearchx.OptLogRequests(true), // 可选：显式开启请求结果日志
//	    elasticSearchx.OptLogDetails(true), // 可选：默认关闭详情
//	)
//	if err != nil {
//	    return err
//	}
//	defer client.Close()
//	_, err = client.Count(ctx, "products", queryDSL)
//	return err
//
// 创建不接收 ctx，StartupTimeout 控制验活总期限，零值默认 5 秒；业务操作仍传入 ctx。
// 请求结果日志默认关闭，OptLogRequests(true) 可开启；Name 用于区分下游实例。
// 详情字段、Body 读取与请求级耗时统计约定见 elasticSearchx 的包文档。
//
// 共享实例使用 v8.NewNamed(cfg, opts...) 或 v8.NewDefault(cfg, opts...)。
// 通过 elasticSearchx.Named(cfg.Name) 或 elasticSearchx.Default() 获取实例，
// 并将 elasticSearchx.CloseAll 登记到 lifecycle.Stack；完整样例见公共包文档。
// 三个版本共用名称空间及默认引用；启动阶段串行初始化，停止业务后统一关闭。
package v8
