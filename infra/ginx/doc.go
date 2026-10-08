// Package ginx 提供独立 Gin Engine、请求日志和 Recovery。
//
// 以下示例需导入 github.com/gin-gonic/gin 和 github.com/bpcoder16/pixiu/infra/ginx：
//
//	router, err := ginx.New(ginx.Config{
//		TrustedProxies:  []string{"127.0.0.1"},
//		LogRequestInfo:  true,
//		LogResponseInfo: true,
//	})
//	if err != nil {
//		return err
//	}
//	router.POST("/items", func(c *gin.Context) {
//		// 使用 c.Request.Context() 传递日志与取消;绑定错误由业务映射响应。
//		c.Status(204)
//	})
//	// router 实现 http.Handler,交给应用选用的 HTTP 服务端。
//
// New 不修改 Gin 全局模式或 Validator,不自动注册管理端点。
// 通过 TraceHeader(PIXIU-Log-Id)接收并回写日志 ID;非空值原样使用,缺失或为空时生成。
// 访问日志统一使用 logit.InfoDuration,不随状态码改变 Info 级别。
// LogRequestInfo、LogResponseInfo 默认关闭,分别追加 request_info、response_info 详情;
// DisableAccessLog 为 true 时两者均不生效,Observe 回调和独立 panic 错误日志不受影响。
// 进入观察中间件时请求 Logger 未启用 Info,本次访问日志及详情采集均关闭,中途开启不补记。
// 入口允许记录时,输出仍遵循请求结束时 Logger 的 Info 级别过滤。
// 请求详情包含 method、Header、body;响应详情包含 Header、body。
// 正文不截断、不脱敏;UTF-8 文本直接记录,二进制以 Base64 记录并标明 body_encoding。
// 请求正文在业务中间件之前完整预读并还原,读取失败会记录 body_error 并向业务保留原错误。
// 响应正文随写出复制,保留 Flush;Header 记录应用提交的快照,不承诺包含传输层自动添加的值。
// 开启详情会保存完整正文;请求体大小与读取期限由应用约束。
package ginx
