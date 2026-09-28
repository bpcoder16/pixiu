// Package httpcall 提供可复用的 HTTP 下游客户端，统一记录调用日志与耗时。
// 它位于基础功能层，依赖 logit 和 Resty v2；设计与边界见 docs/httpcall-design.md。
//
// 应用启动时通过 option 完成 Resty 配置，后续请求复用该实例和连接池；
// 以下初始化示例需导入 net/http、time、github.com/go-resty/resty/v2 和
// github.com/bpcoder16/pixiu/infra/httpcall：
//
//	client := httpcall.New("inventory",
//	    httpcall.OptLogDetails(true), // 按需记录请求和响应的详细信息
//	    httpcall.OptResty(func(r *resty.Client) {
//	        r.SetTimeout(90 * time.Second)
//	        r.SetBaseURL("https://inventory.example.com")
//	        r.SetRetryCount(2) // 最多重试 2 次，加上首次请求最多尝试 3 次
//	        r.SetRetryWaitTime(200 * time.Millisecond)
//	        r.SetRetryMaxWaitTime(time.Second)
//	        r.AddRetryCondition(func(resp *resty.Response, err error) bool {
//	            return resp != nil && resp.Request.Method == resty.MethodGet &&
//	                (err != nil || resp.StatusCode() == http.StatusTooManyRequests)
//	        })
//	    }),
//	)
//
// 省略 SetTimeout 时，每次 HTTP 尝试最长 60 秒。
// 示例只对 GET 的执行错误或 HTTP 429 重试，下面创建资源的 POST 不会自动重试；
// 需要重试 POST 时，应先确认下游具备幂等保障。
//
// 在请求处理函数中按接口设置截止时间。下面的函数需要导入 context、fmt、time 和
// github.com/bpcoder16/pixiu/infra/httpcall：
//
//	func createItem(ctx context.Context, client *httpcall.Client, sku string) error {
//	    reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
//	    defer cancel()
//	    resp, err := client.Request(reqCtx).
//	        SetHeader("Content-Type", "application/json").
//	        SetBody(map[string]string{"sku": sku}).
//	        Post("/items")
//	    if err != nil {
//	        return err
//	    }
//	    if resp.IsError() {
//	        return fmt.Errorf("inventory: HTTP %d", resp.StatusCode())
//	    }
//	    return nil
//	}
//
// Request(ctx) 返回 Resty 请求，也可直接调用 Get、Put、Patch、Delete 等方法。
// HTTP 4xx/5xx 遵循 Resty 语义，不自动返回 Go error，业务应检查响应状态。
// 不配置 SetRetryCount 时默认不自动重试。OptResty 中可用 SetRetryCount、SetRetryWaitTime、
// SetRetryMaxWaitTime 和 AddRetryCondition 配置重试次数、退避和条件。
// OptResty 中的 SetTimeout(0) 可关闭客户端级上限，此时应通过请求 context 设置截止时间。
// 超时参数遵循 Resty 自身语义。OptResty 可在 New 时配置认证、重试和传输器等底层参数；
// nil 回调会 panic。自定义 Resty 事件回调也应在 OptResty 中注册；New 后通过
// Resty() 追加回调，若回调 panic，可能使同一次调用产生两条结果日志。
// 公共认证和请求头可在 OptResty 中通过 SetAuthToken、SetHeader 设置；
// 仅针对单次调用的值应设置在 Request(ctx) 返回的请求上。
// 请求 context 可设置更短的截止时间，但不能放宽单次尝试的上限。
// 开启 Resty 重试后,整次调用可能超过单次尝试上限;需要总截止时间时为请求 context 设置超时。
// 每次调用通过 logit 记录统一的 downstream_type、downstream_duration_ms、
// downstream_id 和 downstream_details；details 固定包含 method、attempt、url、status、err，
// 缺值时字符串为 ""、数字为 0；URL 使用完整地址，attempt 为实际尝试次数。
// ctx 已调用 logit.WithStart 时，每次 Execute 还会自动编号记录 httpcall 下游耗时，
// 供业务调用 logit.InfoDuration 汇总；未调用 WithStart 时跳过。
// OptLogDetails 默认关闭；开启后还会记录双方 Header、可读取的 Body、最终 URL、
// 响应状态文本、HTTP 协议和 Content-Length。不会读取业务接管的响应流；缺值用
// 空对象、空字符串或 0，未知的 Content-Length 保留 -1。详细内容不脱敏或截断，
// 可能包含凭据和大量数据，应保护日志存储与收集链路。
// 已有的 logit context 字段会随日志输出。
// 需要按请求分流结果日志时，可先用 logit.WithLoggerName(ctx, name) 设置请求 context；
// 非空名字使用已注册的命名 Logger，未注册或没有名字时使用默认 Logger。
// Resty 自身的 Warnf/Errorf 另写入 stderr，带 httpcall、name 和级别，供进程日志收集器捕获；
// 重试期间可能产生多条诊断记录。Debugf 不输出；诊断信息不作脱敏。
package httpcall
