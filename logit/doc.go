// Package logit 是 pixiu 的日志模块:门面与核心同包,零第三方依赖,性能优先。
//
// 日常使用(不需要轮转时只需 import logit 一个包):
//
//	logger := logit.MustNew(logit.OptWriter(logit.Stdout()))
//	logit.SetDefault(logger) // 显式启用默认日志
//	defer logit.Close(logger)
//	ctx := context.Background()
//	ctx = logit.WithContextLogID(ctx)               // 入口初始化字段并生成链路 ID
//	ctx = logit.WithStart(ctx)                      // 记录请求起点
//	msgCtx := logit.NewContextScope(ctx)             // 并发消息各用独立字段存储
//	msgCtx = logit.NewDurationScope(msgCtx)          // 独立计时,不继承父请求耗时表
//	logit.AddField(ctx, logit.Str("uid", "42"))    // 请求级字段
//	logit.Info(ctx, "user login", logit.Int("uid", 42))
//	logit.AddDownstreamDurationAuto(ctx, "mysql", 12*time.Millisecond) // 自动生成 mysql_1
//	logit.InfoDuration(ctx, "request done")       // 自动追加 mysql_1、self 和 total 耗时
//	svc := logit.With(logit.Str("mod", "Order")) // 模块级子 Logger
//	svc.Error(ctx, "create failed", logit.Err(err))
//
// WithContextLogID 每次调用都会追加新的 ID，只在入口调用一次；后续直接传递 ctx。
// 无需 ID 时仍使用 WithContext；已有 ID 时初始化后调用
// AddMeta(ctx, Str(LogId, id))。LogIDFromContext 读取 meta 中最后一个非空字符串 LogId，
// 不从普通字段或调用点字段读取，不改变日志重复字段的输出规则。
//
// 基础功能模块可用 AddDownstreamDurationAuto 自动编号并记录下游耗时；ctx 没有
// WithStart 时跳过，不要求未启用耗时统计的调用方额外初始化 context。
//
// 按 context 选择命名 Logger(未注册时回退默认 Logger)：
//
//	logit.SetNamed("http", logit.MustNew(logit.OptWriter(logit.Stderr())))
//	httpCtx := logit.WithLoggerName(ctx, "http")
//	if logit.InfoEnabled(httpCtx) {
//	    logit.Output(httpCtx, logit.InfoLevel, 0, "request done")
//	}
//
// DebugEnabled/InfoEnabled/WarnEnabled/ErrorEnabled 按 ctx 选择 Logger；
// 任意级别可通过 LoggerFromContext(ctx).Enabled(level) 判断。
// 无 ctx 的 logit.With 始终基于默认 Logger。上例 Output 的 callDepth=0 指向其直接调用者。
//
// 统一记录下游调用时，可用 DownstreamFields 生成四个标准字段：
//
//	fields := logit.DownstreamFields("httpcall", "wechat", 12*time.Millisecond,
//	    map[string]any{"method": "POST", "status": 200})
//	logit.Info(ctx, "downstream call", fields...)
//
// 四个标准字段名也可通过普通字段构造器直接写入。
//
// 构造专属实例(依赖注入 / 测试):
//
//	buf := &bytes.Buffer{}
//	logger := logit.MustNew(logit.OptWriter(logit.NewWriter(buf)))
//
// 生产落盘链路:
//
//	file, _ := rotatefile.New("/var/log/app/app.log") // 独立轮转包；要求绝对路径
//	rotated := logit.NewWriter(file)
//	logger := logit.MustNew(logit.OptDispatch(
//	    logit.Target{Levels: []logit.Level{logit.DebugLevel, logit.InfoLevel}, Writer: rotated},
//	))
//	defer logit.Close(logger) // 应用退出最后一步
//	_ = logit.SetMinLevel(logger, logit.InfoLevel) // 可在运行期原子调整
//
// 应用退出时，先停止接收新请求、等待后台任务结束并关闭其他组件，再关闭 Logger。
// 其他组件在收尾期间可能仍需记录日志，因此 logit.Close 应是资源关闭的最后一步。
//
// 多资源关闭时，可用 lifecycle.Stack 替代上例的 defer，按登记的逆序关闭：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error { return logit.Close(logger) }); err != nil {
//	    return err
//	}
//	if err := stack.Register(client.Close); err != nil { // client 由应用创建
//	    return err
//	}
//	// 停止业务入口并等待使用 client 的任务结束后：
//	return stack.Close() // 先关闭 client，最后关闭 logger
//
// 全局默认 Logger 初始为空操作 Logger；所有级别（含 Fatal）直接返回，不输出、不退出，
// 不校验输出字段或执行 Defer 回调；Enabled 始终为 false，With 仍为空操作，Close 返回 nil。
// 启动期用 SetDefault 显式启用；独立构造和显式注册的命名 Logger 不受默认配置影响。
// 初始化前取得的 Logger 或 With 子 Logger 保持空操作，需要输出时须重新获取。
// 使用 bootstrap 的新项目须先完成 MustBaseInit，再通过 Default、With 或 Named 创建和保存模块 Logger；
// 不要在先于 bootstrap 执行的包变量初始化或 Go 的 init() 中缓存这些 Logger。
// 后续 SetDefault 或命名注册不会更新已保存的空操作实例；包级 Info(ctx, ...) 等方法每次选择当前 Logger。
// WithContext 前置约束及 panic 专用接口语义不受默认 Logger 是否配置影响。
// 测试可先用 Default 保存旧值,再用 SetDefault 替换并在结束时恢复。
// panic 处理见 ReportPanic/RecoverAndReport；processStart 保存包初始化时的时间点，
// 输出时按当前 time.Local 格式化。应用必须在并发使用日志前完成默认时区设置。
package logit
