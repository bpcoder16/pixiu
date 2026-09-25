// Package logit 是 pixiu 的日志模块:门面与核心同包,零第三方依赖,性能优先。
// 设计文档见 docs/log-module-design.md。
//
// 日常使用(不需要轮转时只需 import logit 一个包):
//
//	ctx = logit.WithContext(ctx)                    // 入口初始化日志字段
//	logit.AddMeta(ctx, logit.Str("logId", logit.NewLogID())) // 按需添加链路 ID
//	logit.AddField(ctx, logit.Str("uid", "42"))    // 请求级字段
//	logit.Info(ctx, "user login", logit.Int("uid", 42))
//	svc := logit.With(logit.Str("mod", "Order")) // 模块级子 Logger
//	svc.Error(ctx, "create failed", logit.Err(err))
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
// 全局默认 Logger 输出到 stdout,启动期用 SetDefault 替换;
// 测试捕获输出用 Swap(替换并返回旧值);panic 处理见 ReportPanic/RecoverAndReport。
package logit
