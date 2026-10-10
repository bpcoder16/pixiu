// Package bootstrap 提供日志和 MySQL 多实例的通用初始化入口，供项目自己的 bootstrap
// 声明资源后调用，再执行业务服务装配。设计见 docs/bootstrap-design.md。
//
// 应用负责加载配置、创建资源关闭栈，并在初始化前登记关闭流程：
//
//	config := httpconfig.MustLoadAppConfig("./conf/app.yaml")
//	var resources lifecycle.Stack
//	defer func() {
//		if err := resources.Close(); err != nil {
//			log.Printf("close resources: %v", err)
//		}
//	}()
//	bootstrap.MustRegisterMySQL("orders", true)
//	bootstrap.MustRegisterMySQL("reports", false)
//	bootstrap.MustBaseInit(&config.AppConfig, &resources)
//	logit.Info(context.Background(), "application initialized")
//
// 示例使用标准库 context、log，以及 pixiu 的 biz/httpconfig、biz/bootstrap、
// lifecycle、logit。初始化 panic 原样传播，已登记资源由应用的 defer 关闭。
// 通用入口只接收 baseconfig.AppConfig；HTTP 配置将其嵌入的基础配置传入。
//
// 初始化最先将 time.Local 设置为 env.TimeLocation()，随后创建日志及轮转资源。
// 配置加载只发布环境，设置默认时区由本包负责；必须在并发使用时间或日志前执行。
// 此设置只在启动阶段应用一次，运行期不切换，关闭或后续初始化失败时不恢复。
// time.Now 和日志轮转使用配置时区；无时区字符串仍需使用 time.ParseInLocation。
// MySQL 的 location、sessionTimeZone 沿用自身配置与默认值，不自动继承应用时区。
//
// MySQL 声明只包含实例名和默认标记；名称为非空 ASCII 字母、数字、下划线、连字符组合。
// 重复名称、第二个默认声明或初始化开始后追加声明均 panic；允许没有默认实例。
// 声明时不读文件，初始化先创建日志，再读取 env.ConfigDirPath() 下的 mysql.<name>.yaml。
// 全部文件严格解析完成后才建立连接；缺失文件、未知字段、非法配置或连接失败均 panic。
// 文件通过 configx.Parse 解析，不注册全局配置或占用配置名称。
// 模板见 conf.example/mysql.example.yaml，文件内不接受 name 和 isDefault。
// database、username、password、charset、location、tlsConfig、驱动超时及 pool
// 在文件顶层统一配置，所有主从共用；master 和 slaves 每项只接受 host、port。
// 每个端点仍创建独立连接池；旧配置中的端点公共字段必须迁移到顶层。
// 文件省略 logSQL、interpolateSQL 时默认 true，显式 false 可关闭。
// 未声明的实例不加载；没有声明时不初始化或关闭 MySQL 模块。
// mysqlx.Default() 与 mysqlx.Named(默认名称) 共享同一个实例，其他实例按名称获取。
// 各实例的 InitTimeout 独立生效，构造使用 mysqlx 内部超时，业务查询使用自己的 context。
// 首个实例创建前只登记一次 mysqlx.CloseAll，不再逐个登记 Client.Close；
// 部分创建失败的连接由 mysqlx 清理，此前已完成的实例由应用栈统一关闭。
//
// 日志固定使用 rotatefile，format 必须显式设置为 text/json；默认启动目录下的 log 目录、每小时轮转、
// 每个分流文件保留 48 个实际文件。config.Log 可配置 format、caller、dir 目录、names、rotate。
// dir 相对路径基于 env.RootDirPath()，绝对路径直接使用；目录自动创建并检查可写权限。
// 默认 Info 写 env.AppName()+".info.log"，Debug 写 env.AppName()+".debug.log"，
// Warn、Error、Fatal 写 env.AppName()+".wf.log"，每条日志仅写入其对应文件。
// names 中的每个名字创建独立 Logger，文件基名为 env.AppName()+"."+name，
// 同样分流为 .info.log、.debug.log、.wf.log；不允许空名、路径分隔符或完全重复的文件名。
// 名字按原始字符串使用和判重，不做大小写折叠或规范化；
// 文件系统大小写规则引起的文件冲突由调用方自行负责。
// 例如 names: [worker] 时，可以通过 logit.Named("worker").Info(ctx, "worker ready")，
// 或 logit.Info(logit.WithLoggerName(ctx, "worker"), "worker ready") 写入命名日志。
// caller 由 config.Log.Caller 控制，默认关闭，对默认及命名 Logger 同时生效。
// release 输出 Info 及以上，debug/test 输出 Debug 及以上。
// 配置加载不初始化日志；每个 Logger 创建成功后向应用栈登记 logit.Close(logger)，
// 全部创建并登记成功后，再发布默认及命名 Logger。
// 构造失败仅清理当前 Logger 已打开的 Writer；此前已登记的 Logger 由 main 的 defer 收尾。
// 调用方须传入空资源栈，让后续资源关闭期间仍可写日志。
// 不重复发布环境；日志时间与轮转均使用启动时设置的应用时区。
// 后续独立资源创建成功后立即登记关闭函数；登记失败须关闭尚未交付的资源。
// 共享资源只登记一次，日志先登记、最后关闭。本包不持有全局关闭栈。
// 接入后台任务后，应用必须先停止入口并等待任务退出，再关闭下游资源；
// 取消 context 不能代替等待。注册及 MustBaseInit 仅供启动期串行调用；
// 参数校验通过即封闭注册，重复初始化或失败后重试会 panic。
package bootstrap
