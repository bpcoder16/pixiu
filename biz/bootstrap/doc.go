// Package bootstrap 提供日志、MySQL、Redis、Elasticsearch 和 NATS 多实例的通用初始化入口，
// 以及应用运行管理器、Gin Engine 和 HTTP 服务的构造方法，供项目自己的 bootstrap 组装。设计见 docs/bootstrap-design.md。
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
//	bootstrap.MustRegisterRedis("orders", true)
//	bootstrap.MustRegisterElasticsearch("orders", true)
//	bootstrap.MustRegisterNATS("orders", true)
//	bootstrap.MustBaseInit(&config.AppConfig, &resources)
//	logit.Info(context.Background(), "application initialized")
//
// 示例使用标准库 context、log，以及 pixiu 的 biz/httpconfig、biz/bootstrap、
// lifecycle、logit。初始化 panic 原样传播，已登记资源由应用的 defer 关闭。
// 通用入口只接收 baseconfig.AppConfig；HTTP 配置将其嵌入的基础配置传入。
//
// 初始化成功后，应用持有管理器，显式登记服务并接管停机信号：
//
//	func start(handler http.Handler) error {
//		manager, err := bootstrap.NewAppManager()
//		if err != nil {
//			return err
//		}
//		service, err := bootstrap.NewHTTPService(handler)
//		if err != nil {
//			return err
//		}
//		if err := manager.Register(service); err != nil {
//			return errors.Join(err, service.Shutdown())
//		}
//		return manager.RunSignals()
//	}
//
// 此示例另用标准库 errors、net/http；handler 由应用提供，http.yaml 从 env.ConfigDirPath() 读取。
// 使用 Gin 的应用可在启动阶段先构造 Engine（另需导入 github.com/gin-gonic/gin 和 pixiu 的 infra/env，
// debug/test 模式还须 blank import 应用生成的 Swag 文档包）：
//
//	gin.SetMode(env.RunMode())
//	engine, err := bootstrap.NewGinEngine()
//	if err != nil {
//		return err
//	}
//	engine.GET("/healthz", func(c *gin.Context) {
//		c.Status(http.StatusNoContent)
//	})
//	return start(engine)
//
// NewGinEngine 从 env.ConfigDirPath() 下的 gin.yaml 读取一次配置，再调用 ginx.New。
// 文件必需，{} 沿用默认值：不信任代理、记录访问日志、不记录请求或响应详情。
// 支持 trustedProxies、loggerName、disableAccessLog、logRequestInfo、logResponseInfo；
// 不接受运行时回调，不注册全局配置或重读文件；缺失、未知字段、错误类型和非法代理均返回错误。
// 模板见 conf.example/gin.yaml；这些配置不属于 app.yaml 或 httpconfig.AppConfig。
// Engine 创建后仅按 env.RunMode() 决定文档路由：debug/test 调用 ginx.RegisterOpenAPI，release 跳过。
// 自动提供 /openapi/doc.json、/elements 和内嵌静态资源，不读取独立文档配置，不自动增加认证。
// 应用负责生成文档并 blank import 注册默认 Swag 文档实例，元信息须在构造前设置；
// 生成目录由应用决定，例如 swag init -o ./openapi。debug/test 未注册文档时返回 nil Engine 和错误，
// 提示生成、导入文档包并保留底层错误链；release 不要求生成文档。
// 文档路由在构造时已注册，之后添加的业务中间件不作用于这些路由。
// 需要自定义文档注册策略的应用可直接使用 ginx.New 和 ginx.RegisterOpenAPI。
// 不修改 Gin 全局模式或 Validator，不监听端口或自动注册业务路由。应用构造一次后持有 Engine，
// 完成业务路由和中间件注册，再传给 NewHTTPService；模式由应用启动时显式设置。
// disableAccessLog 覆盖详情开关；详情完整记录且不脱敏，开启前须在 Engine 外层约束请求体规模及读取期限。
// logResponseInfo 会记录完整文档 UI 资源，使用文档时建议关闭。
//
// NewAppManager() 或 NewAppManager(0) 使用 apprun 默认的 20 秒总关闭期限，
// 也可传入一个不小于 5 秒的 time.Duration，负值或不足 5 秒的正值返回错误；
// 不保存全局管理器，不自动注册服务或接管信号。
// NewHTTPService 从 env.ConfigDirPath() 下读取 http.yaml，严格解析 httpserver.Config 并返回名为 http 的 apprun.Service，
// 构造时不监听，HTTP ShutdownTimeout 省略或为零时默认 15 秒。
// Run 将启动与关闭竞争产生的 httpserver.ErrClosed 视为正常退出，保留其他运行与关闭错误。
// 注册失败由调用方关闭该服务；注册成功后由管理器关闭，不重复登记到资源栈。
// HTTP 超时后强制关闭普通连接，不额外等待 handler；管理器返回后应用尽力清理共享资源及日志。
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
// Redis 使用 MustRegisterRedis(name, isDefault) 声明，与 MySQL 独立判重，允许同名。
// MySQL 初始化后，读取配置目录下的 redis.<name>.yaml，先严格解析全部文件并完成文件层校验，
// 再按声明顺序由 redisx 校验客户端配置、创建并 Ping 验活；未声明时不初始化或关闭 Redis 模块。
// 模板见 conf.example/redis.example.yaml；首版使用单节点 TCP，文件不接受 name、isDefault。
// host、port、username、password、db、驱动超时、maxRetries、pool 和日志开关由文件提供。
// host 必填，port 零值默认 6379，db 非负；pool.size 对应 PoolSize，
// pool.maxActiveConns 限制该池连接数。其余连接参数零值沿用 go-redis 语义。
// 时间使用带单位字符串，负值无效；maxRetries 的 -1 禁用命令重试，0 使用驱动默认重试。
// 初始化没有 initTimeout 总预算，沿用 redisx 的驱动超时与重试；业务命令传入自己的 context。
// logCommands 默认 false，只关闭正常命令的 Info 日志，慢调用和错误日志仍包含请求参数。
// redisx.Default().Client() 与 redisx.Named(name).Client() 获取同一默认客户端。
// 创建首个实例前只登记一次 redisx.CloseAll，单个失败实例由 redisx 立即清理；
// 此前成功的 Redis、MySQL 与日志交给应用资源栈关闭，不重复登记单个客户端 Close。
// 关闭前先停止并等待外部生产者及业务管理的订阅；NATS 托管消费由其 CloseAll 排空，
// 再关闭 Elasticsearch、Redis、MySQL 与日志。消费者仍使用的项目资源也必须晚于 NATS 关闭。
//
// Elasticsearch 使用 MustRegisterElasticsearch(name, isDefault) 声明，与 MySQL、Redis 独立判重。
// Redis 初始化后读取 elasticsearch.<name>.yaml，文件前缀统一小写；未声明时跳过。
// 模板见 conf.example/elasticsearch.example.yaml，version 必填且支持 7/8/9，文件不接受 name、isDefault。
// 全部 ES 文件严格解析并准备后，再登记一次 elasticsearchx.CloseAll，按版本创建默认或命名客户端。
// 三个版本共用默认与命名注册表；elasticsearchx.Default() 与 Named(默认名称) 返回同一实例。
// caCertFile 支持绝对路径或相对配置目录的 PEM 文件；缺失或为空时失败，证书校验由底层完成。
// startupTimeout 零值默认 5s，只限制启动验活与版本检查；业务操作使用各自的非 nil context。
// 连接、认证和 pool 参数的默认值及语义校验沿用 elasticsearchx。
// logRequests 省略默认 true，false 关闭所有请求结果日志，包含慢调用和错误，但仍记录下游耗时。
// logDetails 默认 false，两项同时开启才采集正文；正文不脱敏、不截断，不额外创建 ES Logger。
// 失败实例由底层清理，先前成功实例交给应用栈；只登记模块 CloseAll，不重复登记 Client.Close。
// 关闭前先停止并等待业务任务，沿用底层 SDK 关闭行为，不新增关闭期限。
//
// NATS 使用 MustRegisterNATS(name, isDefault) 声明，Elasticsearch 之后读取 nats.<name>.yaml。
// 全部 NATS 文件严格解析并转换后，登记一次 natsx.CloseAll，再创建默认或命名客户端；
// 未声明时跳过。模板见 conf.example/nats.example.yaml；文件不接受 name、isDefault。
// 转换阶段检查 URL 解析及内嵌凭据，通用参数由 natsx 在构造时校验；
// 后续实例校验失败时，此前成功的实例由应用关闭栈回收。
// urls 必填，地址非空且无首尾空白，不接受 URL 内嵌凭据；认证使用 username/password。
// 超时零值使用 natsx 默认值，负值无效；connectTimeout 默认 5s，是单次拨号期限。
// drainTimeout 默认 10s，仅用于底层连接 Drain；closeTimeout 默认 20s，限制完整关闭流程。
// jetStream.publishAsyncTimeout 默认 5s，只限制默认上下文的异步 PubAck 等待。
// 连接成功不代表 JetStream 已启用或业务权限已验证；业务自行准备 Stream/Consumer，
// 在 handler 依赖就绪后注册订阅。消息格式、ACK、重试和幂等策略由业务负责。
// NATS 关闭前先停止并等待外部生产者，托管订阅及 worker 由 Client.Close 排空，期间允许
// 回调继续发布和请求。消费者及其发布服务必须在启动时持有客户端引用，不能在排空期间
// 再次查询 Default/Named；回调依赖的资源晚于 NATS 关闭，日志最后关闭。
// CloseTimeout 超时强制关闭连接并取消消息 ctx，不能强制终止不响应取消的业务函数。
//
// 日志固定使用 rotatefile，format 必须显式设置为 text/json；默认启动目录下的 log 目录、每小时轮转、
// 每个分流文件保留 48 个实际文件。config.Log 可配置 format、caller、dir 目录、names、rotate。
// dir 相对路径基于 env.RootDirPath()，绝对路径直接使用；目录自动创建并检查可写权限。
// 业务日志写入 dir/<appName>/biz；应用名作为独立目录名，禁止为 . 或 ..。
// 默认 Info 写 info.log，Debug 写 debug.log，Warn、Error、Fatal 写 wf.log，每条日志仅写入其对应文件。
// names 中的每个名字创建独立 Logger，文件为 <name>.info.log、<name>.debug.log、<name>.wf.log；
// 所有文件名均不带 appName 前缀；不允许空名、路径分隔符或完全重复的文件名。
// 名字按原始字符串使用和判重，不做大小写折叠或规范化；
// 文件系统大小写规则引起的文件冲突由调用方自行负责。
// 例如 names: [worker] 时，可以通过 logit.Named("worker").Info(ctx, "worker ready")，
// 或 logit.Info(logit.WithLoggerName(ctx, "worker"), "worker ready") 写入命名日志。
// caller 由 config.Log.Caller 控制，默认关闭，对默认及命名 Logger 同时生效。
// hookStd 省略时默认开启，显式 false 关闭；Linux/macOS 上将 stdout/stderr
// 分别原样追加到 dir/<appName>/std/stdout.log 和 dir/<appName>/std/stderr.log，
// 自动创建 std 目录，文件不编码、不轮转。
// 重定向在发布 Logger 前完成，失败终止初始化；fd 1/2 保留至进程退出，
// 不随资源栈恢复，确保应用 defer 之后的 runtime panic 仍被收集。
// release 输出 Info 及以上，debug/test 输出 Debug 及以上。
// 配置加载不初始化日志；每个 Logger 创建成功后向应用栈登记 logit.Close(logger)，
// 全部创建并登记成功后，再发布默认及命名 Logger。
// 构造失败仅清理当前 Logger 已打开的 Writer；此前已登记的 Logger 由 main 的 defer 收尾。
// 调用方须传入空资源栈，让后续资源关闭期间仍可写日志。
// 不重复发布环境；日志时间与轮转均使用启动时设置的应用时区。
// 后续独立资源创建成功后立即安排关闭；登记失败须关闭尚未交付的资源，
// 被 NATS 消费回调使用的资源必须安排在 NATS 排空完成后关闭。
// 共享资源只登记一次，日志先登记、最后关闭。本包不持有全局关闭栈。
// 接入后台任务后，应用必须先停止入口并等待任务退出，再关闭下游资源；
// 取消 context 不能代替等待。注册及 MustBaseInit 仅供启动期串行调用；
// 参数校验通过即封闭注册，重复初始化或失败后重试会 panic。
package bootstrap
