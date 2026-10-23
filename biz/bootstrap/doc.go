// Package bootstrap 提供日志、MySQL、Redis、Elasticsearch、OSS、阿里云 ES 和 NATS 多实例的通用初始化入口，
// 以及应用运行管理器、Gin Engine、HTTP 和 Cron 服务的构造方法，供项目自己的 bootstrap 组装。
//
// 应用负责加载配置、创建资源关闭栈，并在初始化前登记关闭流程。
// 以下片段放在具名返回 runErr error 的 run 函数内；main 在返回错误后退出：
//
//	config := appconfig.MustLoadAppConfig("./conf/app.yaml")
//	var resources lifecycle.Stack
//	defer func() {
//		runErr = errors.Join(runErr, resources.Close())
//	}()
//	bootstrap.MustRegisterMySQL("orders", true)
//	bootstrap.MustRegisterMySQL("reports", false)
//	bootstrap.MustRegisterRedis("orders", true)
//	bootstrap.MustRegisterElasticsearch("orders", true)
//	bootstrap.MustRegisterAliyunOSS("assets", true)
//	bootstrap.MustRegisterAliyunES("search-control", true)
//	bootstrap.MustRegisterNATS("orders", true)
//	bootstrap.MustBaseInit(config, &resources)
//	moduleLogger := logit.With(logit.Str("mod", "orders"))
//	moduleLogger.Info(context.Background(), "application initialized")
//
// 示例使用标准库 context、errors，以及 pixiu 的 biz/appconfig、biz/bootstrap、
// lifecycle、logit。初始化 panic 原样传播，已创建资源由应用的 defer 关闭。
// 通用入口接收 *appconfig.AppConfig；HTTP 与 Cron 共用 env/log 配置及初始化流程。
// 先应用 env 时区，再依次初始化日志、OSS、阿里云 ES、MySQL、Redis、Elasticsearch 和 NATS；
// 共享客户端仅初始化已声明的实例。HTTP、Cron 和 NATS 消费者完成排空或超时取消后，
// 资源栈依次关闭 NATS、Elasticsearch、Redis、MySQL、阿里云 ES、OSS、日志。
// 该顺序按组件类别描述，不保证同一组件内多个命名实例的关闭顺序。
//
// 新项目应先完成 MustBaseInit，再通过 logit.Default、logit.With 或 logit.Named 创建和保存模块 Logger，
// 如上例所示；不要在先于 bootstrap 执行的包变量初始化或 Go 的 init() 中缓存这些 Logger。
// 初始化前取得的空操作实例不会随后续 SetDefault 或命名注册自动更新。
// 包级 logit.Info(ctx, ...) 等方法每次调用时选择当前 Logger，可在初始化完成后直接使用。
//
// 迁移期间由应用自管日志时，将上例的 MustBaseInit 调用替换为：
//
//	bootstrap.MustBaseInitWithoutLogit(config, &resources)
//
// 此入口跳过日志配置校验、默认及命名 Logger 创建、日志关闭登记和 stdout/stderr 重定向，
// app.yaml 可以省略 log，已有 Logger 保持原样；自管日志由应用负责最后关闭。
// 参数校验、env 时区设置和其他组件初始化保持一致；两个入口共用一次初始化状态，
// 任一入口开始初始化后，不论成功或失败，均不能再调用任一入口或追加声明。
// 两种入口均须在其他 goroutine 并发使用时间或日志前调用。
//
// 初始化成功后，可将服务构造函数交给 Start，统一登记服务并接管停机信号：
//
//	func start(handler http.Handler) error {
//		return bootstrap.Start(func() (apprun.Service, error) {
//			return bootstrap.NewHTTPService(handler)
//		})
//	}
//
// 此示例另用标准库 net/http 和 pixiu 的 infra/apprun；handler 由应用提供，http.yaml 从 env.ConfigDirPath() 读取。
// ServiceFactory 的签名为 func() (apprun.Service, error)，Start 按传入顺序同步构造并登记，全部成功后运行。
// 后续构造或登记失败时逆序回收此前成功构造的服务，包括本轮登记失败的服务，并保留原错误与回收错误。
// 启动前回收时，Stop/Wait/Cancel 的 panic 转为 apprun.PanicError，保留堆栈并继续回收其他服务。
// Wait 返回错误、panic 或共享 context 到期均调用该服务的 Cancel 兜底。
// 构造函数返回错误时负责自行回收本轮部分资源；nil 构造函数返回错误，无服务时返回 apprun.ErrNoServices。
// Start 使用管理器默认的 30 秒运行期总关闭预算；进入运行阶段后由管理器统一停收并行排空服务，不重复回收。
// 构造及启动前回收不计入运行期关闭预算，信号仅从所有服务登记成功后开始接管。
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
// 常用 Gin HTTP 装配可直接使用 NewGinHTTPService，由其设置模式并读取两份配置；
// 业务只提供路由注册回调和文档模块，通过 Start 接管运行（另需导入 infra/ginx）：
//
//	func startGin() error {
//		return bootstrap.Start(func() (apprun.Service, error) {
//			return bootstrap.NewGinHTTPService(func(r *gin.Engine) {
//				r.GET("/healthz", func(c *gin.Context) {
//					c.Status(http.StatusNoContent)
//				})
//			},
//				ginx.OpenAPIModule{Name: "app", Title: "App"},
//				ginx.OpenAPIModule{Name: "admin", Title: "Admin"},
//			)
//		})
//	}
//
// 回调在 Engine 和文档路由创建成功后同步调用一次，nil 表示不注册业务路由。
// NewGinHTTPService 须在启动阶段串行调用，运行期间不得借此切换 Gin 全局模式。
//
// NewGinEngine 从 env.ConfigDirPath() 下的 gin.yaml 读取一次配置，再调用 ginx.New。
// 文件必需，{} 沿用默认值：不信任代理、记录访问日志、不记录请求或响应详情。
// 支持 trustedProxies、loggerName、disableAccessLog、logRequestInfo、logResponseInfo；
// 不接受运行时回调，不注册全局配置或重读文件；缺失、未知字段、错误类型和非法代理均返回错误。
// 模板见 conf.example/gin.yaml；这些配置不属于 app.yaml 或 appconfig.AppConfig。
// Engine 创建后仅按 env.RunMode() 决定文档路由：debug/test 调用 ginx.RegisterOpenAPI，release 跳过。
// 不传参数时提供默认文档 /openapi/doc.json、/docs 和内嵌静态资源。
// 需要多个模块时，由应用显式声明名称和标题（另需导入 infra/ginx）：
//
//	engine, err := bootstrap.NewGinEngine(
//		ginx.OpenAPIModule{Name: "app", Title: "App"},
//		ginx.OpenAPIModule{Name: "admin", Title: "Admin"},
//	)
//
// 此时每个模块提供 /openapi/<Name>.json 与 /docs/<Name>，/docs 展示首个模块。
// 页面共用静态资源并提供顶部模块导航；模块列表由代码提供，不读取独立文档配置，不自动增加认证。
// 应用负责生成文档并 blank import 注册同名 Swag 文档实例，元信息须在构造前设置；
// 多模块使用 swag init 的 --instanceName 与 --tags 等参数分别生成所需文档。
// 生成目录由应用决定，例如 swag init -o ./openapi。debug/test 未注册文档时返回 nil Engine 和错误，
// 提示生成、导入文档包并保留底层错误链；release 不要求生成文档。
// 文档路由在构造时已注册，之后添加的业务中间件不作用于这些路由。
// 需要自定义文档注册策略的应用可直接使用 ginx.New 和 ginx.RegisterOpenAPI。
// 不修改 Gin 全局模式或 Validator，不监听端口或自动注册业务路由。应用构造一次后持有 Engine，
// 完成业务路由和中间件注册，再传给 NewHTTPService；模式由应用启动时显式设置。
// disableAccessLog 覆盖详情开关；详情完整记录且不脱敏，开启前须在 Engine 外层约束请求体规模及读取期限。
// 全部文档 GET 路由自动忽略访问日志、详情及 Observe，保留 Recovery。
// 包括文档页面、首页别名、JSON 端点和三个内嵌静态资源。
//
// NewAppManager() 或 NewAppManager(0) 使用 apprun 默认的 30 秒总关闭期限，
// 也可传入一个不小于 5 秒的 time.Duration，负值或不足 5 秒的正值返回错误；
// 不保存全局管理器，不自动注册服务或接管信号。
// 需要自定义预算或自行控制运行 context 时，直接持有 NewAppManager 返回的管理器，登记后调用 Run 或 RunSignals。
// NewHTTPService 从 env.ConfigDirPath() 下读取 http.yaml，严格解析 httpserver.Config 并返回名为 http 的 apprun.Service，
// 构造时不监听，HTTP 排空期限由管理器提供，不再读取 shutdownTimeout。
// Run 将启动与关闭竞争产生的 httpserver.ErrClosed 视为正常退出，保留其他运行与关闭错误。
// 直接使用管理器时，进入有效 Run 前的构造或登记失败由调用方逆序关闭全部已构造服务（含已登记服务）。
// 运行后由管理器关闭，不重复登记到资源栈。
// 排空超时取消剩余任务、关闭未完成普通连接，然后由入口有界回收资源并退出进程。
//
// Cron 独立加载 cron.yaml，通过任务名绑定业务方法；不需要 HTTP/Gin 配置。
// 包级具名函数可通过 infra/cronx 在业务包中批量声明，无需为每个任务编写 init：
//
//	func Heartbeat(ctx context.Context) error {
//		logit.Info(ctx, "heartbeat")
//		return nil
//	}
//
//	var Registry = cronx.MustNewHandlerRegistry(
//		Heartbeat,
//	)
//
// 构造服务时调用 NewCronService(Registry.Handlers()...)，示例自动名称为 Heartbeat。
// 注册器只登记元数据、不读配置或执行任务，各实例独立，Handlers 返回副本。
// 零值 cronx.HandlerRegistry 也可直接使用；MustRegister(handler, name) 支持显式稳定名称，
// 适用于闭包、方法值、泛型函数和需要保持 YAML/锁标识兼容的重命名。
// 自动名称保留包级函数名的大小写，配置中的 name 必须与之完全一致；nil、无法自动取名、非法名称及重名均 panic。
// 登记与读取快照可并发，首次使用后不得复制注册器；登记不会更新已构造的 Scheduler。
//
// 也可直接传入带有稳定名称的 cronx.NamedHandler；以下示例只在 jobs 中启用 scan-orders：
//
//	service, err := bootstrap.NewCronService(cronx.NamedHandler{
//		Name: "scan-orders",
//		Handler: func(ctx context.Context) error {
//			logit.Info(ctx, "scan orders")
//			return nil
//		},
//	})
//	if err != nil {
//		return err
//	}
//	manager, err := bootstrap.NewAppManager(40 * time.Second)
//	if err != nil {
//		_ = service.Stop()
//		service.Cancel()
//		return err
//	}
//	if err := manager.Register(service); err != nil {
//		_ = service.Stop()
//		service.Cancel()
//		return err
//	}
//	return manager.RunSignals()
//
// 此示例使用标准库 context、time；cron.yaml 中只配置 scan-orders 或补齐其他 Handler，模板见 conf.example/cron.yaml。
// 代码可以注册全部 Handler，jobs 只列出需要启用的任务；引用未注册的名称会报绑定错误。
// 注释或删除某个 job 后重启即可停用，全部注释或 jobs: [] 时允许零任务运行。
// 任务支持六字段 Cron、固定 every、随机 randomEvery，三者互斥；关闭由 Stop、Wait(ctx)、Cancel 组成，不设置组件级排空预算。
// every 与 randomEvery.min 最短均为 100ms，小于下限时构造报错。
// 默认管理器的 30s 可覆盖独立 Cron；上例保留显式 40s 作为自定义预算的用法。
// 使用默认预算时，可通过 ServiceFactory 直接构造 Cron 服务：
//
//	return bootstrap.Start(func() (apprun.Service, error) {
//		return bootstrap.NewCronService(Registry.Handlers()...)
//	})
//
// NewCronService 读取调度配置并绑定 Handler，构造错误原样返回，成功后由管理器接管运行与关闭。
// lock 段存在时通过已初始化的命名 Redis 构造 gocron-redis-lock，配置 TTL 与自动续租；
// 任务须设置正 timeout。仅在租约有效时互斥，不保证每周期一次，失租不自动取消业务。
// 混合运行统一停收所有服务、共享一个排空期限；资源栈在正常完成或超时取消后执行。
// 具体独立与混合运行样例后续在 pixiu-demo 中引入。
// Wait 超时后仍可能有业务运行；Cancel 不保证业务已结束，入口回收资源后退出。
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
// 此前成功创建的共享实例和日志交给应用资源栈关闭，不重复登记单个客户端 Close。
// 关闭前先停止并等待外部生产者及业务管理的订阅；NATS 托管消费由其 CloseAll 排空，
// 再依次关闭 Elasticsearch、Redis、MySQL、阿里云 ES、OSS 与日志。资源栈共享总期限，超时后入口立即退出。
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
// 阿里云 OSS 使用 MustRegisterAliyunOSS(name, isDefault) 声明，一个实例固定对应一个 Bucket。
// 日志初始化阶段之后、阿里云 ES 之前读取 aliyunossx.<name>.yaml，未声明时跳过。
// 模板见 conf.example/aliyunossx.example.yaml；文件不接受 name、isDefault。
// region、bucket、accessKeyID、accessKeySecret 必填，固定 AK/SK 由 ossx.New 内部封装。
// endpoint 可选，空值按 Region 推导；自定义值须为不含凭据、查询或非根路径的 HTTP(S) URL。
// requestTimeout 零值默认 60s，slowThreshold 零值默认 500ms，负值无效；时间须带单位。
// logRequests 默认 false，关闭全部结果日志；下游耗时登记独立生效。
// 全部文件先严格解析，再由 ossx.New 校验语义并构造；不联网验证 Bucket 或权限，没有 initTimeout。
// 文件不替换环境变量占位符；动态凭据仍可通过底层 ossx.Config.Credential 注入，与 AK/SK 互斥。
// 首个实例创建前只登记一次 ossx.CloseAll，部分失败由应用栈回收已完成的实例。
// ossx.Default() 与 ossx.Named(默认名称) 返回同一实例，上传请求不再指定 Bucket。
// 退出时先停止并等待使用方，按通用逆序在阿里云 ES 之后、日志之前关闭 OSS；Close 不排空在途上传。
//
// 阿里云 ES 管理端使用 MustRegisterAliyunES(name, isDefault) 声明，每个客户端固定一个 InstanceID。
// OSS 之后、MySQL 之前读取 aliyunesx.<name>.yaml，未声明时跳过；与 Elasticsearch 数据面独立。
// 模板见 conf.example/aliyunesx.example.yaml；name、isDefault 只由声明提供。
// region、instanceID、accessKeyID、accessKeySecret 必填；endpoint 可选，须为 HTTP(S) URL。
// requestTimeout 默认 60s，slowThreshold 默认 200ms，负值无效；logRequests 默认 false。
// 全部文件先严格解析，再由 esx.New 校验语义并构造固定 AK/SK 凭据，不联网验活，没有 initTimeout。
// 文件不替换环境变量占位符；动态 Credential 仍可直接注入 esx.Config，与 AK/SK 互斥。
// 首个实例创建前只登记一次 esx.CloseAll，部分失败由应用栈回收已完成的实例。
// esx.Default() 与 esx.Named(默认名称) 返回同一实例；查询和更新不再逐次指定 InstanceID。
// 日志类型为 AliyunES，instance_id 来自配置；不记录凭据、签名、下载 URL 或正文。
// 退出时按通用逆序在 MySQL 之后关闭阿里云 ES，随后关闭 OSS 和日志；Close 不排空在途请求。
//
// NATS 使用 MustRegisterNATS(name, isDefault) 声明，Elasticsearch 之后读取 nats.<name>.yaml。
// 全部 NATS 文件严格解析并转换后，先登记一次 natsx.CloseAll，再创建默认或命名客户端；
// 未声明时跳过。模板见 conf.example/nats.example.yaml；文件不接受 name、isDefault。
// loggerName 指定连接及消费链路的命名 Logger；模板为 nats，须在 app.yaml 的 log.names 中声明。
// 省略、null、空值或名称未注册时回退默认 Logger；HTTP/Cron 发布仍跟随调用方 ctx。
// 消费者通过 NewNATSConsumerService 纳入 Start，与 HTTP/Cron 同时停收，登记顺序不保证业务依赖。
// 全部服务共享总关闭预算；ErrShutdownTimeout 时取消剩余任务，入口仍执行资源栈回收，普通错误沿用管理器和资源栈汇总。
// 转换阶段检查 URL 解析及内嵌凭据，通用参数由 natsx 在构造时校验；
// 入口须在初始化前登记资源栈 defer，后续实例校验失败时也回收已创建的客户端。
// urls 必填，地址非空且无首尾空白，不接受 URL 内嵌凭据；认证使用 username/password。
// namespace 在此连接文件配置，省略、null 或空字符串时不添加前缀；仅允许 ASCII 字母、数字、_、-。
// natsx 在连接前校验并固定 namespace；纯发布客户端也生效，不依赖注册配置或 Handler。
// 超时零值使用 natsx 默认值，负值无效；connectTimeout 默认 5s，是单次拨号期限。
// drainTimeout 默认 10s，仅用于底层连接 Drain；closeTimeout 默认 20s，限制完整关闭流程。
// jetStream.publishAsyncTimeout 默认 5s，只限制默认上下文的异步 PubAck 等待。
// 连接成功不代表 JetStream 已启用或业务权限已验证。迁移和 Handler 依赖就绪后，
// NewNATSConsumerService 读取独立的 nats-register.<name>.yaml 并绑定 Handler，
// 构造错误原样返回；Run 调用 natsx 的三个注册入口：
//
//	jsHandlers := natsx.MustNewJetStreamHandlerRegistry()
//	jsHandlers.MustRegister(func(ctx context.Context, msg jetstream.Msg) error {
//	    // 完成本条业务后返回；组件根据结果 ACK 或重投。
//	    return nil
//	}, "HandleOrderCreated")
//	coreHandlers := natsx.MustNewCoreHandlerRegistry()
//	coreHandlers.MustRegister(func(ctx context.Context, msg *nats.Msg) error {
//	    return nil
//	}, "HandleConfigRefresh")
//	return bootstrap.Start(func() (apprun.Service, error) {
//		return bootstrap.NewNATSConsumerService("orders", jsHandlers, coreHandlers)
//	})
//
// 示例需导入 context、infra/apprun、infra/natsx、nats.go 和 nats.go/jetstream，并已初始化 orders 命名客户端及配置环境。
// 客户端未登记或 CloseAll 开始后查询沿用 natsx.Named 的 panic；配置错误由构造返回，注册失败由 Run 返回。
// 调用方无论排空结果都回收资源；Run/Stop/Wait/Cancel 分工，启动失败也收尾已启动消费。
// 服务的 Cancel 先复用 Stop，再取消剩余消费；可直接或重复调用，不关闭共享连接。
// Start 并发运行服务，登记顺序不规定停机依赖。手动注册可使用 natsx 的三个集中注册入口，资源回收前通过 StopConsumers、WaitConsumers 和必要的 CancelConsumers 处理消费。
// 注册 context 可由 Stop 取消，不设置整体注册期限；资源请求与 Core Flush 沿用 SDK 默认行为。
// Client 关闭状态在配置预检后交给 natsx 注册入口校验；长期 Handler 使用独立消息 context。
// 文件可使用 conf.example/nats-register.example.yaml，保留示例已登记的 Handler 对应项。
// JetStream 消费者配置在 streams[].consumers，自动继承父级 name；可配置多个消费者或只声明 Stream。
// 每项 Stream 必须声明 subjects，统一创建或校验；不支持仅引用外部 Stream。
// 旧顶层 jetStreamConsumers 和消费者的 stream 字段须移除，否则严格解析报错。
// 消费者的可选参数注释同样位于 consumers 项内，按需取消注释即可使用。
// 注册文件不接受 namespace；该值统一来自 nats.<name>.yaml 的客户端配置。
// 例如 namespace: test1 时，Stream 名称及引用 ORDERS 变为 test1_ORDERS，Durable worker 变为 test1_worker；
// Core name、非空 queue 同样添加 test1_；subjects、filterSubjects、Core subject 添加 test1.。
// 通配主题 orders.> 变为 test1.orders.>；空 queue 保持广播，空过滤列表仍消费整个 Stream。
// 配置填写原始名称，Handler 引用不加前缀；消费者继承的 Stream 名称由注册入口添加一次前缀。
// 发布和 Request 同样传入原始主题，例如 orders.created；Client 自动发送到 test1.orders.created。
// 回调、日志和原生返回值保留前缀；Request 回复用 Msg.Respond，不改写回复 inbox。
// Conn()/JetStream() 原生入口不添加前缀；旧注册文件中的 namespace 须移至连接文件，否则严格解析报错。
// 修改 namespace 后使用新的资源名称，旧 Stream/Durable 不自动迁移或删除。
// 包级具名函数可直接传入构造器，自动名称保留大小写；方法值、闭包、泛型函数必须显式命名。
// streamDefaults.replicas 可省略或为 null，沿用当前实例初始化时 nats.<name>.yaml 中 urls 的条目数。
// streamDefaults 整体省略、null 或空对象同样回退；显式 replicas 解码后必须为正整数，0 或负数报错。
// 自动计数不去重、不计入自动发现节点、不重读连接文件；直接创建命名客户端时采用 natsx.Config.URLs 的数量。
// urls 仅列种子地址、使用代理入口或需要不同副本策略时应显式配置；最终全局值即使没有 Stream 也校验。
// 逐项 replicas 解码后非 nil 且大于零时覆盖，否则继承；解码及数字转换沿用 configx。
// 逐项 replicas 不得超过显式或自动推导的全局值，超限时在注册任何资源前返回错误。
// Stream maxBytes、retention、maxAge、duplicates 可选，省略或 null 沿用 Default（当前为 -1、limits、90 天、2 分钟）。
// maxBytes 使用整数字节数，例如 104857600 表示 100 MiB；0 或 -1 不设 Stream 容量上限，仍受账户及服务端配额限制。
// 数字解码沿用 configx，不接受带单位字符串；范围及配额校验交给服务端。
// retention 只接受 limits、interest、workqueue，非法策略在注册任何资源前报错。
// 两个时长沿用 configx 解码，推荐 2160h、2m 等 Go 时长字符串；数字按纳秒处理，不支持 d 单位。
// maxAge 为 0 时不限制消息年龄；duplicates 为 0 时由服务端确定窗口，不代表关闭去重。
// 时长范围及组合约束由服务端校验；maxAge 大于 0 时，去重窗口不得超过它。
// JetStream 的 ackWait、maxDeliver、maxAckPending、consume.workers、consume.maxMessages、retryDelay 可选。
// 省略或 null 沿用 Default（当前依次为 60s、5、1000、1、0、0）；consume 整体也可省略或为 null。
// 时长沿用 configx 的 Go 时长字符串或纳秒数解码；显式值覆盖 Default，交给 natsx 和服务端校验。
// ackWait 必须大于 0；retryDelay 不得为负，0 等待 ackWait 超时重投，正值在失败后发送延迟 NAK。
// consume.workers、consume.maxMessages 不得为负；零值分别按 1、有效 workers 的两倍处理。
// workers 控制本地 Handler 并发，maxMessages 是不含处理中消息的 SDK 预取上限。
// maxDeliver 包含首次投递；maxAckPending 是同一 Durable 下所有实例合计的未确认消息数上限。
// coreConsumers 的 queue 非空时，subscribers 必须配置且解码后大于 1，否则在注册任何资源前报错。
// 按 subscribers 为当前 Client 建立同组订阅，多订阅 Handler 须支持并发。
// queue 为空时始终只有一个广播订阅，subscribers 可省略，负数由 natsx 注册入口拒绝。
// 所有其他运行参数沿用 natsx Default，未引用的 Handler 不启动。
// 配置层检查全局副本数、逐项副本数上限、Stream subjects 列表必填及 retention、Core 队列订阅数，
// 并在注册前绑定全部 Handler。
// 各类资源的通用参数校验和判重交给各自的 natsx 注册入口，失败也占用该入口的注册次数。
// 三个入口各调用一次，空清单也占用次数，不与手动注册混用。
// 各入口分阶段预检，后序校验失败时前序资源或消费可能已启动。
// Run 注册期间可能执行业务；失败先由管理器排空已启动消费，再由资源栈收尾，不自动重试或删除远端资源。
// 修改文件重启生效；删除声明不删除 Durable 或进度。消息协议和业务幂等由应用负责。
// NATS 消费服务的 Stop 启动消费排空，Wait 等待实际回调结束；期间可查询 Default/Named、发布和请求。
// 排空完成或超时取消后进入资源回收；资源栈默认总期限 20 秒，可通过 lifecycle.New 自定义。
// 客户端 CloseTimeout 约束后续异步确认及连接关闭，日志最后关闭。
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
// 项目可在 MustBaseInit 后、业务启动前调用 RunMySQLMigration(name, tables)，
// tables 使用 infra/schemamigrate.Table；严格读取 migrate.<name>.yaml，文件必需。
// enabled 默认 false；开启后逐表执行 GORM AutoMigrate，内部创建 Context，timeout 默认 5m。
// 只迁移显式登记的表，忽略模型关联及其外键；关联表和多对多中间表须单独登记。
// 关闭或空清单不取数据库；迁移核心从 mysqlx.Named(name) 获取主库，失败返回错误与部分报告。
// 迁移不会自动挂到 MustBaseInit 或连接构造中；没有迁移锁，发布时避免多实例同时执行。
// 模板见 conf.example/migrate.example.yaml，详细契约见 infra/schemamigrate/doc.go。
//
// 需要显式设置运行期总关闭预算时，使用 StartWithConfig（另需导入标准库 time）：
//
//	func startWithTimeout(handler http.Handler) error {
//		return bootstrap.StartWithConfig(bootstrap.StartConfig{
//			ShutdownTimeout: 2 * time.Minute,
//		}, func() (apprun.Service, error) {
//			return bootstrap.NewHTTPService(handler)
//		})
//	}
//
// StartWithConfig 要求显式预算至少 5s；Start/NewAppManager 默认仍为 30s。
// 总预算不包含资源栈、Client.Close 和日志；这些由应用随后调用 lifecycle.Stack.Close 回收，默认整体 20 秒，日志最后关闭。
package bootstrap
