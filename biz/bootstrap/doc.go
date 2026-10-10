// Package bootstrap 提供日志、MySQL、Redis 和 Elasticsearch 多实例的通用初始化入口，供项目自己的 bootstrap
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
//	bootstrap.MustRegisterRedis("orders", true)
//	bootstrap.MustRegisterElasticsearch("orders", true)
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
// Redis 使用 MustRegisterRedis(name, isDefault) 声明，与 MySQL 独立判重，允许同名。
// MySQL 初始化后，读取配置目录下的 redis.<name>.yaml，先严格解析、校验全部 Redis 文件，
// 再按声明顺序创建并 Ping 验活；未声明时不初始化或关闭 Redis 模块。
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
// 关闭前先停止并等待业务任务及订阅退出，再逆序关闭项目资源、Elasticsearch、Redis、MySQL 与日志。
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
