// Package httpconfig 组合 configx 和 env，提供 HTTP 应用启动配置加载与环境初始化。
// 第一版包含 env.appName、env.runMode、env.timeLocation 三个必填字段和可选 localIP。
//
// 将本包 conf.example/app.yaml 复制到应用的 conf/app.yaml 并修改后，
// 在应用启动时调用（需导入 github.com/bpcoder16/pixiu/biz/httpconfig
// 和 github.com/bpcoder16/pixiu/infra/env）：
//
//	cfg := httpconfig.MustLoadAppConfig("./conf/app.yaml")
//	_ = cfg.Env.AppName
//	_ = env.AppName()
//	_ = env.RunMode()
//	_ = env.TimeLocation()
//	_ = env.ConfigDirPath()
//	_ = env.LocalIP()
//	_ = env.RootDirPath()
//
// 文件读取复用 infra/configx，支持 YAML、TOML、JSON，未知字段和弱类型转换报错。
// AppConfig.Env 复用 env.Config，并交给 env.Init 校验及发布环境。
// 应用名称不能为空或全为空白；运行模式必须为 debug、test、release；时区须能通过
// time.LoadLocation 解析。加载器不内嵌时区数据库，时区数据使用标准库查找机制。
// localIP 非空时须为 IPv4 或 IPv6，空值由 env 先查询本机 IPv4，失败后查询 IPv6；
// 两次查询均失败时保留空值，不阻止启动，补齐结果不回写返回的配置。
// 依赖非空 IP 的应用应显式配置或在初始化后检查 env.LocalIP()。
// 相对文件路径基于工作目录；入口将文件绝对路径的父目录写入 cfg.Env.ConfigDirPath，
// 再交给 env.Init 原样保存，供 env.ConfigDirPath() 读取；该字段不从文件解析，
// 不追踪符号链接到目标文件的目录。不自动搜索配置文件或回退到模板。
// 配置目录必须可列举、可遍历，不要求可写，不递归检查子目录或文件内容读取权限。
// env.RootDirPath() 保存 env.Init 时的工作目录绝对路径快照，由 env 自动获取。
//
// MustLoadAppConfig 失败或重复调用时 panic，仅用于启动期，失败应退出进程。
// configx 保留名称为 pixiu.biz.httpconfig.app，应用不得占用此名称。
// configx 在结构体解析成功后注册；环境校验失败可能保留该注册项，
// 因此不承诺在同一进程中恢复并重试，也不提供重置或热更新。
//
// env 全部校验成功后一次性发布环境快照，之后可并发读取；初始化前读取会 panic。
// 如果环境已由其他启动入口初始化，本入口会 panic 并保留原环境。
// 返回的配置是共享指针，修改后不会自动更新全局环境，配置并发读写由调用方协调。
// 加载不修改 time.Local 或 Gin 模式，也不初始化日志或其他组件。
package httpconfig
