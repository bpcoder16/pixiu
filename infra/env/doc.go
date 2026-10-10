// Package env 提供独立于启动方式的应用运行环境，供 HTTP、命令行等应用复用。
// 本包依赖标准库和 netx，不读取配置文件；配置加载和组件初始化由上层负责。
//
// 启动入口准备好配置和已存在且可列举、可遍历的目录后调用（需导入
// github.com/bpcoder16/pixiu/infra/env）：
//
//	if err := env.Init(env.Config{
//	    AppName:       "example-cmd",
//	    RunMode:       env.RunModeRelease,
//	    TimeLocation:  "Asia/Shanghai",
//	    ConfigDirPath: "/etc/example-cmd",
//	    LocalIP:       "192.0.2.10",
//	}); err != nil {
//	    panic(err)
//	}
//	_ = env.AppName()
//	_ = env.RunMode()
//	_ = env.TimeLocation()
//	_ = env.ConfigDirPath()
//	_ = env.LocalIP()
//	_ = env.RootDirPath()
//
// AppName 不能为空或全为空白；RunMode 只接受 debug、test、release；
// TimeLocation 必填并通过 time.LoadLocation 解析，不内嵌时区数据库。
// Config.ConfigDirPath 由调用方提供，必须是已存在且当前进程可列举、可遍历的绝对目录，
// 校验后原样保存，不清理路径，不从配置文件解析。符号链接校验其目标目录；
// 只试读一个目录条目验证列举权限，空目录有效；不要求目录可写，
// 不递归检查子目录或文件内容读取权限。后续文件访问仍由使用方处理错误。
// 目录关闭失败会合并到校验错误中并保留错误链，不发布环境。
// LocalIP 可选，非空时必须是 IPv4 或 IPv6 地址并原样保存；空字符串调用
// netx.LocalIPv4 查询，失败后调用 netx.LocalIPv6，均失败时保留空值，不阻止初始化。
// 自动选择遵循 netx 的网卡筛选规则，
// 不保证是主网卡或默认出站地址。补齐值只保存到环境快照，不修改输入配置。
// 依赖非空 IP 的应用应显式配置或在初始化后检查 LocalIP()。
// RootDirPath 在 Init 时通过 os.Getwd 自动获取工作目录绝对路径，不需要配置；
// 获取失败不发布环境。初始化后切换工作目录不会更新该快照。
//
// 全部校验成功后原子发布环境快照，并发初始化只有一次成功；任何启动入口
// 再次初始化都返回 ErrAlreadyInitialized。校验失败不发布环境，允许修正后重试。
// 初始化前调用读取方法会 panic，成功后可并发读取。修改输入配置不会更新环境。
// 本包不修改 time.Local，不提供重置或热更新，不初始化其他组件。
// 接入 biz/bootstrap 时，由其在日志初始化前将 time.Local 设置为 TimeLocation()。
package env
