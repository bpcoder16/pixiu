// Package redisx 管理单机 Redis 客户端的创建、验活、关闭和可配置的命令结果日志。
// 设计与边界见 docs/redisx-design.md。配置文件由应用解析；一个实例对应一个逻辑 Redis 目标。
//
// 以下两个示例需导入 errors、time、github.com/redis/go-redis/v9、
// github.com/bpcoder16/pixiu/infra/redisx、github.com/bpcoder16/pixiu/lifecycle
// 和 github.com/bpcoder16/pixiu/logit；logger 由应用提供。
// 独立客户端使用 New 创建，每个客户端都单独登记 Close：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error {
//	    return logit.Close(logger)
//	}); err != nil {
//	    return err
//	}
//	client, err := redisx.New(redisx.Config{
//	    Name: "cache",
//	    Options: redis.Options{
//	        Addr:         "127.0.0.1:6379",
//	        MaxRetries:   -1,
//	        DialTimeout:  time.Second,
//	        ReadTimeout:  time.Second,
//	        WriteTimeout: time.Second,
//	    },
//	})
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	if err := stack.Register(client.Close); err != nil {
//	    return errors.Join(err, client.Close(), stack.Close())
//	}
//	// 业务处理期间使用 client.Client()；停止任务和订阅后统一关闭。
//	return stack.Close()
//
// 命名客户端使用 NewNamed 按 Config.Name 创建并登记，所有命名客户端只登记
// 一次 CloseAll。下例 cfg 和 sessionCfg 是应用已构造的两个 Config：
//
//	var stack lifecycle.Stack
//	if err := stack.Register(func() error {
//	    return logit.Close(logger)
//	}); err != nil {
//	    return err
//	}
//	if err := stack.Register(redisx.CloseAll); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	_, err := redisx.NewNamed(cfg)
//	if err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	if _, err := redisx.NewNamed(sessionCfg); err != nil {
//	    return errors.Join(err, stack.Close())
//	}
//	client := redisx.Named(cfg.Name)
//	_ = client
//	// 业务处理期间使用 client.Client() 执行命令。
//	// 应用退出阶段，停止业务和订阅后：
//	return stack.Close()
//
// Named 对不存在的名称或已开始关闭的注册表会 panic。启动阶段由调用方串行创建
// 命名客户端和默认客户端，不并发初始化。所有创建返回后才
// 按名称查询；停止使用客户端的任务和订阅后再调用 CloseAll。
// NewNamed 不与 CloseAll 并发调用，运行期不再新增命名客户端。
// 独立 New 创建的客户端仍由调用方单独关闭。
// 应用先登记日志关闭函数，再登记 Redis 关闭函数，确保日志最后关闭。
// 命名客户端不应单独关闭或重复登记关闭函数。
//
// New、NewNamed 和 NewDefault 不接收外部 ctx，初始化 Ping 使用内部
// context.Background()；超时和重试由 Config.Options 控制，不额外设置总 deadline。
// 初始化失败时清理连接并返回错误。客户端生命周期由 Close 或 CloseAll 显式管理。
// 真实 Redis 命令继续使用调用方 ctx；未禁用驱动读写 deadline 时，命令读写会参考
// ctx 的截止时间。主动 cancel 不保证打断进行中的 socket I/O，取消后仍可能返回成功。
// 业务命令需要限制等待时间时，应使用带 deadline 的 context 并保留合理的读写超时；
// 取消业务 ctx 不关闭客户端，其他选项沿用 go-redis 语义。
// Network 是传输方式，Addr 是连接地址。普通 TCP 连接只需设置 host:port 形式的
// Addr，Network 留空时驱动默认使用 tcp；从 Host、Port 组装时建议用 net.JoinHostPort
// 兼容 IPv6。Unix socket 连接应设置 Network: "unix" 和路径 Addr；TLS 连接仍使用
// TCP 地址，并设置 TLSConfig，不应将 Network 设为 "tls"。redisx 要求 Addr 非空。
// SlowThreshold 为 0 时默认 200 毫秒，负数无效。
// MaxRetries 为 0 时驱动默认重试 3 次，为 -1 时禁用重试；非幂等操作应显式选择。
// 默认只记录失败与慢调用；LogCommands 开启后正常命令也以 Debug 输出。
// 所有已输出的业务命令日志均附加完整请求参数，可能包含 key、值和凭据。
// 日志保留 error_type 分类，并以 error_code 输出白名单内的 Redis 错误码，
// 其他情况为空字符串；不包含命令返回值或原始错误文本。底层客户端供业务使用全部命令、
// Pipeline、Lua 和 Pub/Sub；调用方不能自行关闭它或在运行期修改配置与 Hook。
// ctx 已调用 logit.WithStart 时，每次业务命令或批量执行会以 Redis_<Name> 为前缀自动编号记录下游耗时，
// 供业务调用 logit.InfoDuration 汇总；此登记不受 LogCommands 和日志级别限制。
// 未调用 WithStart 时跳过；初始化 Ping 和连接握手命令不计入。
//
// 单个逻辑下游可显式初始化默认客户端，省去每次按名称查询。cfg 和业务 ctx 由应用提供：
//
//	if _, err := redisx.NewDefault(cfg); err != nil {
//	    return err
//	}
//	return redisx.Default().Client().Ping(ctx).Err()
//
// 启动阶段由调用方串行调用 NewNamed 和 NewDefault，不并发初始化。
// Name 仍必填；Default() 与 Named(cfg.Name) 返回同一实例，New 和 NewNamed
// 不自动设置默认实例。重复默认初始化报错；失败可重试。未初始化或 CloseAll
// 开始后调用 Default 会 panic。应用停止任务和订阅后调用 CloseAll，默认与命名实例
// 统一关闭一次，日志最后关闭；默认实例不要单独登记 Close。
package redisx
