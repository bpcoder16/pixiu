// Package redislock 通过 infra/redisx 实现单机 Redis 上的非阻塞锁。
// 获取以 SET NX PX 原子写入随机令牌及 TTL，释放以 Lua 原子比较令牌并删除。
//
// 以下示例需导入 context、time、github.com/redis/go-redis/v9、
// github.com/bpcoder16/pixiu/infra/redisx、github.com/bpcoder16/pixiu/biz/lockx
// 和 github.com/bpcoder16/pixiu/biz/lockx/redislock；ctx 由应用提供：
//
//	client, err := redisx.New(ctx, redisx.Config{
//	    Name: "locks",
//	    Options: redis.Options{
//	        Addr:         "127.0.0.1:6379",
//	        MaxRetries:   -1,
//	        DialTimeout:  time.Second,
//	        ReadTimeout:  time.Second,
//	        WriteTimeout: time.Second,
//	    },
//	})
//	if err != nil {
//	    return err
//	}
//	// 实际应用应在停止业务和释放锁之后、关闭日志之前关闭客户端。
//	defer client.Close()
//	locker, err := redislock.New(client, redislock.Config{TTL: 10 * time.Second})
//	if err != nil {
//	    return err
//	}
//	executed, err := lockx.TryDo(ctx, locker, "orders:lock:confirm:123",
//	    func(ctx context.Context) error {
//	        return confirm(ctx)
//	    },
//	)
//	if err != nil {
//	    return err
//	}
//	if !executed {
//	    // 已由其他执行者持有，按业务策略跳过或返回“处理中”。
//	    return nil
//	}
//	return nil
//
// confirm 为应用提供的业务函数。Locker 复用并可并发使用传入的客户端，
// 不负责连接池的创建和关闭。New 要求创建客户端时设置 MaxRetries=-1，
// 避免响应丢失后的自动重试掩盖不确定结果。
//
// 锁不可重入，不自动续租；ttl 向上取整到毫秒，包含获取阶段消耗的时间。
// 已到期的旧句柄不能删除新锁，重复释放返回 lockx.ErrNotHeld。
// 网络失败不能确定请求是否执行，业务仅在确认获取成功后运行。
// Redis 命令继续经过 redisx 日志与请求耗时登记；参数日志可能包含 key 和令牌。
// 单机故障切换及超时后仍在运行的业务不具备严格互斥保证，详见 docs/lockx-design.md。
package redislock
