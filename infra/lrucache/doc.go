// Package lrucache 提供有界 LRU 缓存，支持独立实例和命名共享实例，可设置实例默认 TTL 和单条目 TTL。
// 它位于基础功能层，使用 ttlcache v3；设计与边界见 docs/lrucache-design.md。
//
// 应用启动时可创建独立实例并注入业务服务，也可按名称注册进程内共享实例。
// 以下示例需导入 time 和 github.com/bpcoder16/pixiu/infra/lrucache：
//
//	sourceCodes := map[string]string{"+86": "CN"}
//	_, err := lrucache.NewNamed("country-codes", lrucache.Config[string, string]{
//	    Capacity: 1_000,
//	    TTL:      24 * time.Hour,
//	    Loader: func(key string) (string, bool) {
//	        value, found := sourceCodes[key]
//	        return value, found
//	    },
//	})
//	if err != nil {
//	    return err
//	}
//	countryCodes, err := lrucache.Named[string, string]("country-codes")
//	if err != nil {
//	    return err
//	}
//	code, found := countryCodes.Get("+86")
//	_, _ = code, found
//
// New 仍可创建不注册的独立缓存。NewNamed 拒绝空名称和重复注册；
// Named 在名称不存在或键值类型不匹配时返回错误，不回退到其他实例。
// 命名实例在进程内持续存在；可在业务初始化时获取一次并保存指针。
// Get 命中会更新 LRU 顺序，默认不延长条目 TTL；配置 RefreshTTLOnGet: true 后会续期。
// 配置 Loader 后，未命中时会同步调用它；返回找到的值会按实例默认 TTL 写入。
// Loader 不传递 context 或错误，同键并发未命中可能重复加载。
// Loader 回源期间若调用 Delete 或 DeleteAll，回源完成后仍可能重新写入该值。
// GetOrSet 和 GetOrSetFunc 提供原子检查及写入，返回的 bool 表示检查时是否存在；
// 返回值在解锁后读取，并发 Set 时可能与 bool 不对应同一时刻。
// 两者均不触发 Loader。GetOrSetFunc 的函数在缓存锁内执行，应快速完成且不可重入缓存。
// Stats 的 Hits 和 Misses 统计 Get、GetOrSet、GetOrSetFunc 与 GetAndDelete 的查找。
// Has 和 GetAndDelete 不触发 Loader；Keys、Items 和 Range 系列仅访问未过期条目。
// Range 系列按 LRU 方向遍历，并发修改时不保证得到完整快照。
// SetWithTTL 的 0 表示该条目不过期。
// 不启动后台清理；写入前会清理过期条目，纯读期间可按需调用 DeleteExpired。
// 容量按条目数计算；可变引用类型的并发安全和拷贝由调用方负责。
package lrucache
