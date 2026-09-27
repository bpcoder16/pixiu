// Package lrucache 提供独立实例的有界 LRU 缓存，可设置实例默认 TTL 和单条目 TTL。
// 它位于基础功能层，使用 ttlcache v3；设计与边界见 docs/lrucache-design.md。
//
// 应用启动时创建不同用途的缓存并注入业务服务，不提供全局单例。
// 以下示例需导入 time 和 github.com/bpcoder16/pixiu/infra/lrucache：
//
//	sourceCodes := map[string]string{"+86": "CN"}
//	countryCodes, err := lrucache.New[string, string](lrucache.Config[string, string]{
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
//	code, found := countryCodes.Get("+86")
//	_, _ = code, found
//
// Get 命中会更新 LRU 顺序，默认不延长条目 TTL；配置 RefreshTTLOnGet: true 后会续期。
// 配置 Loader 后，未命中时会同步调用它；返回找到的值会按实例默认 TTL 写入。
// Loader 不传递 context 或错误，同键并发未命中可能重复加载。
// SetWithTTL 的 0 表示该条目不过期。
// 不启动后台清理；写入前会清理过期条目，纯读期间可按需调用 DeleteExpired。
// 容量按条目数计算；可变引用类型的并发安全和拷贝由调用方负责。
package lrucache
