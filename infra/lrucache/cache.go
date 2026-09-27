package lrucache

import (
	"fmt"
	"time"

	"github.com/jellydator/ttlcache/v3"
)

// Config 配置单个缓存实例。
type Config[K comparable, V any] struct {
	Capacity        int               // 必填，最大条目数，必须大于 0。
	TTL             time.Duration     // 可选，默认过期时间；0 表示默认不过期。
	RefreshTTLOnGet bool              // 可选，命中时重新计算过期时间；默认 false，不续期。
	Loader          func(K) (V, bool) // 可选，未命中时同步加载；返回 false 表示未找到，须支持并发调用。
}

// Stats 是单个缓存实例创建以来的累计统计。删除和过期清理也计入 Evictions。
type Stats struct {
	Hits       uint64 // Get 命中次数。
	Misses     uint64 // 缓存未命中次数，包括 Loader 成功回源的读取。
	Insertions uint64 // 新键写入次数。
	Updates    uint64 // 已有键的值被覆盖次数。
	Evictions  uint64 // 条目移除次数，包括容量淘汰、删除和过期清理。
}

// Cache 是并发安全、有界且可按条目设置过期时间的 LRU 缓存。零值不可用，须经 New 创建。
type Cache[K comparable, V any] struct {
	cache *ttlcache.Cache[K, V]
}

// New 创建独立缓存实例。容量必须大于 0，默认 TTL 不得为负。
// 实例固定启用容量上限；默认读命中不续期，可通过 RefreshTTLOnGet 开启续期。
func New[K comparable, V any](cfg Config[K, V]) (*Cache[K, V], error) {
	if cfg.Capacity <= 0 {
		return nil, fmt.Errorf("lrucache: capacity must be positive: %d", cfg.Capacity)
	}
	if cfg.TTL < 0 {
		return nil, fmt.Errorf("lrucache: TTL must not be negative: %s", cfg.TTL)
	}
	opts := []ttlcache.Option[K, V]{
		ttlcache.WithCapacity[K, V](uint64(cfg.Capacity)),
		ttlcache.WithTTL[K, V](cfg.TTL),
	}
	if !cfg.RefreshTTLOnGet {
		opts = append(opts, ttlcache.WithDisableTouchOnHit[K, V]())
	}
	if cfg.Loader != nil {
		opts = append(opts, ttlcache.WithLoader[K, V](ttlcache.LoaderFunc[K, V](
			func(c *ttlcache.Cache[K, V], key K) *ttlcache.Item[K, V] {
				value, found := cfg.Loader(key)
				if !found {
					return nil
				}
				// 与 Cache.Set 一致，先清理过期条目，避免它们占满容量后淘汰有效条目。
				c.DeleteExpired()
				return c.Set(key, value, ttlcache.DefaultTTL)
			},
		)))
	}
	return &Cache[K, V]{cache: ttlcache.New[K, V](opts...)}, nil
}

// Set 使用实例默认 TTL 写入。覆盖已有键时从本次写入重新计时。
func (c *Cache[K, V]) Set(key K, value V) {
	// 底层容量计算会包含尚未物理删除的过期条目，先清理以免误淘汰有效条目。
	c.cache.DeleteExpired()
	c.cache.Set(key, value, ttlcache.DefaultTTL)
}

// SetWithTTL 使用条目 TTL 写入。TTL 为 0 表示不过期；负数会返回错误且不写入。
func (c *Cache[K, V]) SetWithTTL(key K, value V, ttl time.Duration) error {
	if ttl < 0 {
		return fmt.Errorf("lrucache: item TTL must not be negative: %s", ttl)
	}
	if ttl == 0 {
		ttl = ttlcache.NoTTL
	}
	c.cache.DeleteExpired()
	c.cache.Set(key, value, ttl)
	return nil
}

// GetOrSet 返回已有值，或按实例默认 TTL 写入给定值；bool 表示写入前是否存在。
// 此方法不会触发 Loader。
func (c *Cache[K, V]) GetOrSet(key K, value V) (V, bool) {
	c.cache.DeleteExpired()
	item, existed := c.cache.GetOrSet(key, value)
	return item.Value(), existed
}

// GetOrSetFunc 返回已有值，或在未命中时调用 fn 生成并写入值；bool 表示写入前是否存在。
// fn 在缓存锁内执行，须快速完成且不可调用同一缓存的方法；此方法不会触发 Loader。
func (c *Cache[K, V]) GetOrSetFunc(key K, fn func() V) (V, bool) {
	c.cache.DeleteExpired()
	item, existed := c.cache.GetOrSetFunc(key, fn)
	return item.Value(), existed
}

// Get 读取未过期条目。命中会更新 LRU 顺序；是否续期由 RefreshTTLOnGet 决定。
// 未命中时若配置 Loader，则同步加载；加载成功按实例默认 TTL 写入并返回。
func (c *Cache[K, V]) Get(key K) (V, bool) {
	item := c.cache.Get(key)
	if item == nil {
		var zero V
		return zero, false
	}
	return item.Value(), true
}

// Has 判断键是否对应未过期条目，不更新 LRU 顺序或 TTL，也不触发 Loader。
func (c *Cache[K, V]) Has(key K) bool {
	return c.cache.Has(key)
}

// Delete 删除指定键；键不存在时不执行操作。
func (c *Cache[K, V]) Delete(key K) {
	c.cache.Delete(key)
}

// GetAndDelete 原子地读取并删除未过期条目；未命中时不触发 Loader。
func (c *Cache[K, V]) GetAndDelete(key K) (V, bool) {
	item, found := c.cache.GetAndDelete(key, ttlcache.WithLoader[K, V](nil))
	if !found {
		var zero V
		return zero, false
	}
	return item.Value(), true
}

// DeleteAll 删除此实例的全部条目，包括已过期但尚未清理的条目。
func (c *Cache[K, V]) DeleteAll() {
	c.cache.DeleteAll()
}

// DeleteExpired 立即删除此实例中已经过期的条目。
func (c *Cache[K, V]) DeleteExpired() {
	c.cache.DeleteExpired()
}

// Len 返回当前未过期的条目数。
func (c *Cache[K, V]) Len() int {
	return c.cache.Len()
}

// Keys 返回未过期键的切片，顺序不作保证。
func (c *Cache[K, V]) Keys() []K {
	return c.cache.Keys()
}

// Items 返回未过期条目的键值映射；映射独立，值为浅拷贝。
func (c *Cache[K, V]) Items() map[K]V {
	items := c.cache.Items()
	values := make(map[K]V, len(items))
	for key, item := range items {
		values[key] = item.Value()
	}
	return values
}

// Range 按最近使用到最久未使用的方向遍历未过期条目；fn 返回 false 时停止。
// 并发修改缓存时遍历为弱一致语义，可能跳过条目；fn 不应依赖完整快照。
func (c *Cache[K, V]) Range(fn func(K, V) bool) {
	c.cache.Range(func(item *ttlcache.Item[K, V]) bool {
		return fn(item.Key(), item.Value())
	})
}

// RangeBackwards 按最久未使用到最近使用的方向遍历未过期条目；fn 返回 false 时停止。
// 并发修改缓存时遍历为弱一致语义，可能跳过条目；fn 不应依赖完整快照。
func (c *Cache[K, V]) RangeBackwards(fn func(K, V) bool) {
	c.cache.RangeBackwards(func(item *ttlcache.Item[K, V]) bool {
		return fn(item.Key(), item.Value())
	})
}

// Stats 返回此实例的累计统计快照。
func (c *Cache[K, V]) Stats() Stats {
	m := c.cache.Metrics()
	return Stats{
		Hits:       m.Hits,
		Misses:     m.Misses,
		Insertions: m.Insertions,
		Updates:    m.Updates,
		Evictions:  m.Evictions,
	}
}
