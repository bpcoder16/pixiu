package lrucache

import (
	"fmt"
	"sync"
)

// namedCaches 保存进程内的命名缓存实例；同一个名字只允许注册一次。
var namedCaches sync.Map // name → *Cache[K, V]

// namedCacheInitMu 串行化首次创建，避免并发注册构造多个实例；已注册名称读取无需此锁。
var namedCacheInitMu sync.Mutex

// NewNamed 首次创建并注册命名缓存，后续同名、同类型调用返回已有实例，忽略传入配置。
// 名称不能为空；类型不匹配返回错误。创建失败不占用名称，可用有效配置重试。
// 各名称的容量、TTL、条目和统计互不影响。
func NewNamed[K comparable, V any](name string, cfg Config[K, V]) (*Cache[K, V], error) {
	if name == "" {
		return nil, fmt.Errorf("lrucache: cache name must not be empty")
	}
	if value, ok := namedCaches.Load(name); ok {
		return castNamedCache[K, V](name, value)
	}
	namedCacheInitMu.Lock()
	defer namedCacheInitMu.Unlock()
	// 等待锁期间其他调用可能已完成注册，须再次检查再创建。
	if value, ok := namedCaches.Load(name); ok {
		return castNamedCache[K, V](name, value)
	}
	cache, err := New(cfg)
	if err != nil {
		return nil, err
	}
	namedCaches.Store(name, cache)
	return cache, nil
}

// Named 返回已注册的命名缓存。名称不存在或请求的键值类型不匹配时返回错误。
// 调用方可在初始化时获取一次并保存指针，避免业务热路径重复查找注册表。
func Named[K comparable, V any](name string) (*Cache[K, V], error) {
	value, ok := namedCaches.Load(name)
	if !ok {
		return nil, fmt.Errorf("lrucache: cache %q is not registered", name)
	}
	return castNamedCache[K, V](name, value)
}

func castNamedCache[K comparable, V any](name string, value any) (*Cache[K, V], error) {
	cache, ok := value.(*Cache[K, V])
	if !ok {
		return nil, fmt.Errorf("lrucache: cache %q has different key or value types", name)
	}
	return cache, nil
}
