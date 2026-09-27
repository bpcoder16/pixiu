package lrucache

import (
	"fmt"
	"sync"
)

// namedCaches 保存进程内的命名缓存实例；同一个名字只允许注册一次。
var namedCaches sync.Map // name → *Cache[K, V]

// NewNamed 创建并注册命名缓存。名称不能为空，重复名称不会替换已有实例。
// 各名称的容量、TTL、条目和统计互不影响。
func NewNamed[K comparable, V any](name string, cfg Config[K, V]) (*Cache[K, V], error) {
	if name == "" {
		return nil, fmt.Errorf("lrucache: cache name must not be empty")
	}
	cache, err := New(cfg)
	if err != nil {
		return nil, err
	}
	if _, loaded := namedCaches.LoadOrStore(name, cache); loaded {
		return nil, fmt.Errorf("lrucache: cache %q is already registered", name)
	}
	return cache, nil
}

// Named 返回已注册的命名缓存。名称不存在或请求的键值类型不匹配时返回错误。
// 调用方可在初始化时获取一次并保存指针，避免业务热路径重复查找注册表。
func Named[K comparable, V any](name string) (*Cache[K, V], error) {
	value, ok := namedCaches.Load(name)
	if !ok {
		return nil, fmt.Errorf("lrucache: cache %q is not registered", name)
	}
	cache, ok := value.(*Cache[K, V])
	if !ok {
		return nil, fmt.Errorf("lrucache: cache %q has different key or value types", name)
	}
	return cache, nil
}
