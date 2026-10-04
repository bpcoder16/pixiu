package lrucache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLoaderOnMiss(t *testing.T) {
	loads := 0
	cache, err := New[string, int](Config[string, int]{
		Capacity: 2,
		Loader: func(key string) (int, bool) {
			loads++
			if key == "found" {
				return 42, true
			}
			return 0, false
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value, ok := cache.Get("found"); !ok || value != 42 {
		t.Fatalf("loaded value = %d, %v", value, ok)
	}
	if value, ok := cache.Get("found"); !ok || value != 42 {
		t.Fatalf("cached value = %d, %v", value, ok)
	}
	for range 2 {
		if _, ok := cache.Get("missing"); ok {
			t.Fatal("loader returned a missing value")
		}
	}
	if loads != 3 {
		t.Fatalf("loader calls = %d, want 3", loads)
	}
	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 3 || stats.Insertions != 1 {
		t.Fatalf("unexpected loader stats: %+v", stats)
	}
}

func TestLoaderUsesDefaultTTLAndPreservesLiveItems(t *testing.T) {
	loads := 0
	cache, err := New[string, int](Config[string, int]{
		Capacity: 2,
		TTL:      20 * time.Millisecond,
		Loader: func(key string) (int, bool) {
			loads++
			return loads, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SetWithTTL("live", 100, 0); err != nil {
		t.Fatal(err)
	}
	cache.Set("expired", 200)
	time.Sleep(35 * time.Millisecond)
	if value, ok := cache.Get("new"); !ok || value != 1 {
		t.Fatalf("first loaded value = %d, %v", value, ok)
	}
	if value, ok := cache.Get("live"); !ok || value != 100 {
		t.Fatalf("live item was evicted: %d, %v", value, ok)
	}
	time.Sleep(35 * time.Millisecond)
	if value, ok := cache.Get("new"); !ok || value != 2 {
		t.Fatalf("loaded value did not expire: %d, %v", value, ok)
	}
}

func TestConcurrentLoaderMisses(t *testing.T) {
	var loads atomic.Int32
	cache, err := New[int, int](Config[int, int]{
		Capacity: 2,
		TTL:      time.Second,
		Loader: func(key int) (int, bool) {
			loads.Add(1)
			time.Sleep(time.Millisecond)
			return key * 2, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if value, ok := cache.Get(21); !ok || value != 42 {
				t.Errorf("concurrent loaded value = %d, %v", value, ok)
			}
		}()
	}
	wg.Wait()
	if loads.Load() == 0 {
		t.Fatal("loader was not called")
	}
}

func TestHasAndDeleteAllDoNotLoad(t *testing.T) {
	loads := 0
	cache, err := New[string, int](Config[string, int]{
		Capacity: 2,
		Loader: func(string) (int, bool) {
			loads++
			return 99, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("a", 1)
	if !cache.Has("a") || cache.Has("missing") {
		t.Fatal("Has returned the wrong cache presence")
	}
	if loads != 0 {
		t.Fatal("Has triggered Loader")
	}
	cache.DeleteAll()
	if cache.Len() != 0 || cache.Has("a") {
		t.Fatal("DeleteAll left a cached item")
	}
	if loads != 0 {
		t.Fatal("DeleteAll triggered Loader")
	}
}

func TestGetOrSetAndGetOrSetFunc(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.SetWithTTL("live", 100, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetWithTTL("expired", 200, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if value, existed := cache.GetOrSet("new", 1); existed || value != 1 {
		t.Fatalf("first GetOrSet = %d, %v", value, existed)
	}
	if !cache.Has("live") {
		t.Fatal("GetOrSet evicted a live item while an expired item occupied capacity")
	}
	if value, existed := cache.GetOrSet("new", 2); !existed || value != 1 {
		t.Fatalf("repeat GetOrSet = %d, %v", value, existed)
	}
	calls := 0
	if value, existed := cache.GetOrSetFunc("new", func() int {
		calls++
		return 3
	}); !existed || value != 1 {
		t.Fatalf("existing GetOrSetFunc = %d, %v", value, existed)
	}
	if calls != 0 {
		t.Fatal("GetOrSetFunc ran for an existing item")
	}
	if value, existed := cache.GetOrSetFunc("new-func", func() int {
		calls++
		return 4
	}); existed || value != 4 || calls != 1 {
		t.Fatalf("new GetOrSetFunc = %d, %v, calls=%d", value, existed, calls)
	}
}

func TestGetOrSetMethodsSkipLoader(t *testing.T) {
	loads := 0
	cache, err := New[string, int](Config[string, int]{
		Capacity: 2,
		Loader: func(string) (int, bool) {
			loads++
			return 99, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if value, existed := cache.GetOrSet("a", 1); existed || value != 1 {
		t.Fatalf("GetOrSet with Loader = %d, %v", value, existed)
	}
	if value, existed := cache.GetOrSetFunc("b", func() int { return 2 }); existed || value != 2 {
		t.Fatalf("GetOrSetFunc with Loader = %d, %v", value, existed)
	}
	if loads != 0 {
		t.Fatalf("GetOrSet methods triggered Loader %d times", loads)
	}
}

func TestConcurrentGetOrSetFuncRunsOnce(t *testing.T) {
	var calls atomic.Int32
	cache, err := New[int, int](Config[int, int]{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, _ := cache.GetOrSetFunc(21, func() int {
				calls.Add(1)
				time.Sleep(time.Millisecond)
				return 42
			})
			if value != 42 {
				t.Errorf("GetOrSetFunc value = %d, want 42", value)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("GetOrSetFunc calls = %d, want 1", calls.Load())
	}
}

func TestGetAndDeleteDoesNotLoad(t *testing.T) {
	loads := 0
	cache, err := New[string, int](Config[string, int]{
		Capacity: 1,
		Loader: func(string) (int, bool) {
			loads++
			return 99, true
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("present", 7)
	if value, found := cache.GetAndDelete("present"); !found || value != 7 {
		t.Fatalf("GetAndDelete present = %d, %v", value, found)
	}
	if cache.Has("present") {
		t.Fatal("GetAndDelete left its item in the cache")
	}
	if value, found := cache.GetAndDelete("missing"); found || value != 0 {
		t.Fatalf("GetAndDelete missing = %d, %v", value, found)
	}
	if loads != 0 {
		t.Fatal("GetAndDelete triggered Loader")
	}
}

func TestKeysAndItems(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("a", 1)
	cache.Set("b", 2)
	cache.Set("c", 3)
	cache.Get("a")
	if err := cache.SetWithTTL("expired", 4, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if cache.Has("expired") {
		t.Fatal("Has reported an expired item")
	}
	keys := cache.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys = %v, want three unexpired keys", keys)
	}
	keySet := make(map[string]bool, len(keys))
	for _, key := range keys {
		keySet[key] = true
	}
	if !keySet["a"] || !keySet["b"] || !keySet["c"] || keySet["expired"] {
		t.Fatalf("Keys = %v", keys)
	}
	items := cache.Items()
	if len(items) != 3 || items["a"] != 1 || items["b"] != 2 || items["c"] != 3 {
		t.Fatalf("Items = %v", items)
	}
	items["a"] = 100
	if value, found := cache.Get("a"); !found || value != 1 {
		t.Fatalf("Items map mutation changed cache: %d, %v", value, found)
	}
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	for _, cfg := range []Config[string, int]{
		{},
		{Capacity: -1},
		{Capacity: 1, TTL: -time.Second},
	} {
		if _, err := New[string, int](cfg); err == nil {
			t.Fatalf("New(%+v) succeeded", cfg)
		}
	}
	if _, err := New[string, int](Config[string, int]{Capacity: 1}); err != nil {
		t.Fatalf("zero TTL should mean no expiration: %v", err)
	}
}

func TestInstancesHaveIndependentCapacityAndTTL(t *testing.T) {
	short, err := New[string, int](Config[string, int]{Capacity: 1, TTL: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	long, err := New[string, int](Config[string, int]{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	short.Set("a", 1)
	long.Set("a", 10)
	long.Set("b", 20)
	short.Set("b", 2)
	if _, ok := short.Get("a"); ok {
		t.Fatal("short cache exceeded its capacity")
	}
	if value, ok := long.Get("a"); !ok || value != 10 {
		t.Fatalf("long cache changed with short cache: %d, %v", value, ok)
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := short.Get("b"); ok {
		t.Fatal("short cache item did not expire")
	}
	if value, ok := long.Get("b"); !ok || value != 20 {
		t.Fatalf("long cache unexpectedly expired: %d, %v", value, ok)
	}
	if short.Stats().Evictions == 0 || long.Stats().Evictions != 0 {
		t.Fatalf("instances shared eviction stats: short=%+v long=%+v", short.Stats(), long.Stats())
	}
}

func TestGetChangesLRUOrderButNotExpiration(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{Capacity: 2, TTL: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("a", 1)
	cache.Set("b", 2)
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("a missing")
	}
	cache.Set("c", 3)
	if _, ok := cache.Get("b"); ok {
		t.Fatal("least recently used item was not evicted")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := cache.Get("a"); !ok {
		t.Fatal("a expired too early")
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := cache.Get("a"); ok {
		t.Fatal("reading a extended its TTL")
	}
}

func TestRefreshTTLOnGet(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{
		Capacity:        1,
		TTL:             150 * time.Millisecond,
		RefreshTTLOnGet: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("key", 1)
	time.Sleep(90 * time.Millisecond)
	if _, ok := cache.Get("key"); !ok {
		t.Fatal("item expired before its first read")
	}
	time.Sleep(90 * time.Millisecond)
	if _, ok := cache.Get("key"); !ok {
		t.Fatal("read did not extend TTL")
	}
	time.Sleep(170 * time.Millisecond)
	if _, ok := cache.Get("key"); ok {
		t.Fatal("item did not expire after reads stopped")
	}
}

func TestSetWithTTLOverridesAndRejectsInvalidTTL(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{Capacity: 3, TTL: 30 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("default", 1)
	if err := cache.SetWithTTL("never", 2, 0); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetWithTTL("short", 3, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := cache.SetWithTTL("never", 4, -time.Second); err == nil {
		t.Fatal("negative item TTL accepted")
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := cache.Get("default"); ok {
		t.Fatal("default item did not expire")
	}
	if _, ok := cache.Get("short"); ok {
		t.Fatal("override TTL did not expire")
	}
	if value, ok := cache.Get("never"); !ok || value != 2 {
		t.Fatalf("invalid TTL changed non-expiring item: %d, %v", value, ok)
	}
	cache.Set("never", 5)
	time.Sleep(50 * time.Millisecond)
	if _, ok := cache.Get("never"); ok {
		t.Fatal("Set did not reset item to default TTL")
	}
}

func TestExpiredItemsAreRemovedBeforeCapacityEviction(t *testing.T) {
	cache, err := New[string, int](Config[string, int]{Capacity: 2, TTL: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set("expired", 1)
	if err := cache.SetWithTTL("live", 2, 0); err != nil {
		t.Fatal(err)
	}
	time.Sleep(35 * time.Millisecond)
	cache.Set("new", 3)
	if value, ok := cache.Get("live"); !ok || value != 2 {
		t.Fatalf("live item evicted while expired item occupied capacity: %d, %v", value, ok)
	}
	if value, ok := cache.Get("new"); !ok || value != 3 {
		t.Fatalf("new item missing: %d, %v", value, ok)
	}
	if got := cache.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestDeleteAndStats(t *testing.T) {
	cache, err := New[int, string](Config[int, string]{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	cache.Set(1, "one")
	cache.Set(1, "updated")
	if value, ok := cache.Get(1); !ok || value != "updated" {
		t.Fatalf("updated value = %q, %v", value, ok)
	}
	cache.Set(2, "two")
	if _, ok := cache.Get(1); ok {
		t.Fatal("evicted item found")
	}
	stats := cache.Stats()
	if stats.Hits != 1 || stats.Misses != 1 || stats.Insertions != 2 || stats.Updates != 1 || stats.Evictions != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	cache.Delete(2)
	if got := cache.Len(); got != 0 {
		t.Fatalf("Len() after Delete = %d", got)
	}
}

func TestConcurrentOperations(t *testing.T) {
	cache, err := New[int, int](Config[int, int]{Capacity: 128, TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				key := (worker*500 + i) % 64
				cache.Set(key, i)
				cache.Get(key)
				if i%10 == 0 {
					cache.DeleteExpired()
					cache.Stats()
				}
			}
		}(worker)
	}
	wg.Wait()
	if cache.Len() > 128 {
		t.Fatalf("capacity exceeded: %d", cache.Len())
	}
}
