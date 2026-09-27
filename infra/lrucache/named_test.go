package lrucache

import "testing"

func TestNewNamedSharesInstance(t *testing.T) {
	name := t.Name()
	created, err := NewNamed(name, Config[string, int]{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	retrieved, err := Named[string, int](name)
	if err != nil {
		t.Fatal(err)
	}
	if retrieved != created {
		t.Fatal("Named did not return the registered instance")
	}
	created.Set("key", 42)
	if value, found := retrieved.Get("key"); !found || value != 42 {
		t.Fatalf("shared value = %d, %v", value, found)
	}
	if created.Stats().Hits != 1 || retrieved.Stats().Insertions != 1 {
		t.Fatalf("stats are not shared: created=%+v retrieved=%+v", created.Stats(), retrieved.Stats())
	}

	other, err := NewNamed(name+"-other", Config[string, int]{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if other == created || other.Has("key") {
		t.Fatal("different names shared cache contents")
	}
}

func TestNamedRejectsInvalidAndDuplicateRegistration(t *testing.T) {
	name := t.Name()
	if _, err := NewNamed("", Config[string, int]{Capacity: 1}); err == nil {
		t.Fatal("empty name was accepted")
	}
	if _, err := NewNamed(name, Config[string, int]{}); err == nil {
		t.Fatal("invalid config was accepted")
	}
	if _, err := Named[string, int](name); err == nil {
		t.Fatal("failed construction reserved the name")
	}
	created, err := NewNamed(name, Config[string, int]{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewNamed(name, Config[string, int]{Capacity: 2}); err == nil {
		t.Fatal("duplicate registration replaced the cache")
	}
	if _, err := NewNamed(name, Config[string, string]{Capacity: 1}); err == nil {
		t.Fatal("same name with another type was accepted")
	}
	if got, err := Named[string, int](name); err != nil || got != created {
		t.Fatalf("registered cache changed after duplicate attempts: %p, %v", got, err)
	}
	if _, err := Named[string, string](name); err == nil {
		t.Fatal("type mismatch was not reported")
	}
	if _, err := Named[string, int](name + "-missing"); err == nil {
		t.Fatal("missing name was not reported")
	}
	if _, err := Named[string, int](""); err == nil {
		t.Fatal("empty lookup name was accepted")
	}
}

func TestNewNamedConcurrentRegistration(t *testing.T) {
	const workers = 12
	name := t.Name()
	type result struct {
		cache *Cache[string, int]
		err   error
	}
	results := make(chan result, workers)
	for range workers {
		go func() {
			cache, err := NewNamed(name, Config[string, int]{Capacity: 1})
			results <- result{cache: cache, err: err}
		}()
	}
	var winner *Cache[string, int]
	for range workers {
		got := <-results
		if got.err == nil {
			if winner != nil {
				t.Fatal("more than one concurrent registration succeeded")
			}
			winner = got.cache
		} else if got.cache != nil {
			t.Fatal("failed registration returned a cache")
		}
	}
	if winner == nil {
		t.Fatal("no concurrent registration succeeded")
	}
	if got, err := Named[string, int](name); err != nil || got != winner {
		t.Fatalf("registered winner = %p, %v", got, err)
	}
}
