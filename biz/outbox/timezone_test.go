package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestStoreLocationConfiguration(t *testing.T) {
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	local := time.Local
	for _, backend := range []Backend{MySQL, PostgreSQL} {
		for _, tc := range []struct {
			name     string
			location *time.Location
		}{
			{name: "默认UTC"},
			{name: "北京时间", location: shanghai},
			{name: "自定义偏移", location: time.FixedZone("custom +05:45", 5*3600+45*60)},
			{name: "夏令时", location: newYork},
		} {
			t.Run(string(backend)+"/"+tc.name, func(t *testing.T) {
				requireBackend(t, backend)
				store, err := NewStore(StoreConfig{
					Backend: backend,
					MasterDB: func(context.Context) *gorm.DB {
						t.Fatal("构造器不应访问数据库")
						return nil
					},
					Location: tc.location,
				})
				if err != nil {
					t.Fatal(err)
				}
				want := tc.location
				if want == nil {
					want = time.UTC
				}
				if store.cfg.Location != want {
					t.Fatalf("配置时区未保留: %v, want %v", store.cfg.Location, want)
				}
			})
		}
	}
	if time.Local != local {
		t.Fatal("Outbox 不应修改进程时区")
	}
}

// 配置时区必须覆盖创建时间、过期时间往返与实际到期判定。
func TestConfiguredLocationLifecycle(t *testing.T) {
	shanghai, _ := time.LoadLocation("Asia/Shanghai")
	newYork, _ := time.LoadLocation("America/New_York")
	for _, backend := range []Backend{MySQL, PostgreSQL} {
		for _, location := range []*time.Location{time.UTC, shanghai, time.FixedZone("custom +05:45", 20700), newYork} {
			t.Run(string(backend)+"/"+location.String(), func(t *testing.T) {
				base, db := liveStore(t, backend)
				cfg := base.cfg
				cfg.Location = location
				s, err := NewStore(cfg)
				if err != nil {
					t.Fatal(err)
				}
				ctx := context.Background()
				expiry := time.Now().UTC().Add(time.Hour).Truncate(time.Microsecond)
				value := task()
				value.ExpiresAt = expiry
				id := enqueueTask(t, s, db, value)
				r, err := s.Get(ctx, id)
				if err != nil {
					t.Fatal(err)
				}
				if r.ExpiresAt == nil || !r.ExpiresAt.Equal(expiry) || time.Since(r.CreatedAt.Time).Abs() > 10*time.Second {
					t.Fatalf("时刻偏移: %+v", r)
				}
				for _, v := range []time.Time{r.CreatedAt.Time, r.UpdatedAt.Time, r.ExpiresAt.Time} {
					if v.Location() != location {
						t.Fatalf("未使用配置时区: %v", v)
					}
				}
				w := testWorker(t, s, func(_ context.Context, delivery Delivery) error {
					if !delivery.ExpiresAt.Equal(expiry) || delivery.ExpiresAt.Location() != location {
						return errors.New("Delivery 的截止时刻或配置时区丢失")
					}
					return nil
				})
				if err := w.Send(ctx, id); err != nil {
					t.Fatal(err)
				}
				r, _ = s.Get(ctx, id)
				if r.Status != Succeeded || r.FinishedAt == nil || r.FinishedAt.Location() != location {
					t.Fatalf("完成时间错误: %+v", r)
				}
				value = task()
				value.ExpiresAt = expiry.Add(-2 * time.Hour)
				id = enqueueTask(t, s, db, value)
				if err := w.Send(ctx, id); !errors.Is(err, ErrExpired) {
					t.Fatalf("过期过滤错误: %v", err)
				}
			})
		}
	}
}
