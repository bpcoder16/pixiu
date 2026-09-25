package rotatefile

import (
	"context"
	"github.com/bpcoder16/pixiu/logit"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestCleanupDelayUsesLocalHour(t *testing.T) {
	// 相同绝对时间在非整小时偏移时区中的下一个本地整点不同。
	now := time.Date(2026, 9, 24, 1, 20, 30, 0, time.UTC)
	for _, tt := range []struct {
		offset int
		want   time.Duration
	}{
		{0, 39*time.Minute + 30*time.Second},
		{5*3600 + 45*60, 54*time.Minute + 30*time.Second},
		{-3*3600 - 30*60, 9*time.Minute + 30*time.Second},
	} {
		local := now.In(time.FixedZone("test", tt.offset))
		if got := nextCleanupDelay(local); got != tt.want {
			t.Errorf("下一个本地整点等待时间 = %v，期望 %v（%s）", got, tt.want, local)
		}
	}
}

func TestE2ERotateHourlyCleanupWithoutWrites(t *testing.T) {
	for _, every := range []time.Duration{time.Hour, 24 * time.Hour} {
		t.Run(every.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// 特意在非整点启动，区分整点清理和启动后每隔一小时清理。
				now := time.Now()
				start := time.Date(now.Year(), now.Month(), now.Day(), now.Hour()+1, 17, 23, 0, now.Location())
				time.Sleep(start.Sub(now))
				path := filepath.Join(t.TempDir(), "app.log")
				cfg := defaultConfig()
				cfg.every, cfg.maxFiles = every, 3
				r, err := openFile(path, cfg, time.Now().Add(-every))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = r.Close() }()
				previous := r.f.Name()
				older := r.periodPath(periodStart(time.Now().Add(-2*every), every))
				expired := r.periodPath(periodStart(time.Now().Add(-3*every), every))
				for _, name := range []string{older, expired} {
					if err := os.WriteFile(name, []byte("archive\n"), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				l := logit.MustNew(logit.OptWriter(logit.NewWriter(r)))
				l.Info(context.Background(), "current period")
				if err := r.Sync(); err != nil {
					t.Fatal(err)
				}
				current := r.f.Name()
				if _, err := os.Stat(expired); err != nil {
					t.Fatalf("轮转及 Sync 不应触发清理: %v", err)
				}
				time.Sleep(42*time.Minute + 37*time.Second - time.Nanosecond)
				synctest.Wait()
				if _, err := os.Stat(expired); err != nil {
					t.Fatalf("整点前不应触发清理: %v", err)
				}
				time.Sleep(time.Nanosecond)
				synctest.Wait()
				if _, err := os.Stat(expired); !os.IsNotExist(err) {
					t.Fatalf("无写入时也应在下一个整点清理: %v", err)
				}
				for _, name := range []string{current, previous, older} {
					if _, err := os.Stat(name); err != nil {
						t.Fatalf("应保留最新三个文件: %s: %v", name, err)
					}
				}
				assertLink(t, path, current)
				if data, err := os.ReadFile(path); err != nil || !strings.Contains(string(data), "current period") {
					t.Fatalf("清理应保留当前日志: %q, %v", data, err)
				}
				if err := os.WriteFile(expired, []byte("retry\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				time.Sleep(time.Hour)
				synctest.Wait()
				if _, err := os.Stat(expired); !os.IsNotExist(err) {
					t.Fatalf("后续整点应继续清理: %v", err)
				}
				if err := logit.Close(l); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(expired, []byte("after close\n"), 0o644); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * time.Hour)
				synctest.Wait()
				if _, err := os.Stat(expired); err != nil {
					t.Fatalf("Close 后不应继续执行清理: %v", err)
				}
			})
		})
	}
}

func TestE2ERotateSwitchGranularityPreservesOtherFiles(t *testing.T) {
	for _, tt := range []struct {
		name                   string
		previous, current      time.Duration
		previousLayout, layout string
	}{
		{"hour_to_day", time.Hour, 24 * time.Hour, hourlyLayout, dailyLayout},
		{"day_to_hour", 24 * time.Hour, time.Hour, dailyLayout, hourlyLayout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// 在中午切换，确保同一天的小时文件比日文件的零点后缀更新。
				now := time.Now()
				start := time.Date(now.Year(), now.Month(), now.Day()+1, 12, 17, 0, 0, now.Location())
				time.Sleep(start.Sub(now))
				path := filepath.Join(t.TempDir(), "app.log")
				previous, err := New(path, OptEvery(tt.previous), OptMaxFiles(3))
				if err != nil {
					t.Fatal(err)
				}
				defer previous.Close()
				const archive = "previous mode\n"
				if _, err := previous.Write([]byte(archive)); err != nil {
					t.Fatal(err)
				}
				if err := previous.Close(); err != nil {
					t.Fatal(err)
				}
				var preserved []string
				for i := 0; i < 3; i++ {
					name := path + "." + start.Add(-time.Duration(i)*tt.previous).Format(tt.previousLayout)
					preserved = append(preserved, name)
					if i > 0 {
						if err := os.WriteFile(name, []byte(archive), 0o644); err != nil {
							t.Fatal(err)
						}
					}
				}
				var currentHistory []string
				for i := 1; i <= 3; i++ {
					name := path + "." + start.Add(-time.Duration(i)*tt.current).Format(tt.layout)
					currentHistory = append(currentHistory, name)
					if err := os.WriteFile(name, []byte(archive), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				current, err := New(path, OptEvery(tt.current), OptMaxFiles(3))
				if err != nil {
					t.Fatal(err)
				}
				defer current.Close()
				l := logit.MustNew(logit.OptWriter(logit.NewWriter(current)))
				l.Info(context.Background(), "new mode")
				currentPath := path + "." + start.Format(tt.layout)
				assertPreserved := func() {
					t.Helper()
					for _, name := range append(preserved, currentHistory[:2]...) {
						if data, err := os.ReadFile(name); err != nil || string(data) != archive {
							t.Errorf("另一维度及当前维度最近两个历史文件应保留: %s: %q, %v", name, data, err)
						}
					}
					if _, err := os.Stat(currentHistory[2]); !os.IsNotExist(err) {
						t.Errorf("当前维度最早的文件应被清理: %v", err)
					}
					assertLink(t, path, currentPath)
					if data, err := os.ReadFile(path); err != nil || !strings.Contains(string(data), "new mode") {
						t.Errorf("稳定软链应可读到切换后的日志: %q, %v", data, err)
					}
				}
				assertPreserved()
				// 重新加入过期文件，验证整点清理也只统计当前维度。
				if err := os.WriteFile(currentHistory[2], []byte(archive), 0o644); err != nil {
					t.Fatal(err)
				}
				time.Sleep(43 * time.Minute)
				synctest.Wait()
				assertPreserved()
			})
		})
	}
}
