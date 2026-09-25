package rotatefile

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotateNamesPreviousPeriodAtBoundary(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	boundary := time.Date(2026, 9, 23, 0, 0, 0, 0, loc)
	for _, tt := range []struct {
		name   string
		every  time.Duration
		before time.Time
		old    string
		new    string
	}{
		{"hour", time.Hour, boundary.Add(-time.Minute), "2026092223", "2026092300"},
		{"day", 24 * time.Hour, boundary.Add(-time.Hour), "20260922", "20260923"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.log")
			cfg := defaultConfig()
			cfg.every = tt.every
			r, err := openFile(path, cfg, tt.before)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			oldPath := path + "." + tt.old
			assertLink(t, path, oldPath)
			if _, err := r.f.Write([]byte("previous\n")); err != nil {
				t.Fatal(err)
			}
			r.mu.Lock()
			ready, err := r.advanceLocked(boundary)
			r.mu.Unlock()
			if !ready || err != nil {
				t.Fatalf("轮转后当前时段应就绪: ready=%v, err=%v", ready, err)
			}
			newPath := path + "." + tt.new
			assertLink(t, path, newPath)
			if data, err := os.ReadFile(oldPath); err != nil || string(data) != "previous\n" {
				t.Fatalf("previous period: data=%q, err=%v", data, err)
			}
			if data, err := os.ReadFile(newPath); err != nil || len(data) != 0 {
				t.Fatalf("new period: data=%q, err=%v", data, err)
			}
			entries, err := os.ReadDir(filepath.Dir(path))
			if err != nil || len(entries) != 3 {
				t.Fatalf("轮转后应只有软链和两个时段文件: %v, %v", entries, err)
			}
		})
	}
}

func TestRotateRestartOpensCurrentPeriodWithoutBackfill(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	path := filepath.Join(t.TempDir(), "app.log")
	cfg := defaultConfig()
	cfg.every = 24 * time.Hour
	first := time.Date(2026, 9, 23, 0, 0, 0, 0, loc)
	r, err := openFile(path, cfg, first)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.f.Write([]byte("old\n")); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openFile(path, cfg, first.AddDate(0, 0, 3))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertLink(t, path, path+".20260926")
	if _, err := reopened.f.Write([]byte("new\n")); err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]string{"20260923": "old\n", "20260926": "new\n"} {
		data, err := os.ReadFile(path + "." + name)
		if err != nil || string(data) != want {
			t.Fatalf("%s: data=%q, err=%v", name, data, err)
		}
	}
	for _, name := range []string{"20260924", "20260925"} {
		if _, err := os.Stat(path + "." + name); !os.IsNotExist(err) {
			t.Fatalf("downtime file %s exists: %v", name, err)
		}
	}
}

func TestRotateRestartWithinPeriodAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	first, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("second\n")); err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "first\nsecond\n" {
		t.Fatalf("same period: data=%q, err=%v", data, err)
	}
}

func TestRotateMaxFilesCountsCurrentPhysicalFile(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	now := time.Date(2026, 9, 23, 12, 0, 0, 0, loc)
	path := filepath.Join(t.TempDir(), "app.log")
	for i := 1; i <= 4; i++ {
		name := path + "." + now.Add(-time.Duration(i)*time.Hour).Format(hourlyLayout)
		if err := os.WriteFile(name, []byte("archive"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := defaultConfig()
	cfg.maxFiles = 3
	r, err := openFile(path, cfg, now)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	_, count := readAll(t, filepath.Dir(path), "app.log")
	if count != 3 {
		t.Fatalf("physical files = %d, want 3", count)
	}
	for _, age := range []int{1, 2} {
		name := path + "." + now.Add(-time.Duration(age)*time.Hour).Format(hourlyLayout)
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("recent archive missing: %s: %v", name, err)
		}
	}
}

func TestRotateMaxFilesThreeKeepsPreviousPeriods(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	cfg := defaultConfig()
	cfg.maxFiles = 3
	start := time.Date(2026, 9, 22, 23, 0, 0, 0, time.Local)
	r, err := openFile(path, cfg, start)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	for i := 1; i <= 3; i++ {
		r.mu.Lock()
		ready, err := r.advanceLocked(start.Add(time.Duration(i) * time.Hour))
		r.mu.Unlock()
		if !ready || err != nil {
			t.Fatalf("轮转失败: ready=%v, err=%v", ready, err)
		}
	}
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanup(); err != nil {
		t.Fatal(err)
	}
	_, count := readAll(t, filepath.Dir(path), "app.log")
	if count != 3 {
		t.Fatalf("physical files = %d, want 3", count)
	}
	for i := 1; i <= 3; i++ {
		if _, err := os.Stat(r.periodPath(start.Add(time.Duration(i) * time.Hour))); err != nil {
			t.Fatalf("应保留当前及最近两个历史文件: %v", err)
		}
	}
	assertLink(t, path, path+".2026092302")
}

func TestRotateCleanupCountsCurrentPeriodAndPreservesUnrelatedFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	for _, suffix := range []string{"20260918", "20260921", "20260922", "2026092210", "2026092222", "20260999", "2026092200.gz", "notes"} {
		if err := os.WriteFile(path+"."+suffix, []byte("keep data"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(path+".20260920", 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Base(path)+".notes", path+".20260919"); err != nil {
		t.Fatal(err)
	}
	cfg := defaultConfig()
	cfg.every = 24 * time.Hour
	cfg.maxFiles = 3
	r, err := openFile(path, cfg, time.Date(2026, 9, 23, 12, 0, 0, 0, time.Local))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := os.Stat(path + ".20260918"); !os.IsNotExist(err) {
		t.Fatalf("oldest period was not deleted: %v", err)
	}
	for _, suffix := range []string{"20260921", "20260922", "2026092210", "2026092222", "20260999", "2026092200.gz", "notes", "20260919"} {
		if data, err := os.ReadFile(path + "." + suffix); err != nil || string(data) != "keep data" {
			t.Errorf("preserved file %s: %q, %v", suffix, data, err)
		}
	}
	if info, err := os.Stat(path + ".20260920"); err != nil || !info.IsDir() {
		t.Fatalf("unrelated directory changed: %v", err)
	}
	assertLink(t, path, path+".20260923")
}

func TestRotateRejectsInvalidPeriodAndFileLimit(t *testing.T) {
	for _, opts := range [][]Option{
		{OptEvery(0)},
		{OptEvery(-time.Hour)},
		{OptEvery(5 * time.Minute)},
		{OptEvery(2 * time.Hour)},
		{OptMaxFiles(1)},
		{OptMaxFiles(2)},
		{OptMaxFiles(0)},
		{OptMaxFiles(-1)},
	} {
		_, err := New(filepath.Join(t.TempDir(), "app.log"), opts...)
		if err == nil {
			t.Fatalf("New accepted invalid options: %v", opts)
		}
	}
}

func TestRotateRejectsRelativePath(t *testing.T) {
	t.Chdir(t.TempDir())
	for _, path := range []string{"app.log", "./app.log"} {
		if w, err := New(path); err == nil {
			_ = w.Close()
			t.Errorf("New accepted relative path %q", path)
		}
	}
}

func TestPeriodStart(t *testing.T) {
	loc := time.FixedZone("CST", 8*3600)
	base := time.Date(2026, 9, 23, 10, 37, 22, 0, loc)
	for _, tt := range []struct {
		every time.Duration
		start string
	}{
		{time.Hour, "2026-09-23 10:00:00"},
		{24 * time.Hour, "2026-09-23 00:00:00"},
	} {
		start := periodStart(base, tt.every)
		if got := start.Format("2006-01-02 15:04:05"); got != tt.start {
			t.Errorf("periodStart(%v) = %s, want %s", tt.every, got, tt.start)
		}
	}
}
