package logit

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRotateCleanupDoesNotTakeWriteLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	cfg := defaultRotateConfig()
	cfg.maxFiles = 3
	r, err := openRotateFile(path, cfg, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	previous := r.periodPath(r.boundary.Add(-time.Hour))
	expired := r.periodPath(r.boundary.Add(-2 * time.Hour))
	for _, name := range []string{previous, expired} {
		if err := os.WriteFile(name, []byte("archive\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// 模拟轮转已创建新文件、尚未切换 Writer；此时仍应保留当前和上一轮文件。
	next := r.periodPath(r.boundary.Add(time.Hour))
	f, err := os.OpenFile(next, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	r.mu.Lock()
	cleaned := make(chan error, 1)
	go func() { cleaned <- r.cleanup() }()
	select {
	case err := <-cleaned:
		r.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		r.mu.Unlock()
		<-cleaned
		t.Fatal("cleanup 不应等待写锁")
	}
	for _, name := range []string{r.f.Name(), previous, next} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("轮转准备期间不应删除 %s: %v", name, err)
		}
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("未删除最旧文件: %v", err)
	}
}

func TestE2ERotateClockRollbackKeepsCurrentFile(t *testing.T) {
	for _, every := range []time.Duration{time.Hour, 24 * time.Hour} {
		t.Run(every.String(), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.log")
			cfg := defaultRotateConfig()
			cfg.every = every
			// 已进入更晚的时段，随后 Write 看到的当前时间相当于发生了回拨。
			r, err := openRotateFile(path, cfg, time.Now().Add(every))
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			current, boundary := r.f.Name(), r.boundary
			l := MustNew(OptWriter(r))
			l.Info(context.Background(), "clock rollback")
			if r.f.Name() != current || !r.boundary.Equal(boundary) {
				t.Fatal("时钟回拨不应重新打开较早时段的文件")
			}
			assertLink(t, path, current)
			data, count := readAll(t, filepath.Dir(path), "app.log")
			if count != 1 || len(data) == 0 || l.(WriteErrorStats).WriteErrors() != 0 {
				t.Fatalf("回拨期间仍应成功写入当前文件: count=%d, data=%q", count, data)
			}
		})
	}
}
