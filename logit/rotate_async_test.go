package logit

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// 用管道的在用 fd 引用阻塞 Close，无需给生产实现注入慢 I/O 钩子。
func holdRotateOldFile(t *testing.T, r *rotateFile) func() {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	t.Cleanup(func() { _ = writer.Close() })
	if err := r.f.Close(); err != nil {
		t.Fatal(err)
	}
	r.f = writer
	conn, err := writer.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		finished <- conn.Control(func(uintptr) {
			close(entered)
			<-release
		})
	}()
	<-entered
	var once sync.Once
	unblock := func() {
		once.Do(func() {
			close(release)
			if err := <-finished; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(unblock)
	return unblock
}

func TestE2ERotateMaintenanceDoesNotBlockWrites(t *testing.T) {
	for _, finish := range []string{"sync", "close"} {
		t.Run(finish, func(t *testing.T) {
			stderr := captureRotateStderr(t)
			path := filepath.Join(t.TempDir(), "app.log")
			r, err := openRotateFile(path, defaultRotateConfig(), time.Now().Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			unblock := holdRotateOldFile(t, r)
			var callbacks int
			l := MustNew(OptWriter(r), OptOnWriteError(func(error) { callbacks++ }))
			written := make(chan struct{})
			go func() {
				l.Info(context.Background(), "cross boundary")
				l.Info(context.Background(), "while maintenance blocked")
				close(written)
			}()
			select {
			case <-written:
			case <-time.After(2 * time.Second):
				t.Fatal("旧文件维护阻塞了日志写入")
			}
			if data, err := os.ReadFile(path); err != nil || strings.Count(string(data), "\n") != 2 {
				t.Fatalf("Write 返回前必须完成当前文件写入: %q, %v", data, err)
			}
			done := make(chan error, 1)
			go func() {
				if finish == "close" {
					done <- Close(l)
				} else {
					done <- r.Sync()
				}
			}()
			select {
			case err := <-done:
				t.Fatalf("%s 未等待旧文件维护: %v", finish, err)
			case <-time.After(20 * time.Millisecond):
			}
			unblock()
			select {
			case err := <-done:
				// 管道不支持 Sync；后台错误仅输出到 stderr。
				if err != nil {
					t.Fatalf("%s 返回的维护错误不符: %v", finish, err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("释放旧文件后仍未完成，可能持锁等待后台任务")
			}
			if output := stderr(); !strings.Contains(output, "logit: close old file") {
				t.Fatalf("旧文件错误未输出到 stderr: %q", output)
			}
			stats := l.(WriteErrorStats)
			if stats.WriteErrors() != 0 || stats.LastWriteError() != nil || callbacks != 0 {
				t.Fatalf("后台错误统计/回调异常: count=%d, last=%v, callbacks=%d", stats.WriteErrors(), stats.LastWriteError(), callbacks)
			}
		})
	}
}

func TestRotateWriteErrorStatsOwnedByLogger(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	r, err := openRotateFile(path, defaultRotateConfig(), time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := os.Mkdir(r.periodPath(periodStart(time.Now(), time.Hour)), 0o755); err != nil {
		t.Fatal(err)
	}
	callbacks := 0
	l := MustNew(OptWriter(r), OptOnWriteError(func(error) { callbacks++ }))
	l.Info(context.Background(), "cannot open current period")
	if _, ok := any(r).(WriteErrorStats); ok {
		t.Fatal("轮转 Writer 不应独立暴露错误统计")
	}
	stats := l.(WriteErrorStats)
	if callbacks != 1 || stats.WriteErrors() != 1 || stats.LastWriteError() == nil {
		t.Fatal("轮转 Writer 的同步错误必须由 Logger 统计并触发回调")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Write([]byte("closed\n")); !errors.Is(err, os.ErrClosed) || stats.WriteErrors() != 1 {
		t.Fatalf("直接写入错误不应计入 Logger 统计: %v", err)
	}
}

func TestRotateCleanupPreservesCurrentPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	w, err := NewRotateFile(path, OptRotateMaxFiles(3))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := w.Write([]byte("current\n")); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "current\n" {
		t.Fatalf("清理不能删除当前文件: %q, %v", data, err)
	}
}

func TestRotateCleanupDoesNotWaitForPendingFiles(t *testing.T) {
	captureRotateStderr(t)
	path := filepath.Join(t.TempDir(), "app.log")
	start := periodStart(time.Now(), time.Hour)
	cfg := defaultRotateConfig()
	cfg.maxFiles = 3
	r, err := openRotateFile(path, cfg, start.Add(-time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	expired := r.f.Name()
	unblock := holdRotateOldFile(t, r)
	advance := func(now time.Time, line string) string {
		t.Helper()
		r.mu.Lock()
		defer r.mu.Unlock()
		if ready, err := r.advanceLocked(now); !ready || err != nil {
			t.Fatalf("轮转失败: ready=%v, err=%v", ready, err)
		}
		if _, err := r.f.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		return r.f.Name()
	}
	first := advance(start, "first\n")
	firstFile := r.f
	previous := advance(start.Add(time.Hour), "second\n")
	current := advance(start.Add(2*time.Hour), "third\n")
	// 首个旧文件阻塞不应影响后续文件关闭。
	deadline := time.After(2 * time.Second)
	for {
		if _, err := firstFile.Stat(); errors.Is(err, os.ErrClosed) {
			break
		}
		select {
		case <-deadline:
			t.Fatal("后续旧文件仍在等待前一次关闭")
		case <-time.After(time.Millisecond):
		}
	}
	if err := r.cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(expired); !os.IsNotExist(err) {
		t.Fatalf("清理不应因旧文件正在关闭而跳过: %v", err)
	}
	for _, name := range []string{first, previous, current} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("应保留最新三个文件: %s: %v", name, err)
		}
	}
	if got := advance(start, "rollback\n"); got != current {
		t.Fatalf("回拨不应切换到旧路径: %s", got)
	}
	unblock()
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	assertLink(t, path, current)
	if data, err := os.ReadFile(path); err != nil || string(data) != "third\nrollback\n" {
		t.Fatalf("回拨后日志应继续写当前文件: %q, %v", data, err)
	}
}

func TestRotateConcurrentWritesDuringRepeatedRotations(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	r, err := openRotateFile(path, defaultRotateConfig(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				if _, err := r.Write([]byte("complete line\n")); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Go(func() {
		for range 40 {
			r.mu.Lock()
			_, err := r.advanceLocked(r.boundary.Add(time.Hour))
			r.mu.Unlock()
			if err != nil {
				t.Error(err)
			}
			if err := r.Sync(); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	data, _ := readAll(t, filepath.Dir(path), "app.log")
	if data != strings.Repeat("complete line\n", 400) {
		t.Fatal("轮转与同步并发时日志不完整")
	}
}

func TestRotateConcurrentCleanupNeverUnlinksActiveFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	start := periodStart(time.Now(), time.Hour)
	cfg := defaultRotateConfig()
	cfg.maxFiles = 3
	r, err := openRotateFile(path, cfg, start)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 120 {
			if err := r.cleanup(); err != nil {
				t.Error(err)
			}
		}
	})
	for range 4 {
		wg.Go(func() {
			for range 30 {
				func() {
					r.mu.Lock()
					defer r.mu.Unlock()
					previous := r.f.Name()
					if _, err := r.advanceLocked(r.boundary.Add(time.Hour)); err != nil {
						t.Error(err)
						return
					}
					if _, err := r.f.Write([]byte("current\n")); err != nil {
						t.Error(err)
					}
					opened, err := r.f.Stat()
					if err != nil {
						t.Error(err)
						return
					}
					if _, err := os.Stat(previous); err != nil {
						t.Errorf("上一轮文件被清理删除: %v", err)
					}
					linked, err := os.Stat(path)
					if err != nil || !os.SameFile(opened, linked) {
						t.Errorf("当前文件的路径被清理删除或替换: %v", err)
					}
				}()
			}
		})
	}
	wg.Wait()
	if err := r.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := r.cleanup(); err != nil {
		t.Fatal(err)
	}
	_, count := readAll(t, filepath.Dir(path), "app.log")
	if count != 3 {
		t.Fatalf("维护完成后文件数 = %d，期望 3", count)
	}
}

// 测试不并行执行；调用方必须先等待 Writer 关闭，再恢复全局 stderr。
func captureRotateStderr(t *testing.T) func() string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stderr-")
	if err != nil {
		t.Fatal(err)
	}
	original := os.Stderr
	os.Stderr = f
	t.Cleanup(func() {
		os.Stderr = original
		_ = f.Close()
	})
	return func() string {
		data, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
}
