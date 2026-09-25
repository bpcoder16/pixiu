package logit

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestRotateWriteFailureKeepsPreviousPeriodAndRecovers(t *testing.T) {
	for _, failure := range []string{"open", "link"} {
		t.Run(failure, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "app.log")
			now := periodStart(time.Now(), time.Hour)
			r, err := openRotateFile(path, defaultRotateConfig(), now.Add(-time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = r.Close() })
			previous := r.f.Name()
			if _, err := r.f.Write([]byte("previous\n")); err != nil {
				t.Fatal(err)
			}
			blocked := r.periodPath(now)
			if failure == "open" {
				err = os.Mkdir(blocked, 0o755)
			} else {
				blocked = path
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				err = os.WriteFile(path, []byte("do not overwrite"), 0o644)
			}
			if err != nil {
				t.Fatal(err)
			}
			if n, err := r.Write([]byte("must not enter previous period\n")); n != 0 || err == nil {
				t.Fatalf("blocked Write = %d, %v", n, err)
			}
			if data, err := os.ReadFile(previous); err != nil || string(data) != "previous\n" {
				t.Fatalf("previous period changed: %q, %v", data, err)
			}
			if failure == "link" {
				if data, err := os.ReadFile(path); err != nil || string(data) != "do not overwrite" {
					t.Fatalf("conflicting path changed: %q, %v", data, err)
				}
			} else {
				assertLink(t, path, previous)
			}
			if err := os.Remove(blocked); err != nil {
				t.Fatal(err)
			}
			if _, err := r.Write([]byte("current\n")); err != nil {
				t.Fatalf("retry: %v", err)
			}
			assertLink(t, path, r.periodPath(now))
			if data, err := os.ReadFile(path); err != nil || string(data) != "current\n" {
				t.Fatalf("current period: %q, %v", data, err)
			}
		})
	}
}

func TestRotateCleanupFailureRetriesOnNextTick(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("需要通过目录权限限制 ReadDir")
	}
	stderr := captureRotateStderr(t)
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "app.log")
		cfg := defaultRotateConfig()
		cfg.maxFiles = 3
		r, err := openRotateFile(path, cfg, time.Now().Add(-time.Hour))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		previous := r.periodPath(r.boundary.Add(-2 * time.Hour))
		for _, name := range []string{previous, r.periodPath(r.boundary.Add(-time.Hour))} {
			if err := os.WriteFile(name, []byte("archive\n"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		callbacks := 0
		l := MustNew(OptWriter(r), OptOnWriteError(func(error) { callbacks++ }))
		l.Info(context.Background(), "current")
		if err := r.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o300); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(dir, 0o700)
		if _, err := os.ReadDir(dir); !os.IsPermission(err) {
			t.Skipf("无法模拟目录读取失败: %v", err)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		failedOutput := stderr()
		if !strings.Contains(failedOutput, "logit: cleanup") || !strings.Contains(failedOutput, path) {
			t.Fatalf("定时清理错误未输出到 stderr: %q", failedOutput)
		}
		stats := l.(WriteErrorStats)
		if callbacks != 0 || stats.WriteErrors() != 0 || stats.LastWriteError() != nil {
			t.Fatal("后台错误不应进入 Write 错误统计或回调")
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		l.Info(context.Background(), "no immediate retry")
		if err := r.Sync(); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(previous); err != nil {
			t.Fatalf("Write/Sync 不应提前重试清理: %v", err)
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if _, err := os.Stat(previous); !os.IsNotExist(err) {
			t.Fatalf("下一个 tick 未清理过期文件: %v", err)
		}
		if stderr() != failedOutput {
			t.Fatal("成功清理不应重复报告历史错误")
		}
		if err := r.Close(); err != nil {
			t.Fatalf("Close 不应返回历史清理错误: %v", err)
		}
	})
}
