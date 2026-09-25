package rotatefile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// readAll 只读取实际日志文件，避免通过 path 软链重复读取活动文件。
func readAll(t *testing.T, dir, prefix string) (string, int) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	files := 0
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), prefix+".") || !e.Type().IsRegular() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		sb.Write(data)
		files++
	}
	return sb.String(), files
}

func assertLink(t *testing.T, path, target string) {
	t.Helper()
	got, err := os.Readlink(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != filepath.Base(target) {
		t.Fatalf("link %s = %q, want %q", path, got, filepath.Base(target))
	}
}

func TestRotateCreatesPeriodFileAndStableLink(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	w, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	r := w
	want := path + "." + periodStart(time.Now(), time.Hour).Format(hourlyLayout)
	assertLink(t, path, want)
	if _, err := w.Write([]byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.Sync(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("关闭后 Sync 应返回 ErrClosed: %v", err)
	}
	if err := w.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("重复 Close 应返回 ErrClosed: %v", err)
	}
	data, err := os.ReadFile(want)
	if err != nil || string(data) != "hello\n" {
		t.Fatalf("period file: data=%q, err=%v", data, err)
	}
	if _, err := w.Write([]byte("after close")); err == nil {
		t.Fatal("Write after Close succeeded")
	}
}

func TestRotateConcurrent(t *testing.T) {
	dir := t.TempDir()
	w, err := New(filepath.Join(dir, "app.log"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, err := w.Write([]byte(fmt.Sprintf("g%d-j%03d\n", n, j))); err != nil {
					t.Errorf("Write: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	all, _ := readAll(t, dir, "app.log")
	if got := strings.Count(all, "\n"); got != 800 {
		t.Errorf("lines = %d, want 800", got)
	}
}

func TestRotateRejectsRegularPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "app.log")
	if err := os.WriteFile(path, []byte("old data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(path); err == nil {
		t.Fatal("regular path was accepted as stable link")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "old data" {
		t.Fatalf("old path changed: data=%q, err=%v", data, err)
	}
}

func TestRotateLongNameLeavesNoTemporaryFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, strings.Repeat("a", 220)+".log")
	w, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("hello\n")); err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(path); err != nil || string(data) != "hello\n" {
		t.Fatalf("长文件名日志内容异常: %q, %v", data, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("目录应只有稳定软链和实际日志文件: %v, %v", entries, err)
	}
}
