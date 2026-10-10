//go:build darwin || linux

package bootstrap_test

import (
	"os"
	"strconv"
	"syscall"
	"testing"
)

// 直接检查句柄；macOS 对 /dev/fd/N 的路径 Stat 不等于对应句柄的 Fstat。
func requireLogFileOpen(t *testing.T, path string, want bool) {
	t.Helper()
	file, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	target := file.Sys().(*syscall.Stat_t)
	fds, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	open := false
	for _, fd := range fds {
		number, err := strconv.Atoi(fd.Name())
		if err != nil {
			continue
		}
		var info syscall.Stat_t
		if err := syscall.Fstat(number, &info); err == nil && info.Dev == target.Dev && info.Ino == target.Ino {
			open = true
			break
		}
	}
	if open != want {
		t.Fatalf("日志文件打开状态不正确: %s, got %t, want %t", path, open, want)
	}
}
