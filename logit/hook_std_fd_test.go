//go:build darwin || linux

package logit

import (
	"errors"
	"os"
	"syscall"
	"testing"
)

type hookFdWriter struct {
	Writer
	fd int
}

func (w hookFdWriter) Fd() uintptr { return uintptr(w.fd) }

func TestHookFdDupSemantics(t *testing.T) {
	for _, operation := range []struct {
		name string
		dup  func(int, int) error
	}{
		{
			name: "redirect",
			dup: func(oldfd, newfd int) error {
				return dupToFd(hookFdWriter{fd: oldfd}, newfd)
			},
		},
		{
			name: "restore",
			dup:  restoreFd,
		},
	} {
		t.Run(operation.name, func(t *testing.T) {
			newFile := func(t *testing.T) *os.File {
				t.Helper()
				f, err := os.CreateTemp(t.TempDir(), "fd-*")
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = f.Close() })
				return f
			}
			flags := func(t *testing.T, fd int) uintptr {
				t.Helper()
				value, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(fd), syscall.F_GETFD, 0)
				if errno != 0 {
					t.Fatal(errno)
				}
				return value
			}
			t.Run("same valid fd", func(t *testing.T) {
				f := newFile(t)
				fd := int(f.Fd())
				before := flags(t, fd)
				if err := operation.dup(fd, fd); err != nil {
					t.Fatal(err)
				}
				if after := flags(t, fd); after != before {
					t.Fatalf("同一 fd 的标志被改变: before=%d, after=%d", before, after)
				}
				if _, err := f.WriteString("still open"); err != nil {
					t.Fatal(err)
				}
			})
			t.Run("same invalid fd", func(t *testing.T) {
				if err := operation.dup(-1, -1); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("无效 fd 应返回 EBADF，实际为 %v", err)
				}
			})
			t.Run("replace target", func(t *testing.T) {
				source, target := newFile(t), newFile(t)
				targetFd := int(target.Fd())
				if err := operation.dup(int(source.Fd()), targetFd); err != nil {
					t.Fatal(err)
				}
				if flags(t, targetFd)&syscall.FD_CLOEXEC != 0 {
					t.Fatal("复制到不同 fd 时应清除 FD_CLOEXEC")
				}
				// 标准流重定向后，即使原 Writer 关闭，目标 fd 也必须保持可写。
				if err := source.Close(); err != nil {
					t.Fatal(err)
				}
				if _, err := syscall.Write(targetFd, []byte("redirected")); err != nil {
					t.Fatal(err)
				}
				if data, err := os.ReadFile(source.Name()); err != nil || string(data) != "redirected" {
					t.Fatalf("源文件内容: %q, %v", data, err)
				}
				if data, err := os.ReadFile(target.Name()); err != nil || len(data) != 0 {
					t.Fatalf("原目标文件不应被写入: %q, %v", data, err)
				}
			})
			t.Run("invalid source preserves target", func(t *testing.T) {
				target := newFile(t)
				fd := int(target.Fd())
				before := flags(t, fd)
				if err := operation.dup(-1, fd); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("无效源 fd 应返回 EBADF，实际为 %v", err)
				}
				if flags(t, fd) != before {
					t.Fatal("失败的重定向改变了目标 fd 标志")
				}
				if _, err := target.WriteString("unchanged"); err != nil {
					t.Fatal(err)
				}
				if data, err := os.ReadFile(target.Name()); err != nil || string(data) != "unchanged" {
					t.Fatalf("失败的重定向改变了目标: %q, %v", data, err)
				}
			})
			t.Run("invalid target", func(t *testing.T) {
				source := newFile(t)
				if err := operation.dup(int(source.Fd()), -1); !errors.Is(err, syscall.EBADF) {
					t.Fatalf("无效目标 fd 应返回 EBADF，实际为 %v", err)
				}
			})
		})
	}
}
