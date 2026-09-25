//go:build darwin || linux

package logit

import "syscall"

// dupToFd 把 w 的文件描述符复制到进程标准 fd(1/2),实现输出劫持。
func dupToFd(w Writer, fd int) error {
	fw, ok := w.(interface{ Fd() uintptr })
	if !ok {
		return errNoFd
	}
	return syscall.Dup2(int(fw.Fd()), fd)
}

// saveFd 备份当前 fd,用于测试恢复。
func saveFd(fd int) (int, error) { return syscall.Dup(fd) }

// restoreFd 用 saved 恢复 fd,用于测试恢复。
func restoreFd(saved, fd int) error { return syscall.Dup2(saved, fd) }

// closeFd 关闭备份描述符,用于测试清理。
func closeFd(fd int) error { return syscall.Close(fd) }
