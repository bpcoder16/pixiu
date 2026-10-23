package logit

import "syscall"

func dupFd(oldfd, newfd int) error {
	if oldfd == newfd {
		// Dup3 拒绝相同 fd；Dup2 则在 fd 有效时保持原状，无效时返回 EBADF。
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, uintptr(oldfd), syscall.F_GETFD, 0)
		if errno != 0 {
			return errno
		}
		return nil
	}
	return syscall.Dup3(oldfd, newfd, 0)
}
