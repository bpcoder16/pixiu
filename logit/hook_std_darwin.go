package logit

import "syscall"

func dupFd(oldfd, newfd int) error { return syscall.Dup2(oldfd, newfd) }
