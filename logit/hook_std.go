package logit

import "errors"

var errNoFd = errors.New("logit: writer does not expose a file descriptor (use OpenFile)")
var errNilHookWriter = errors.New("logit: nil hook writer")

// HookStdout 把进程标准输出劫持到 w:fmt.Println、三方库直接打印等
// 原本走 stdout 的输出改写入 w(推荐使用 OpenFile 打开独立文件)。
// w 需实现 Fd() uintptr；nil Writer 返回错误。进程级一次性操作,重复调用以最后一次为准。
//
// 注意:dup2 劫持绑定的是打开瞬间的文件描述符;若目标文件会轮转,
// 轮转发生后续写仍落在已转出的旧文件(随保留策略被清理)。
// 建议劫持到独立的非轮转文件(如专用 std.log)。
func HookStdout(w Writer) error { return dupToFd(w, 1) }

// HookStderr 把进程标准错误劫持到 w:未捕获 panic 的 runtime 输出、
// 三方库错误打印等一并进入 w。要求与注意事项同 HookStdout。
func HookStderr(w Writer) error { return dupToFd(w, 2) }
