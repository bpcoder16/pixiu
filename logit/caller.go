package logit

import (
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// moduleRoot 是本包源码的绝对路径前缀(…/pixiu/),用本文件自身的编译路径推导,
// 与代码检出位置无关。运行时把调用点路径裁剪掉该前缀,得到 log/logger.go:42 形式。
var moduleRoot = func() string {
	_, file, _, _ := runtime.Caller(0) // …/pixiu/logit/caller.go
	return filepath.Dir(filepath.Dir(file)) + string(filepath.Separator)
}()

// CallerPath 返回调用点 file:line(跳过 skip 层栈帧,0 = 调用 CallerPath 处),
// 路径裁剪掉模块前缀以缩短行宽。定位失败返回 "unknown"。
func CallerPath(skip int) string {
	_, file, line, ok := runtime.Caller(skip + 1)
	if !ok {
		return "unknown"
	}
	path := trimCallerPath(file)
	var digits [20]byte
	number := strconv.AppendInt(digits[:0], int64(line), 10)
	var out strings.Builder
	out.Grow(len(path) + 1 + len(number))
	out.WriteString(path)
	out.WriteByte(':')
	out.Write(number)
	return out.String()
}

func trimCallerPath(file string) string {
	if moduleRoot != "" && strings.HasPrefix(file, moduleRoot) {
		return file[len(moduleRoot):]
	}
	// 第三方模块或标准库路径
	for _, p := range []string{"pkg/mod/", "src/"} {
		if i := strings.LastIndex(file, p); i >= 0 {
			return file[i+len(p):]
		}
	}
	// 业务项目通常不与 pixiu 位于同一模块根目录。保留末级目录与文件名,
	// 既足够定位调用点,也避免把构建机绝对路径写进日志。
	sep := byte(filepath.Separator)
	last := strings.LastIndexByte(file, sep)
	if last < 0 {
		return file
	}
	if last == 0 {
		return file[1:]
	}
	parent := strings.LastIndexByte(file[:last], sep)
	return file[parent+1:]
}
