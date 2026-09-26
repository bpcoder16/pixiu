// Package rotatefile 提供独立于日志模块的按本地小时或天轮转的文件写入器。
//
// 直接写入的调用样例：
//
//	package main
//
//	import (
//		"errors"
//		"fmt"
//		"os"
//		"path/filepath"
//		"time"
//
//		"github.com/bpcoder16/pixiu/rotatefile"
//	)
//
//	func main() {
//		dir, err := os.MkdirTemp("", "pixiu-rotate-example-")
//		if err != nil {
//			fmt.Println(err)
//			return
//		}
//		defer os.RemoveAll(dir)
//		path := filepath.Join(dir, "app.log")
//		file, err := rotatefile.New(path,
//			rotatefile.OptEvery(time.Hour),
//			rotatefile.OptMaxFiles(48),
//		)
//		if err != nil {
//			fmt.Println(err)
//			return
//		}
//		_, writeErr := file.Write([]byte("started\n"))
//		if err := errors.Join(writeErr, file.Close()); err != nil {
//			fmt.Println(err)
//		}
//	}
//
// New 只接受绝对路径，并建立指向当前时段文件的稳定软链。默认按本地整小时轮转；
// OptEvery(24*time.Hour) 改为按天轮转。进入新时段后的首次 Write 才会切换文件，
// 每个本地整点执行一次旧文件清理。调用方使用结束后必须 Close，以等待收尾并停止清理循环。
// 如需立即同步当前文件，可调用 Sync。
package rotatefile
