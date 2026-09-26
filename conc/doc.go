// Package conc 编排单次调用中的并发任务。
//
// 典型调用：
//
//	package main
//
//	import (
//		"context"
//		"fmt"
//
//		"github.com/bpcoder16/pixiu/conc"
//	)
//
//	func main() {
//		report, err := conc.RunNamed(context.Background(), map[string]conc.Task{
//			"profile": func(context.Context) (any, error) { return "alice", nil },
//			"orders":  func(context.Context) (any, error) { return 3, nil },
//		}, conc.WithLimit(2), conc.WithCancelOnError())
//		if err != nil {
//			fmt.Println("任务失败:", err)
//			return
//		}
//		fmt.Println(report.Results["profile"].Value, report.Results["orders"].Value)
//		fmt.Println("整组耗时:", report.Duration)
//		fmt.Println("profile 执行耗时:", report.Results["profile"].Duration)
//	}
//
// RunNamed 等待全部任务结束，返回整组耗时、每项任务的结果和带任务名的聚合错误。
// Result.Duration 仅包含实际执行，Report.Duration 还包含调度与限流等待。
// 未执行的任务以 Result.Started=false、Result.Duration=0 表示。
// 即使部分任务失败，返回的结果仍包含各任务的 Result，调用方可逐项检查 Result.Err。
// 默认任务失败不会取消其他任务；WithCancelOnError 可让任务失败后取消传给任务的派生 context。
// 调用方传入的 context 不会因此被取消。
// 取消模式仍等待已启动的任务结束，任务函数需要自行响应 context。
// 调用方可用 WithLimit 限制本次调用的并发数。
// 任务来自 map，启动顺序及聚合错误的文本顺序不保证。
// 传入的 context 取消后，尚未开始且已观察到取消的任务不会执行。
// 任务 panic 会转为包含堆栈的 PanicError，由调用方决定如何记录或上报。
//
// 本包没有全局任务池或进程级并发额度，也不执行后台任务或自动写日志。
package conc
