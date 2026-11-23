// Package percentx 在 int64 万分之一整数与百分比文本之间精确转换。
// 1 表示 0.01%，10000 表示 100%。Format 最多保留两位小数，去除末尾零。
// Parse 接受 ASCII 百分号结尾的整数或一至两位小数，支持负数和前导零。
// 转换覆盖整个 int64 范围，不经过浮点数。调用方可用 errors.Is 区分
// ErrInvalidPercent（格式错误）和 ErrPercentOverflow（整数溢出）。
//
//	value, err := percentx.Parse("-10.01%")
//	if err != nil {
//		return err
//	}
//	fmt.Println(value)                  // -1001
//	fmt.Println(percentx.Format(value)) // -10.01%
package percentx
