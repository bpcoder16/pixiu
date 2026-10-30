package common

import (
	"errors"
	"strings"

	"github.com/bpcoder16/pixiu/moneyx"
)

var (
	// ErrInvalidPercent 表示百分比缺少 % 后缀，或数值不是带可选负号的整数或一至两位小数。
	ErrInvalidPercent = errors.New("common: invalid percent format")
	// ErrPercentOverflow 表示百分比转换为万分之一整数后超出 int64 范围。
	ErrPercentOverflow = errors.New("common: percent overflows int64")
)

// FormatPercent 将万分之一整数转换为百分比字符串，最多两位小数并去除末尾零。
// 例如 1000 返回 10%，-1001 返回 -10.01%；支持 int64 完整范围，不使用浮点运算。
func FormatPercent(value int64) string {
	percentage := moneyx.CentToYuan(value)
	return strings.TrimRight(strings.TrimRight(percentage, "0"), ".") + "%"
}

// ParsePercent 将带 % 后缀的百分比字符串转换为万分之一整数，例如 -10.01% 返回 -1001。
// 接受整数或一至两位小数、前导负号和前导零；负零返回 0，不限制在 0% 至 100%。
// 不清理空白，不接受正号、科学计数法或多余小数位，不舍入。
// 失败返回 0 和 ErrInvalidPercent 或 ErrPercentOverflow，可用 errors.Is 判断。
func ParsePercent(value string) (int64, error) {
	percentage, ok := strings.CutSuffix(value, "%")
	if !ok {
		return 0, ErrInvalidPercent
	}
	// 百分数到万分之一整数也是两位定点缩放，复用 moneyx 保留 int64 边界精度。
	result, err := moneyx.YuanToCent(percentage)
	if errors.Is(err, moneyx.ErrOverflow) {
		return 0, ErrPercentOverflow
	}
	if err != nil {
		return 0, ErrInvalidPercent
	}
	return result, nil
}
