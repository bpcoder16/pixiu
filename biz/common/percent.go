package common

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
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
	whole, fraction := value/100, value%100
	sign := ""
	if value < 0 {
		// 拆分后再取反，避免直接对 int64 最小值取反溢出，并保留不足 1% 时的负号。
		sign = "-"
		whole, fraction = -whole, -fraction
	}
	percentage := fmt.Sprintf("%s%d.%02d", sign, whole, fraction)
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
	digits, negative := strings.CutPrefix(percentage, "-")
	whole, fraction, hasDot := strings.Cut(digits, ".")
	if whole == "" || (hasDot && (fraction == "" || len(fraction) > 2)) {
		return 0, ErrInvalidPercent
	}
	for _, part := range []string{whole, fraction} {
		for i := range len(part) {
			if part[i] < '0' || part[i] > '9' {
				return 0, ErrInvalidPercent
			}
		}
	}
	// 小数补齐两位后直接解析为万分之一整数；保留负号以覆盖 int64 最小值。
	fraction += strings.Repeat("0", 2-len(fraction))
	scaled := whole + fraction
	if negative {
		scaled = "-" + scaled
	}
	result, err := strconv.ParseInt(scaled, 10, 64)
	if err != nil {
		return 0, ErrPercentOverflow
	}
	return result, nil
}
