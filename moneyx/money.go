package moneyx

import (
	"fmt"
	"strconv"
	"strings"
)

// YuanToCent 将元金额转换为 int64 分金额，接受整数或一至两位小数，允许前导负号和前导零。
// 不清理空白、不接受正号或科学计数法、不舍入；负零转换为 0，失败返回 0 和可用 errors.Is 判断的错误。
// 去除可选负号后，依次检查结构与小数部分长度、ASCII 数字字符、分金额范围。
func YuanToCent(value string) (int64, error) {
	digits, negative := strings.CutPrefix(value, "-")
	whole, fraction, hasDot := strings.Cut(digits, ".")
	if whole == "" || (hasDot && (fraction == "" || len(fraction) > 2)) {
		return 0, ErrInvalidFormat
	}
	for _, part := range []string{whole, fraction} {
		for i := range len(part) {
			if part[i] < '0' || part[i] > '9' {
				return 0, ErrInvalidCharacter
			}
		}
	}
	// 小数补齐两位并保留负号后直接解析为分，避免浮点运算，也覆盖 int64 最小值。
	fraction += strings.Repeat("0", 2-len(fraction))
	amount := whole + fraction
	if negative {
		amount = "-" + amount
	}
	cents, err := strconv.ParseInt(amount, 10, 64)
	if err != nil {
		return 0, ErrOverflow
	}
	return cents, nil
}

// CentToYuan 将 int64 分金额转换为元字符串，固定保留两位小数，不损失精度，零输出为 "0.00"。
func CentToYuan(value int64) string {
	whole, fraction := value/100, value%100
	if value < 0 {
		// 先拆出整数和小数部分再取反，避免直接对 int64 最小值取反溢出。
		return fmt.Sprintf("-%d.%02d", -whole, -fraction)
	}
	return fmt.Sprintf("%d.%02d", whole, fraction)
}
