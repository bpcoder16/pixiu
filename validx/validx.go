package validx

import "unicode/utf8"

// IsMobileCN 检查中国大陆手机号的宽松格式：11 位 ASCII 数字，以 1 开头，第二位为 3～9。
// 不接受国家码、空白或分隔符，不检查号段是否已分配或号码是否开通。
func IsMobileCN(s string) bool {
	return len(s) == 11 && s[0] == '1' && s[1] >= '3' && s[1] <= '9' && IsDigits(s[2:])
}

// IsDigits 检查非空字符串是否仅包含 ASCII 数字 0～9，允许前导零。
func IsDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

// RuneLenBetween 检查合法 UTF-8 字符串的 rune 数量是否位于闭区间 [min, max]。
// 负数边界或 min > max 返回 false；min 为 0 时允许空字符串。
// 不做归一化或空白清理，组合 emoji 可能包含多个 rune。
func RuneLenBetween(s string, min, max int) bool {
	// 每个 rune 占 1～4 字节；用除法预判上界，避免 max * 4 溢出。
	if min < 0 || max < min || len(s) < min || len(s)/utf8.UTFMax > max {
		return false
	}
	if !utf8.ValidString(s) {
		return false
	}
	count := utf8.RuneCountInString(s)
	return count >= min && count <= max
}
