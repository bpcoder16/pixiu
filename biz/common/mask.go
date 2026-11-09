package common

import (
	"strings"
	"unicode/utf8"
)

// MaskName 对展示名称脱敏，按Unicode码点计数，空值和单字符原样返回，双字符只隐藏尾字。
// 三字符及以上保留首尾，中间分别使用1个、2个、最多3个符号；不裁剪空白或推断姓氏。
// symbols仅取首项，省略、为空或不是合法UTF-8时使用*；非空字符串作为完整符号重复。
// 非法UTF-8名称整体替换为一个符号；组合音标和复合表情不按字素簇分割。
func MaskName(value string, symbols ...string) string {
	symbol := "*"
	if len(symbols) > 0 && symbols[0] != "" && utf8.ValidString(symbols[0]) {
		symbol = symbols[0]
	}
	if !utf8.ValidString(value) {
		return symbol
	}
	count := utf8.RuneCountInString(value)
	if count <= 1 {
		return value
	}
	_, firstSize := utf8.DecodeRuneInString(value)
	if count == 2 {
		return value[:firstSize] + symbol
	}
	_, lastSize := utf8.DecodeLastRuneInString(value)
	// 限制符号数量，长名称只展示首尾，不通过星号数量继续暴露原始长度。
	return value[:firstSize] + strings.Repeat(symbol, min(count-2, 3)) + value[len(value)-lastSize:]
}
