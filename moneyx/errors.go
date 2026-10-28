package moneyx

import "errors"

var (
	// ErrInvalidFormat 表示去除可选负号后整数部分为空，或小数点后的部分不满足一至两个字节的长度要求。
	ErrInvalidFormat = errors.New("moneyx: invalid amount format")
	// ErrInvalidCharacter 表示结构检查通过后，整数或小数部分包含非 ASCII 数字字符。
	ErrInvalidCharacter = errors.New("moneyx: invalid amount character")
	// ErrOverflow 表示转换后的分金额超出 int64 范围。
	ErrOverflow = errors.New("moneyx: amount overflows int64 cents")
)
