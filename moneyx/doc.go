// Package moneyx 提供无状态、仅依赖标准库的元字符串与 int64 分金额转换，支持正数、负数和零。
//
// YuanToCent 接受 ASCII 十进制整数或一至两位小数，允许一个前导负号及前导零；
// 例如 "12"、"12.3"、"-00012.34" 分别转换为 1200、1230、-1234 分。
// 拒绝空白、正号、科学计数法、千位分隔符、全角数字及多余小数位，不自动清理或舍入。
// 元金额范围为 "-92233720368547758.08" 至 "92233720368547758.07"，对应 int64 的完整范围。
// "-0"、"-0.00" 等负零输入均转换为 0；CentToYuan 将零输出为 "0.00"。
// CentToYuan 始终输出固定两位小数；任意 int64 分金额往返转换不损失精度。
//
// 去除可选的前导负号后，错误依次按以下顺序判断，失败时分金额返回 0：
//   - ErrInvalidFormat：整数部分为空，或存在小数点但其后部分为空、超过两个字节。
//   - ErrInvalidCharacter：结构与长度检查通过后，整数或小数部分含非 ASCII 数字。
//   - ErrOverflow：格式合法，但转换后的分金额超出 int64 范围。
//
// 调用方通过 errors.Is 判断错误并映射业务提示，不依赖错误文本。
// 本包不管理币种、汇率或业务金额上下限；是否允许负数、零金额等规则由应用判断。
//
// 以下示例需导入 fmt 和 github.com/bpcoder16/pixiu/moneyx：
//
//	cent, err := moneyx.YuanToCent("-12.30")
//	if err != nil {
//	    return err
//	}
//	fmt.Println(cent)                   // -1230
//	fmt.Println(moneyx.CentToYuan(cent)) // -12.30
package moneyx
