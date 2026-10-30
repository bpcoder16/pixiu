// Package common 提供常规业务通用工具：按应用时区生成 ID、格式化时间及精确转换百分比。
//
// GenerateIDWithYYMMDDHHmmssSSS 和非 nil 时间的 FormatTime 依赖已初始化的 infra/env。
// 本包不初始化环境、不持有客户端或连接；百分比转换无需初始化环境。
//
// # ID 与时间
//
// GenerateIDWithYYMMDDHHmmssSSS(prefix) 返回原样前缀、15 位时间和 4 位随机数字：
// YY 为两位年，MM 为月，DD 为日，HH 为 24 小时制小时，mm 为分钟，ss 为秒，SSS 为毫秒。
// 例如前缀 PD、上海时间 2026-10-30 12:34:56.007、随机数 7，返回 PD2610301234560070007。
// 时间使用应用时区，随机数字使用 crypto/rand；不保证唯一，落库须依靠唯一索引和冲突重试。
// FormatTime(*time.Time) 按应用时区输出 RFC3339，省略小数秒；nil 返回空字符串。
//
// # 百分比
//
// FormatPercent 和 ParsePercent 在 int64 万分之一整数与百分比字符串之间精确转换，
// 支持正数、负数和零，不使用浮点运算。例如 1000 对应 10%，-1001 对应 -10.01%。
// FormatPercent 最多保留两位小数并去除末尾零，零输出为 0%。
// ParsePercent 必须有 ASCII % 后缀，数值接受整数或一至两位小数、前导负号和前导零。
// 拒绝空白、正号、科学计数法、千位分隔符、全角字符及多余小数位，不自动清理或舍入。
// -0%、-0.00% 等负零输入转换为 0；是否允许负数或超过 100% 由业务层判断。
// 可解析范围为 -92233720368547758.08% 至 92233720368547758.07%，覆盖 int64 完整范围。
// 格式错误返回 ErrInvalidPercent，格式合法但超出范围返回 ErrPercentOverflow；
// 失败时数值返回 0，调用方通过 errors.Is 判断错误。
//
// 以下示例需导入 fmt 和 github.com/bpcoder16/pixiu/biz/common：
//
//	value, err := common.ParsePercent("-10.01%")
//	if err != nil {
//	    return err
//	}
//	fmt.Println(value)                       // -1001
//	fmt.Println(common.FormatPercent(value)) // -10.01%
package common
