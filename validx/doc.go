// Package validx 提供无状态、仅依赖标准库的参数格式验证函数。
//
// 支持中国大陆手机号、常用 ASCII 邮箱、HTTP/HTTPS URL、IPv4/IPv6、
// ASCII 数字串及 UTF-8 rune 长度范围。函数可并发调用，非法输入返回 false。
// 不自动清理空白、补协议、删除国家码或归一化 Unicode，不访问网络。
// 格式通过不代表号码已分配、邮箱存在或地址可达；必填、可选字段和业务错误由调用方处理。
//
// 手机号只做 1[3-9][0-9]{9} 的完整格式初筛，不维护运营商号段表。
// 邮箱采用未加引号的 dot-atom 本地部分和至少两段的 ASCII 域名，
// 不接受显示名称、注释、地址列表、IP 字面量域名或原始 Unicode 邮箱。
// 主机名每段为 1～63 字节的 ASCII 字母、数字或连字符，连字符不能位于段首尾，
// 总长最多 253 字节；邮箱域名不接受尾部根点，不检查顶级域名清单或 IDNA 编码含义。
// HTTP URL 允许单段主机名、一个尾部根点、私有及回环地址；
// 仅由数字和点组成的主机须严格符合 IPv4 语法，不接受简写数字地址。
// 完整输入须为合法 UTF-8，百分号转义须合法，不能包含未转义空白或控制字符。
// IP 验证不接受端口、CIDR、方括号或 zone，IPv4 映射 IPv6 仍归 IPv6。
//
// 除 RuneLenBetween 在 min 为 0 时可接受空字符串外，格式函数均拒绝空字符串。
// 长度按 Unicode 码点数计算，不按字节或字素簇计算；组合 emoji 可能包含多个 rune。
//
// 以下示例需导入 errors 和 github.com/bpcoder16/pixiu/validx：
//
//	func validateContact(mobile, email, nickname string) error {
//		if !validx.IsMobileCN(mobile) {
//			return errors.New("手机号格式错误")
//		}
//		if email != "" && !validx.IsEmail(email) {
//			return errors.New("邮箱格式错误")
//		}
//		if !validx.RuneLenBetween(nickname, 2, 20) {
//			return errors.New("昵称长度应为 2～20 个字符")
//		}
//		return nil
//	}
package validx
