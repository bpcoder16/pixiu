package natsx

import "slices"

// namespaceValue 仅用于已校验的原始值；空 queue、Name 和过滤条件保持其原有语义。
// 不猜测调用方是否已加前缀，原始名称或主题本身也可能以 namespace 开头。
func namespaceValue(prefix, value string) string {
	if prefix == "" || value == "" {
		return value
	}
	return prefix + value
}

func namespaceSubjects(prefix string, subjects []string) []string {
	if prefix == "" || len(subjects) == 0 {
		return subjects
	}
	// 注册声明可跨客户端复用，不能在调用方的底层切片上追加前缀。
	result := slices.Clone(subjects)
	for i, subject := range result {
		result[i] = prefix + subject
	}
	return result
}
