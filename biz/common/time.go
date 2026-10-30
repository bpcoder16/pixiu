package common

import (
	"time"

	"github.com/bpcoder16/pixiu/infra/env"
)

// FormatTime 按应用时区输出 RFC3339 时间，nil 返回空字符串；调用前须初始化 Pixiu env。
func FormatTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.In(env.TimeLocation()).Format(time.RFC3339)
}
